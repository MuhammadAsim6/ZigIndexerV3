-- initdb/050-triggers.sql
-- Triggers for maintaining current state tables from event tables

-- ============================================================================
-- 1. BANK BALANCES CURRENT - Aggregate from balance_deltas
--    Uses FOR EACH STATEMENT with transition tables for performance.
--    Passes raw deltas through EXCLUDED and computes final balances in the
--    ON CONFLICT clause for concurrency safety.
-- ============================================================================

CREATE OR REPLACE FUNCTION bank.update_balances_current_batch()
RETURNS TRIGGER AS $$
BEGIN
    -- Step 1: Aggregate all inserted deltas by (account, denom)
    -- Step 2: Build per-account JSONB of RAW DELTAS (not final values)
    -- Step 3: Upsert — INSERT path stores delta as initial balance (correct for new accounts)
    --                   ON CONFLICT path adds delta to current balance using row-lock-protected read

    WITH aggregated AS (
        SELECT account, denom, SUM(delta) AS total_delta
        FROM inserted_rows
        GROUP BY account, denom
    ),
    per_account_deltas AS (
        SELECT
            account,
            jsonb_object_agg(denom, total_delta::TEXT) AS delta_balances
        FROM aggregated
        GROUP BY account
    )
    INSERT INTO bank.balances_current (account, balances)
    SELECT account, delta_balances
    FROM per_account_deltas
    ON CONFLICT (account) DO UPDATE
    SET balances = (
        SELECT jsonb_object_agg(key, new_value)
        FROM (
            -- Denoms in this batch: add delta to current balance
            SELECT
                key,
                (COALESCE((bank.balances_current.balances->>key)::NUMERIC(80,0), 0)
                 + value::NUMERIC(80,0))::TEXT AS new_value
            FROM jsonb_each_text(EXCLUDED.balances)
            UNION ALL
            -- Denoms NOT in this batch: preserve existing balance
            SELECT key, value AS new_value
            FROM jsonb_each_text(bank.balances_current.balances)
            WHERE key NOT IN (SELECT key FROM jsonb_each_text(EXCLUDED.balances))
        ) merged
    );

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if exists
DROP TRIGGER IF EXISTS trg_balance_deltas_current ON bank.balance_deltas;

-- Statement-level trigger with transition table (PostgreSQL 11+)
CREATE TRIGGER trg_balance_deltas_current
AFTER INSERT ON bank.balance_deltas
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT EXECUTE FUNCTION bank.update_balances_current_batch();


-- ============================================================================
-- 2. STAKE DELEGATIONS CURRENT - Aggregate from delegation_events
-- ============================================================================

CREATE OR REPLACE FUNCTION stake.update_delegations_current()
RETURNS TRIGGER AS $$
DECLARE
    current_amt NUMERIC(80, 0);
    new_amt NUMERIC(80, 0);
    validator_addr TEXT;
BEGIN
    -- Determine validator based on event type
    validator_addr := COALESCE(NEW.validator_dst, NEW.validator_src);
    
    IF validator_addr IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Get current delegation amount
    SELECT amount INTO current_amt
    FROM stake.delegations_current
    WHERE delegator_address = NEW.delegator_address 
      AND validator_address = validator_addr
      AND denom = NEW.denom;
    
    IF current_amt IS NULL THEN
        current_amt := 0;
    END IF;
    
    -- Calculate new amount based on event type
    CASE NEW.event_type
        WHEN 'delegate' THEN
            new_amt := current_amt + NEW.amount;
        WHEN 'undelegate' THEN
            new_amt := current_amt - NEW.amount;
        WHEN 'redelegate' THEN
            -- For redelegate, we need to handle both validators
            -- Decrease from src
            IF NEW.validator_src IS NOT NULL THEN
                UPDATE stake.delegations_current
                SET amount = GREATEST(0, amount - NEW.amount)
                WHERE delegator_address = NEW.delegator_address 
                  AND validator_address = NEW.validator_src
                  AND denom = NEW.denom;
            END IF;
            -- Increase to dst
            new_amt := current_amt + NEW.amount;
            validator_addr := NEW.validator_dst;
        ELSE
            new_amt := current_amt;
    END CASE;
    
    -- Ensure non-negative
    new_amt := GREATEST(0, new_amt);
    
    -- Upsert the delegation
    IF new_amt > 0 THEN
        INSERT INTO stake.delegations_current (delegator_address, validator_address, denom, amount)
        VALUES (NEW.delegator_address, validator_addr, NEW.denom, new_amt)
        ON CONFLICT (delegator_address, validator_address, denom) DO UPDATE
        SET amount = EXCLUDED.amount;
    ELSE
        -- Remove zero delegations
        DELETE FROM stake.delegations_current
        WHERE delegator_address = NEW.delegator_address 
          AND validator_address = validator_addr
          AND denom = NEW.denom;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if exists  
DROP TRIGGER IF EXISTS trg_delegation_events_current ON stake.delegation_events;

CREATE TRIGGER trg_delegation_events_current
AFTER INSERT ON stake.delegation_events
FOR EACH ROW EXECUTE FUNCTION stake.update_delegations_current();


-- ============================================================================
-- 3. INITIAL POPULATION (Run once for existing data)
-- ============================================================================

-- Populate balances_current from historical balance_deltas
CREATE OR REPLACE FUNCTION bank.populate_balances_current()
RETURNS void AS $$
BEGIN
    TRUNCATE bank.balances_current;
    
    INSERT INTO bank.balances_current (account, balances)
    SELECT 
        account,
        jsonb_object_agg(denom, total::TEXT)
    FROM (
        SELECT account, denom, SUM(delta::NUMERIC(80,0)) as total
        FROM bank.balance_deltas
        GROUP BY account, denom
    ) aggregated
    GROUP BY account;
END;
$$ LANGUAGE plpgsql;

-- Populate delegations_current from historical delegation_events
CREATE OR REPLACE FUNCTION stake.populate_delegations_current()
RETURNS void AS $$
BEGIN
    TRUNCATE stake.delegations_current;
    
    INSERT INTO stake.delegations_current (delegator_address, validator_address, denom, amount)
    SELECT 
        delegator_address,
        COALESCE(validator_dst, validator_src) as validator_address,
        denom,
        SUM(
            CASE event_type
                WHEN 'delegate' THEN amount
                WHEN 'undelegate' THEN -amount
                ELSE 0
            END
        ) as amount
    FROM stake.delegation_events
    WHERE COALESCE(validator_dst, validator_src) IS NOT NULL
    GROUP BY delegator_address, COALESCE(validator_dst, validator_src), denom
    HAVING SUM(
        CASE event_type
            WHEN 'delegate' THEN amount
            WHEN 'undelegate' THEN -amount
            ELSE 0
        END
    ) > 0;
END;
$$ LANGUAGE plpgsql;

-- Run initial population
SELECT bank.populate_balances_current();
SELECT stake.populate_delegations_current();
