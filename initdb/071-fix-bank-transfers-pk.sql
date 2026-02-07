-- initdb/071-fix-bank-transfers-pk.sql
-- Purpose: Add event_index to bank.transfers PK to prevent data loss on multi-transfers in same msg

DO $$
BEGIN
    -- 1. Add event_index column if missing
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'bank' AND table_name = 'transfers' AND column_name = 'event_index'
    ) THEN
        ALTER TABLE bank.transfers ADD COLUMN event_index INT NOT NULL DEFAULT 0;
    END IF;

    -- 2. Drop old PK constraint (if exists)
    -- Note: Can't easily drop PK on partitioned table without cascading to partitions,
    -- but usually we can just drop the constraint on the parent.
    -- However, Postgres might require dropping partitions first or doing it on each.
    -- Let's try dropping the constraint on the parent.
    
    -- Find constraint name
    DECLARE pb_pk_name TEXT;
    BEGIN
        SELECT constraint_name INTO pb_pk_name
        FROM information_schema.table_constraints
        WHERE table_schema = 'bank' AND table_name = 'transfers' AND constraint_type = 'PRIMARY KEY';

        IF pb_pk_name IS NOT NULL THEN
            EXECUTE 'ALTER TABLE bank.transfers DROP CONSTRAINT ' || pb_pk_name;
        END IF;
    END;

    -- 3. Add new PK
    ALTER TABLE bank.transfers ADD PRIMARY KEY (height, tx_hash, msg_index, event_index, from_addr, to_addr, denom);

END $$;
