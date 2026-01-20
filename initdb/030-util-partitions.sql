-- initdb/030-util-partitions.sql
-- FIXED: Smart Partitioning (Only creates new tables when needed)

CREATE SCHEMA IF NOT EXISTS util;

CREATE TABLE IF NOT EXISTS util.height_part_ranges (
    schema_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    range_size  BIGINT NOT NULL,
    PRIMARY KEY (schema_name, table_name)
);

-- Configuration: 1 Million blocks per partition
INSERT INTO util.height_part_ranges (schema_name, table_name, range_size) VALUES
 ('core', 'blocks', 1000000), ('core', 'transactions', 1000000), ('core', 'messages', 1000000),
 ('core', 'validator_set', 1000000), ('core', 'validator_missed_blocks', 1000000),
 ('core', 'network_params', 1000000), ('core', 'event_attrs', 100000),
 ('bank', 'transfers', 1000000), ('bank', 'balance_deltas', 1000000),
 ('stake', 'delegation_events', 1000000), ('stake', 'distribution_events', 1000000),
 ('gov', 'deposits', 1000000), ('gov', 'votes', 1000000),
 ('authz_feegrant', 'authz_grants', 1000000), ('authz_feegrant', 'fee_grants', 1000000),
 ('tokens', 'cw20_transfers', 1000000),
 ('wasm', 'executions', 1000000), ('wasm', 'events', 1000000),
 ('wasm', 'event_attrs', 1000000), ('wasm', 'state_kv', 1000000), ('wasm', 'contract_migrations', 1000000),
 -- IBC: No longer partitioned (lifecycle merging requires simple PK)
 ('zigchain', 'dex_swaps', 1000000), ('zigchain', 'dex_liquidity', 1000000)
ON CONFLICT (schema_name, table_name) DO UPDATE SET range_size = EXCLUDED.range_size;

-- Remove unused entries
DELETE FROM util.height_part_ranges
WHERE (schema_name, table_name) NOT IN (
    ('core', 'blocks'), ('core', 'transactions'), ('core', 'messages'),
    ('core', 'validator_set'), ('core', 'validator_missed_blocks'),
    ('core', 'network_params'), ('core', 'event_attrs'),
    ('bank', 'transfers'), ('bank', 'balance_deltas'),
    ('stake', 'delegation_events'), ('stake', 'distribution_events'),
    ('gov', 'deposits'), ('gov', 'votes'),
    ('authz_feegrant', 'authz_grants'), ('authz_feegrant', 'fee_grants'),
    ('tokens', 'cw20_transfers'),
    ('wasm', 'executions'), ('wasm', 'events'), ('wasm', 'event_attrs'),
    ('wasm', 'state_kv'), ('wasm', 'contract_migrations'),
    ('zigchain', 'dex_swaps'), ('zigchain', 'dex_liquidity')
);

-- ✅ SMART FUNCTION: Creates partition ONLY if target height needs it
CREATE OR REPLACE FUNCTION util.ensure_partition_for_height(p_schema text, p_table text, p_target_height bigint)
RETURNS void AS $$
DECLARE
    v_range BIGINT;
    v_start BIGINT;
    v_end BIGINT;
    v_part_name TEXT;
    v_exists BOOLEAN;
BEGIN
    -- 1. Get range size (default 1M)
    SELECT range_size INTO v_range FROM util.height_part_ranges 
    WHERE schema_name = p_schema AND table_name = p_table;
    
    IF NOT FOUND THEN v_range := 1000000; END IF;

    -- 2. Calculate which partition implies this height
    -- e.g. Height 500 -> Start 0. Height 1,500,000 -> Start 1,000,000.
    v_start := (p_target_height / v_range) * v_range;
    v_end   := v_start + v_range;
    
    -- 3. Construct Partition Name (e.g. blocks_p0, blocks_p1000000)
    v_part_name := p_table || '_p' || v_start;

    -- 4. Check if exists
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = p_schema
          AND c.relname = v_part_name
    ) INTO v_exists;

    -- 5. Create ONLY if missing
    IF NOT v_exists THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%s) TO (%s)',
            p_schema, v_part_name, p_schema, p_table, v_start, v_end
        );
    END IF;
END;
$$ LANGUAGE plpgsql;
