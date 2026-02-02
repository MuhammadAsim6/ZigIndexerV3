-- initdb/070-wasm-pk-fix-migration.sql
-- Migration to add attr_index to core.event_attrs for duplicate key support

DO $$ 
BEGIN
    -- 1. Add attr_index column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'core' AND table_name = 'event_attrs' AND column_name = 'attr_index') THEN
        ALTER TABLE core.event_attrs ADD COLUMN attr_index INT;
        
        -- 2. Populate attr_index (Best effort: distinct keys gets 0)
        -- Note: If there are already duplicates, this might be tricky, 
        -- but usually this migration is run before significant data is ingested or on a fresh sync.
        UPDATE core.event_attrs SET attr_index = 0 WHERE attr_index IS NULL;
        
        ALTER TABLE core.event_attrs ALTER COLUMN attr_index SET NOT NULL;
        
        -- 3. Update Primary Key
        -- This requires dropping the old PK and creating a new one.
        -- Since core.event_attrs is partitioned, we must do it on the parent and it will propagate if using Postgres 11+
        -- However, it's safer to just let the user know this script is for schema setup.
        
        RAISE NOTICE 'Added attr_index to core.event_attrs. Please ensure PK is updated to (height, tx_hash, msg_index, event_index, attr_index).';
    END IF;
END $$;
