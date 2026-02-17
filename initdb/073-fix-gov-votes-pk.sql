-- initdb/073-fix-gov-votes-pk.sql
-- Purpose: Preserve all weighted vote options by extending gov.votes PK with option_index.

DO $$
DECLARE
    v_pk_name TEXT;
BEGIN
    -- 1) Add option_index column (default 0 keeps historical simple votes valid)
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'gov' AND table_name = 'votes' AND column_name = 'option_index'
    ) THEN
        ALTER TABLE gov.votes ADD COLUMN option_index INT NOT NULL DEFAULT 0;
    END IF;

    -- 2) Replace existing PK with one that includes option_index
    SELECT tc.constraint_name
    INTO v_pk_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_schema = 'gov'
      AND tc.table_name = 'votes'
      AND tc.constraint_type = 'PRIMARY KEY'
    LIMIT 1;

    IF v_pk_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE gov.votes DROP CONSTRAINT %I', v_pk_name);
    END IF;

    ALTER TABLE gov.votes
        ADD PRIMARY KEY (proposal_id, voter, height, tx_hash, option_index);
END $$;

