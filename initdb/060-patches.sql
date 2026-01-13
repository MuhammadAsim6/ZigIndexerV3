-- 020-patches.sql
-- Purpose: Additional performance indexes.
-- Cleaned up to remove redundancy with the new 010-schema.

-- TRANSACTIONS: Optimization for success/failure queries
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'transactions') THEN
    CREATE INDEX IF NOT EXISTS idx_txs_success ON core.transactions (height DESC, tx_index) WHERE code = 0;
    CREATE INDEX IF NOT EXISTS idx_txs_time ON core.transactions (time DESC);
  END IF;
END $$;

-- EVENTS: Extra lookup index
-- Note: core.events NOW HAS height (Added in 010), so this is safe.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'events') THEN
    CREATE INDEX IF NOT EXISTS idx_events_type_msg ON core.events (event_type, msg_index);
    CREATE INDEX IF NOT EXISTS idx_events_attrs_gin ON core.events USING GIN (attributes);
  END IF;
END $$;

-- EVENT_ATTRS: Safety index
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_attrs') THEN
    CREATE INDEX IF NOT EXISTS idx_event_attrs_key ON core.event_attrs (key);
  END IF;
END $$;

-- ============================================================================
-- AUTOVACUUM: Aggressive settings for high-update tables
-- ============================================================================

-- bank.balances_current: Trigger causes frequent updates
ALTER TABLE bank.balances_current SET (
    autovacuum_vacuum_scale_factor = 0.05,  -- Vacuum after 5% dead rows (default: 20%)
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 50        -- Minimum 50 dead rows to trigger
);

-- stake.delegations_current: Same issue
ALTER TABLE stake.delegations_current SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 20
);

-- core.indexer_progress: Updated every flush
ALTER TABLE core.indexer_progress SET (
    autovacuum_vacuum_scale_factor = 0.01,  -- Very aggressive
    autovacuum_vacuum_threshold = 10
);

-- core.validators: Updated on startup and runtime
ALTER TABLE core.validators SET (
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_vacuum_threshold = 5
);
