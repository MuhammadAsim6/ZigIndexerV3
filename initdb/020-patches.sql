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
  END IF;
END $$;

-- EVENT_ATTRS: Safety index
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_attrs') THEN
    CREATE INDEX IF NOT EXISTS idx_event_attrs_key ON core.event_attrs (key);
  END IF;
END $$;
