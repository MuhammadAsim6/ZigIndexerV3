-- initdb/050-analytics-schema.sql
-- Comprehensive Analytics Schema for Chain Data Indexer

-- [DELETED] SWAP VOLUME HOURLY
-- [DELETED] TRANSFER VOLUME DAILY
-- [DELETED] ACTIVE WALLETS DAILY

-- ============================================================================
-- 4. TOKEN HOLDER COUNTS (Current State Snapshot)
-- ============================================================================
CREATE TABLE IF NOT EXISTS analytics.token_holder_counts (
    denom             TEXT NOT NULL PRIMARY KEY,
    holder_count      BIGINT DEFAULT 0,
    total_supply      NUMERIC(80,0) DEFAULT 0,
    last_updated      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_holder_counts_denom ON analytics.token_holder_counts (denom);

-- [DELETED] GAS STATS DAILY
-- [DELETED] MESSAGE TYPE STATS DAILY

