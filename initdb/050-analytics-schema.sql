-- initdb/050-analytics-schema.sql
-- Comprehensive Analytics Schema for Chain Data Indexer

-- ============================================================================
-- 1. SWAP VOLUME HOURLY (Pre-aggregated DEX Analytics)
-- ============================================================================
CREATE TABLE IF NOT EXISTS analytics.swap_volume_hourly (
    hour              TIMESTAMPTZ NOT NULL,
    contract          TEXT NOT NULL,
    offer_asset       TEXT NOT NULL,
    ask_asset         TEXT NOT NULL,
    swap_count        BIGINT DEFAULT 0,
    total_offer       NUMERIC(80,0) DEFAULT 0,
    total_return      NUMERIC(80,0) DEFAULT 0,
    total_commission  NUMERIC(80,0) DEFAULT 0,
    avg_price         NUMERIC(40,18) DEFAULT 0,
    PRIMARY KEY (hour, contract, offer_asset, ask_asset)
);

CREATE INDEX IF NOT EXISTS idx_swap_volume_hour ON analytics.swap_volume_hourly (hour DESC);
CREATE INDEX IF NOT EXISTS idx_swap_volume_contract ON analytics.swap_volume_hourly (contract);

-- ============================================================================
-- 2. TRANSFER VOLUME DAILY (Bank Module Analytics)
-- ============================================================================
CREATE TABLE IF NOT EXISTS analytics.transfer_volume_daily (
    day               DATE NOT NULL,
    denom             TEXT NOT NULL,
    transfer_count    BIGINT DEFAULT 0,
    total_amount      NUMERIC(80,0) DEFAULT 0,
    unique_senders    BIGINT DEFAULT 0,
    unique_receivers  BIGINT DEFAULT 0,
    PRIMARY KEY (day, denom)
);

CREATE INDEX IF NOT EXISTS idx_transfer_volume_day ON analytics.transfer_volume_daily (day DESC);
CREATE INDEX IF NOT EXISTS idx_transfer_volume_denom ON analytics.transfer_volume_daily (denom);

-- ============================================================================
-- 3. ACTIVE WALLETS DAILY (User Activity Analytics)
-- ============================================================================
CREATE TABLE IF NOT EXISTS analytics.active_wallets_daily (
    day               DATE NOT NULL PRIMARY KEY,
    active_wallets    BIGINT DEFAULT 0,
    new_wallets       BIGINT DEFAULT 0,
    tx_count          BIGINT DEFAULT 0,
    avg_gas_used      NUMERIC(20,2) DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_active_wallets_day ON analytics.active_wallets_daily (day DESC);

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

-- ============================================================================
-- 5. GAS STATS DAILY (Network Health Analytics)
-- ============================================================================
CREATE TABLE IF NOT EXISTS analytics.gas_stats_daily (
    day               DATE NOT NULL PRIMARY KEY,
    total_gas_used    NUMERIC(80,0) DEFAULT 0,
    avg_gas_per_tx    NUMERIC(20,2) DEFAULT 0,
    max_gas_tx        TEXT,
    block_count       BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_gas_stats_day ON analytics.gas_stats_daily (day DESC);

-- ============================================================================
-- 6. MESSAGE TYPE STATS DAILY (Protocol Usage Analytics)
-- ============================================================================
CREATE TABLE IF NOT EXISTS analytics.msg_type_stats_daily (
    day               DATE NOT NULL,
    msg_type          TEXT NOT NULL,
    msg_count         BIGINT DEFAULT 0,
    PRIMARY KEY (day, msg_type)
);

CREATE INDEX IF NOT EXISTS idx_msg_stats_day ON analytics.msg_type_stats_daily (day DESC);
CREATE INDEX IF NOT EXISTS idx_msg_stats_type ON analytics.msg_type_stats_daily (msg_type);
