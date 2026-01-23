-- initdb/070-zigchain-wasm-analytics.sql
-- Enhanced WASM DEX Analytics for Zigchain
-- Tracks detailed swap data from CosmWasm AMM contracts

-- ============================================================================
-- 1. WASM SWAPS (Primary: CosmWasm DEX Activity)
-- ============================================================================
CREATE TABLE IF NOT EXISTS wasm.dex_swaps (
    tx_hash           TEXT NOT NULL,
    msg_index         INT NOT NULL,
    event_index       INT NOT NULL,
    contract          TEXT NOT NULL,    -- Pool contract address
    sender            TEXT NOT NULL,
    receiver          TEXT,
    offer_asset       TEXT,             -- e.g., "uzig"
    ask_asset         TEXT,             -- e.g., "coin.zig109f7g2...stzig"
    offer_amount      TEXT,             -- Changed: Event attrs are strings
    return_amount     TEXT,
    spread_amount     TEXT,
    commission_amount TEXT,
    maker_fee_amount  TEXT,
    fee_share_amount  TEXT,
    reserves          TEXT,             -- Pool reserves after swap
    -- NEW: Analytics columns
    pair_id           TEXT,             -- Sorted pair identifier (e.g., "uzig-stzig")
    effective_price   NUMERIC(40,18),   -- return_amount / offer_amount
    price_impact      NUMERIC(20,10),   -- (spread_amount / offer_amount) * 100
    total_fee         TEXT,             -- commission + maker_fee + fee_share
    block_height      BIGINT NOT NULL,
    timestamp         TIMESTAMPTZ,
    PRIMARY KEY (tx_hash, msg_index, event_index, block_height)

) PARTITION BY RANGE (block_height);

CREATE TABLE IF NOT EXISTS wasm.dex_swaps_p0 PARTITION OF wasm.dex_swaps 
    FOR VALUES FROM (0) TO (1000000);
-- Additional partitions created automatically by util.ensure_partition_for_height()

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_contract ON wasm.dex_swaps (contract, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_sender ON wasm.dex_swaps (sender, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_offer ON wasm.dex_swaps (offer_asset, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_ask ON wasm.dex_swaps (ask_asset, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_height ON wasm.dex_swaps USING BRIN (block_height);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_pair ON wasm.dex_swaps (pair_id, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_timestamp ON wasm.dex_swaps (timestamp DESC);


-- ============================================================================
-- 2. FACTORY TOKENS (Track coin.zigXXX tokens)
-- ============================================================================
CREATE TABLE IF NOT EXISTS tokens.factory_tokens (
    denom             TEXT PRIMARY KEY,
    base_denom        TEXT,             -- Extracted symbol (stzig, mango, etc.)
    creator           TEXT NOT NULL,
    symbol            TEXT,             -- Human-readable symbol
    first_seen_height BIGINT NOT NULL,
    first_seen_tx     TEXT
);

CREATE INDEX IF NOT EXISTS idx_factory_tokens_creator ON tokens.factory_tokens (creator);
CREATE INDEX IF NOT EXISTS idx_factory_tokens_symbol ON tokens.factory_tokens (symbol);

-- ============================================================================
-- 3. SWAP VOLUME HOURLY (Pre-aggregated Analytics)
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
    PRIMARY KEY (hour, contract, offer_asset, ask_asset)
);

CREATE INDEX IF NOT EXISTS idx_swap_volume_hour ON analytics.swap_volume_hourly (hour DESC);
CREATE INDEX IF NOT EXISTS idx_swap_volume_contract ON analytics.swap_volume_hourly (contract);

-- ============================================================================
-- 4. HELPER: Update Pool Reserves (Trigger for dex_pools)
-- ============================================================================
CREATE OR REPLACE FUNCTION zigchain.update_pool_reserves()
RETURNS TRIGGER AS $$
BEGIN
    -- Parse reserves format: "denom1:amount1,denom2:amount2"
    -- Update dex_pools with latest reserves if contract matches a pool
    -- This is a placeholder for future implementation
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- COMMENT: Can enable trigger later for real-time pool tracking
-- CREATE TRIGGER trg_update_reserves AFTER INSERT ON wasm.dex_swaps
--     FOR EACH ROW EXECUTE FUNCTION zigchain.update_pool_reserves();
