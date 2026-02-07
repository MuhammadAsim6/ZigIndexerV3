-- initdb/041-token-registry.sql
-- Universal Token Registry for all assets on Zigchain

CREATE TABLE IF NOT EXISTS tokens.registry (
    denom             TEXT PRIMARY KEY,  -- Unique identifier (e.g., 'uzig', 'coin.zig...', 'ibc/...')
    type              TEXT NOT NULL,     -- 'native', 'factory', 'cw20', 'ibc'
    base_denom        TEXT,              -- Human readable symbol (uzig, stzig, etc.)
    symbol            TEXT,              -- Display symbol
    decimals          INT DEFAULT 6,     -- Precision
    creator           TEXT,              -- Contract address or wallet
    contract_address  TEXT,              -- Mandatory for 'cw20'
    first_seen_height BIGINT,
    first_seen_tx     TEXT,
    metadata          JSONB,             -- For extra info like URI, description, or IBC path
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_token_registry_type ON tokens.registry(type);
CREATE INDEX IF NOT EXISTS idx_token_registry_symbol ON tokens.registry(symbol);
CREATE INDEX IF NOT EXISTS idx_token_registry_height ON tokens.registry(first_seen_height);

-- Ordered View for easier querying
CREATE OR REPLACE VIEW tokens.registry_view AS
SELECT * FROM tokens.registry
ORDER BY first_seen_height ASC, denom ASC;
