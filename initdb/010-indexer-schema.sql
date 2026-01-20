-- initdb/010-indexer-schema.sql
-- FIXED: Added missing table 'wasm.events' to prevent crash

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS bank;
CREATE SCHEMA IF NOT EXISTS stake;
CREATE SCHEMA IF NOT EXISTS gov;
CREATE SCHEMA IF NOT EXISTS ibc;
CREATE SCHEMA IF NOT EXISTS wasm;
CREATE SCHEMA IF NOT EXISTS authz_feegrant;
CREATE SCHEMA IF NOT EXISTS groups;
CREATE SCHEMA IF NOT EXISTS tokens;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Enums
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ibc_packet_status') THEN
        CREATE TYPE ibc_packet_status AS ENUM ('sent','received','acknowledged','timeout','failed');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'proposal_status') THEN
        CREATE TYPE proposal_status AS ENUM ('deposit_period','voting_period','passed','rejected','failed','withdrawn');
    END IF;
END $$;

-- ============================================================================
-- 1) BLOCKS & VALIDATORS
-- ============================================================================
CREATE TABLE core.blocks (
    height       BIGINT PRIMARY KEY,
    block_hash   TEXT NOT NULL,
    time         TIMESTAMPTZ NOT NULL,
    proposer_address TEXT,
    tx_count     INT NOT NULL,
    size_bytes   INT NULL,
    last_commit_hash TEXT NULL,
    data_hash    TEXT NULL,
    evidence_count INT DEFAULT 0,
    app_hash     TEXT NULL
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS core.blocks_p0 PARTITION OF core.blocks FOR VALUES FROM (0) TO (1000000);

CREATE INDEX IF NOT EXISTS idx_blocks_time ON core.blocks USING BTREE (time);
CREATE INDEX IF NOT EXISTS idx_blocks_proposer ON core.blocks (proposer_address);

CREATE TABLE core.validators (
    operator_address    TEXT PRIMARY KEY,
    consensus_address   TEXT UNIQUE,
    consensus_pubkey    TEXT,
    moniker             TEXT,
    website             TEXT,
    details             TEXT,
    commission_rate     NUMERIC(20, 18),
    max_commission_rate NUMERIC(20, 18),
    max_change_rate     NUMERIC(20, 18),
    min_self_delegation NUMERIC(64, 0),
    status              TEXT,
    updated_at_height   BIGINT,
    updated_at_time     TIMESTAMPTZ
);

CREATE TABLE core.validator_set (
    height            BIGINT NOT NULL,
    operator_address  TEXT   NOT NULL,
    voting_power      BIGINT NOT NULL,
    proposer_priority BIGINT NULL,
    PRIMARY KEY (height, operator_address)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS core.validator_set_p0 PARTITION OF core.validator_set FOR VALUES FROM (0) TO (1000000);

CREATE TABLE core.validator_missed_blocks (
    operator_address TEXT   NOT NULL,
    height           BIGINT NOT NULL,
    PRIMARY KEY (operator_address, height)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS core.validator_missed_blocks_p0 PARTITION OF core.validator_missed_blocks FOR VALUES FROM (0) TO (1000000);

-- ============================================================================
-- 2) TRANSACTIONS / MESSAGES / EVENTS
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.transactions (
    tx_hash    TEXT        NOT NULL,
    height     BIGINT      NOT NULL,
    tx_index   INT         NOT NULL,
    code       INT         NOT NULL,
    gas_wanted BIGINT      NULL,
    gas_used   BIGINT      NULL,
    fee        JSONB       NULL,
    memo       TEXT        NULL,
    signers    TEXT[]      NULL,
    raw_tx     JSONB       NULL,
    log_summary TEXT       NULL,
    time       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (height, tx_hash)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS core.transactions_p0 PARTITION OF core.transactions FOR VALUES FROM (0) TO (1000000);
CREATE INDEX IF NOT EXISTS idx_txs_hash ON core.transactions (tx_hash);

CREATE TABLE IF NOT EXISTS core.messages (
    tx_hash   TEXT   NOT NULL,
    msg_index INT    NOT NULL,
    height    BIGINT NOT NULL,
    type_url  TEXT   NOT NULL,
    value     JSONB  NOT NULL,
    signer    TEXT   NULL,
    PRIMARY KEY (height, tx_hash, msg_index)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS core.messages_p0 PARTITION OF core.messages FOR VALUES FROM (0) TO (1000000);

CREATE TABLE core.events (
    tx_hash     TEXT  NOT NULL,
    msg_index   INT   NOT NULL,
    event_index INT   NOT NULL,
    event_type  TEXT  NOT NULL,
    attributes  JSONB NOT NULL,
    height      BIGINT NOT NULL,
    PRIMARY KEY (tx_hash, msg_index, event_index)
) PARTITION BY HASH (tx_hash);

CREATE TABLE core.event_attrs (
    tx_hash     TEXT NOT NULL,
    msg_index   INT  NOT NULL,
    event_index INT  NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT NULL,
    height      BIGINT NOT NULL,
    PRIMARY KEY (height, tx_hash, msg_index, event_index, key)
) PARTITION BY RANGE (height);

-- Tracks heights that failed RPC fetch/processing after retries.
CREATE TABLE IF NOT EXISTS core.missing_blocks (
    height      BIGINT PRIMARY KEY,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts    INT NOT NULL DEFAULT 1,
    last_error  TEXT NULL,
    status      TEXT NOT NULL DEFAULT 'missing',
    resolved_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_missing_blocks_status ON core.missing_blocks(status);
CREATE INDEX IF NOT EXISTS idx_missing_blocks_last_seen ON core.missing_blocks(last_seen DESC);

-- ============================================================================
-- 3) BANK & STAKE
-- ============================================================================
CREATE TABLE IF NOT EXISTS bank.transfers (
    tx_hash   TEXT           NOT NULL,
    msg_index INT            NOT NULL,
    from_addr TEXT           NOT NULL,
    to_addr   TEXT           NOT NULL,
    denom     TEXT           NOT NULL,
    amount    NUMERIC(80, 0) NOT NULL,
    height    BIGINT         NOT NULL,
    PRIMARY KEY (height, tx_hash, msg_index, from_addr, to_addr, denom)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS bank.transfers_p0 PARTITION OF bank.transfers FOR VALUES FROM (0) TO (1000000);

CREATE TABLE bank.balance_deltas (
    height  BIGINT         NOT NULL,
    account TEXT           NOT NULL,
    denom   TEXT           NOT NULL,
    delta   NUMERIC(80, 0) NOT NULL,
    PRIMARY KEY (height, account, denom)
) PARTITION BY RANGE (height);

CREATE TABLE bank.balances_current (
    account  TEXT PRIMARY KEY,
    balances JSONB NOT NULL
) WITH (FILLFACTOR = 80);

CREATE TABLE stake.delegation_events (
    height            BIGINT         NOT NULL,
    tx_hash           TEXT           NOT NULL,
    msg_index         INT            NOT NULL,
    event_type        TEXT           NOT NULL,
    delegator_address TEXT           NOT NULL,
    validator_src     TEXT           NULL,
    validator_dst     TEXT           NULL,
    denom             TEXT           NOT NULL,
    amount            NUMERIC(80, 0) NOT NULL,
    completion_time   TIMESTAMPTZ    NULL,
    PRIMARY KEY (height, tx_hash, msg_index)
) PARTITION BY RANGE (height);

CREATE TABLE stake.delegations_current (
    delegator_address TEXT           NOT NULL,
    validator_address TEXT           NOT NULL,
    denom             TEXT           NOT NULL,
    amount            NUMERIC(80, 0) NOT NULL,
    PRIMARY KEY (delegator_address, validator_address, denom)
) WITH (FILLFACTOR = 80);

CREATE TABLE stake.distribution_events (
    height            BIGINT         NOT NULL,
    tx_hash           TEXT           NOT NULL,
    msg_index         INT            NOT NULL,
    event_type        TEXT           NOT NULL, 
    delegator_address TEXT           NULL,
    validator_address TEXT           NULL,
    denom             TEXT           NULL,
    amount            NUMERIC(80, 0) NULL,
    withdraw_address  TEXT           NULL,
    PRIMARY KEY (height, tx_hash, msg_index)
) PARTITION BY RANGE (height);

-- ============================================================================
-- 4) GOVERNANCE
-- ============================================================================
CREATE TABLE gov.proposals (
    proposal_id   BIGINT PRIMARY KEY,
    submitter     TEXT NULL,
    title         TEXT NOT NULL,
    summary       TEXT NULL,
    proposal_type TEXT NULL,
    status        proposal_status NOT NULL,
    deposit_end   TIMESTAMPTZ NULL,
    voting_start  TIMESTAMPTZ NULL,
    voting_end    TIMESTAMPTZ NULL,
    total_deposit JSONB NULL,
    changes       JSONB NULL,
    submit_time   TIMESTAMPTZ NULL
);

CREATE TABLE gov.deposits (
    proposal_id BIGINT         NOT NULL,
    depositor   TEXT           NOT NULL,
    denom       TEXT           NOT NULL,
    amount      NUMERIC(80, 0) NOT NULL,
    height      BIGINT         NOT NULL,
    tx_hash     TEXT           NOT NULL,
    PRIMARY KEY (proposal_id, depositor, denom, height, tx_hash)
) PARTITION BY RANGE (height);

CREATE TABLE gov.votes (
    proposal_id BIGINT          NOT NULL,
    voter       TEXT            NOT NULL,
    option      TEXT            NOT NULL,
    weight      NUMERIC(20, 18) NULL,
    height      BIGINT          NOT NULL,
    tx_hash     TEXT            NOT NULL,
    PRIMARY KEY (proposal_id, voter, height, tx_hash)
) PARTITION BY RANGE (height);

-- ============================================================================
-- 5) IBC
-- ============================================================================
CREATE TABLE ibc.channels (
    port_id          TEXT NOT NULL,
    channel_id       TEXT NOT NULL,
    state            TEXT NULL,
    ordering         TEXT NULL,
    connection_hops  TEXT[] NULL,
    counterparty_port TEXT NULL,
    counterparty_channel TEXT NULL,
    version          TEXT NULL,
    PRIMARY KEY (port_id, channel_id)
);

CREATE TABLE ibc.clients (
    client_id        TEXT PRIMARY KEY,
    chain_id         TEXT NULL,
    client_type      TEXT NULL,
    updated_at_height BIGINT NULL,
    updated_at_time   TIMESTAMPTZ NULL
);

CREATE TABLE ibc.denoms (
    hash             TEXT PRIMARY KEY, -- e.g. ibc/6490A7...
    full_path        TEXT NOT NULL,    -- e.g. transfer/channel-3/uusdc
    base_denom       TEXT NOT NULL     -- e.g. uusdc
);

CREATE TABLE ibc.packets (
    port_id_src      TEXT NOT NULL,
    channel_id_src   TEXT NOT NULL,
    sequence         BIGINT NOT NULL,
    port_id_dst      TEXT NULL,
    channel_id_dst   TEXT NULL,
    timeout_height   TEXT NULL,
    timeout_ts       TEXT NULL,
    status           ibc_packet_status NOT NULL DEFAULT 'sent',
    -- Send info
    tx_hash_send     TEXT NULL,
    height_send      BIGINT NULL,
    time_send        TIMESTAMPTZ NULL,
    -- Recv info
    tx_hash_recv     TEXT NULL,
    height_recv      BIGINT NULL,
    time_recv        TIMESTAMPTZ NULL,
    -- Ack info
    tx_hash_ack      TEXT NULL,
    height_ack       BIGINT NULL,
    time_ack         TIMESTAMPTZ NULL,
    ack_success      BOOLEAN NULL,
    ack_error        TEXT NULL,
    -- Timeout info
    tx_hash_timeout  TEXT NULL,
    height_timeout   BIGINT NULL,
    time_timeout     TIMESTAMPTZ NULL,
    -- Metadata
    relayer_send     TEXT NULL,
    relayer_recv     TEXT NULL,
    relayer_ack      TEXT NULL,
    denom            TEXT NULL,
    amount           NUMERIC(80, 0) NULL,
    sender           TEXT NULL,
    receiver         TEXT NULL,
    memo             TEXT NULL,
    PRIMARY KEY (port_id_src, channel_id_src, sequence)
);

CREATE TABLE ibc.transfers (
    port_id_src      TEXT NOT NULL,
    channel_id_src   TEXT NOT NULL,
    sequence         BIGINT NOT NULL,
    port_id_dst      TEXT NULL,
    channel_id_dst   TEXT NULL,
    sender           TEXT NULL,
    receiver         TEXT NULL,
    denom            TEXT NULL,
    amount           NUMERIC(80, 0) NULL,
    memo             TEXT NULL,
    timeout_height   TEXT NULL,
    timeout_ts       TEXT NULL,
    status           ibc_packet_status NOT NULL DEFAULT 'sent',
    -- Send info
    tx_hash_send     TEXT NULL,
    height_send      BIGINT NULL,
    time_send        TIMESTAMPTZ NULL,
    -- Recv info
    tx_hash_recv     TEXT NULL,
    height_recv      BIGINT NULL,
    time_recv        TIMESTAMPTZ NULL,
    -- Ack info
    tx_hash_ack      TEXT NULL,
    height_ack       BIGINT NULL,
    time_ack         TIMESTAMPTZ NULL,
    ack_success      BOOLEAN NULL,
    ack_error        TEXT NULL,
    -- Timeout info
    tx_hash_timeout  TEXT NULL,
    height_timeout   BIGINT NULL,
    time_timeout     TIMESTAMPTZ NULL,
    -- Relayers
    relayer_send     TEXT NULL,
    relayer_recv     TEXT NULL,
    relayer_ack      TEXT NULL,
    PRIMARY KEY (port_id_src, channel_id_src, sequence)
);

-- IBC Connections (Channel → Client mapping)
CREATE TABLE ibc.connections (
    connection_id              TEXT PRIMARY KEY,
    client_id                  TEXT NOT NULL,
    counterparty_connection_id TEXT NULL,
    counterparty_client_id     TEXT NULL,
    state                      TEXT NULL
);

-- ============================================================================
-- IBC INDEXES (Performance)
-- ============================================================================
CREATE INDEX idx_ibc_packets_status ON ibc.packets(status);
CREATE INDEX idx_ibc_packets_channel ON ibc.packets(channel_id_src);
CREATE INDEX idx_ibc_transfers_sender ON ibc.transfers(sender);
CREATE INDEX idx_ibc_transfers_receiver ON ibc.transfers(receiver);
CREATE INDEX idx_ibc_transfers_status ON ibc.transfers(status);
CREATE INDEX idx_ibc_transfers_channel ON ibc.transfers(channel_id_src);
CREATE INDEX idx_ibc_transfers_denom ON ibc.transfers(denom);

-- ============================================================================
-- 6) WASM
-- ============================================================================
CREATE TABLE wasm.codes (
    code_id        BIGINT PRIMARY KEY,
    checksum       TEXT   NOT NULL,
    creator        TEXT   NULL,
    instantiate_permission JSONB NULL,
    store_tx_hash  TEXT   NULL,
    store_height   BIGINT NULL
);

CREATE TABLE wasm.contracts (
    address        TEXT PRIMARY KEY,
    code_id        BIGINT NOT NULL,  -- Removed FK: code may not exist if indexing from later block
    creator        TEXT   NULL,
    admin          TEXT   NULL,
    label          TEXT   NULL,
    created_height BIGINT NULL,
    created_tx_hash TEXT  NULL
);

CREATE TABLE wasm.contract_migrations (
    contract     TEXT   NOT NULL,  -- Removed FK: contract may not exist if indexing from later block
    from_code_id BIGINT NULL,
    to_code_id   BIGINT NOT NULL,
    height       BIGINT NOT NULL,
    tx_hash      TEXT   NOT NULL,
    PRIMARY KEY (contract, height, tx_hash)
) PARTITION BY RANGE (height);

CREATE TABLE IF NOT EXISTS wasm.executions (
    tx_hash   TEXT    NOT NULL,
    msg_index INT     NOT NULL,
    contract  TEXT    NOT NULL,
    caller    TEXT    NULL,
    funds     JSONB   NULL,
    msg       JSONB   NOT NULL,
    success   BOOLEAN NOT NULL,
    error     TEXT    NULL,
    gas_used  BIGINT  NULL,
    height    BIGINT  NOT NULL,
    PRIMARY KEY (height, tx_hash, msg_index)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS wasm.executions_p0 PARTITION OF wasm.executions FOR VALUES FROM (0) TO (1000000);

-- 👇👇 MISSING TABLE ADDED 👇👇
CREATE TABLE IF NOT EXISTS wasm.events (
    contract    TEXT NOT NULL,
    height      BIGINT NOT NULL,
    tx_hash     TEXT NOT NULL,
    msg_index   INT NOT NULL,
    event_index INT NOT NULL,
    event_type  TEXT NOT NULL,
    attributes  JSONB NOT NULL,
    PRIMARY KEY (height, tx_hash, msg_index, event_index)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS wasm.events_p0 PARTITION OF wasm.events FOR VALUES FROM (0) TO (1000000);

CREATE TABLE IF NOT EXISTS wasm.event_attrs (
    contract    TEXT NOT NULL,
    height      BIGINT NOT NULL,
    tx_hash     TEXT NOT NULL,
    msg_index   INT NOT NULL,
    event_index INT NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT NULL,
    PRIMARY KEY (height, tx_hash, msg_index, event_index, key)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS wasm.event_attrs_p0 PARTITION OF wasm.event_attrs FOR VALUES FROM (0) TO (1000000);

CREATE TABLE wasm.state_kv (
    contract   TEXT   NOT NULL,
    key        BYTEA  NOT NULL,
    height     BIGINT NOT NULL,
    value      BYTEA  NOT NULL,
    PRIMARY KEY (contract, key, height)
) PARTITION BY RANGE (height);

-- ============================================================================
-- 7) AUTHZ / FEEGRANT
-- ============================================================================
CREATE TABLE authz_feegrant.authz_grants (
    granter      TEXT        NOT NULL,
    grantee      TEXT        NOT NULL,
    msg_type_url TEXT        NOT NULL,
    expiration   TIMESTAMPTZ NULL,
    height       BIGINT      NOT NULL,
    revoked      BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (granter, grantee, msg_type_url, height)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS authz_feegrant.authz_grants_p0 PARTITION OF authz_feegrant.authz_grants FOR VALUES FROM (0) TO (1000000);

CREATE TABLE authz_feegrant.fee_grants (
    granter    TEXT        NOT NULL,
    grantee    TEXT        NOT NULL,
    allowance  JSONB       NULL,
    expiration TIMESTAMPTZ NULL,
    height     BIGINT      NOT NULL,
    revoked    BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (granter, grantee, height)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS authz_feegrant.fee_grants_p0 PARTITION OF authz_feegrant.fee_grants FOR VALUES FROM (0) TO (1000000);

CREATE INDEX IF NOT EXISTS idx_authz_grants_grantee ON authz_feegrant.authz_grants (grantee, height DESC);
CREATE INDEX IF NOT EXISTS idx_fee_grants_grantee ON authz_feegrant.fee_grants (grantee, height DESC);

-- ============================================================================
-- 8) TOKENS (CW20)
-- ============================================================================
CREATE TABLE IF NOT EXISTS tokens.cw20_transfers (
    contract  TEXT           NOT NULL,
    from_addr TEXT           NOT NULL,
    to_addr   TEXT           NOT NULL,
    amount    NUMERIC(80, 0) NOT NULL,
    height    BIGINT         NOT NULL,
    tx_hash   TEXT           NOT NULL,
    PRIMARY KEY (height, tx_hash, contract, from_addr, to_addr)
) PARTITION BY RANGE (height);
CREATE TABLE IF NOT EXISTS tokens.cw20_transfers_p0 PARTITION OF tokens.cw20_transfers FOR VALUES FROM (0) TO (1000000);

CREATE INDEX IF NOT EXISTS idx_cw20_from ON tokens.cw20_transfers (contract, from_addr, height DESC);
CREATE INDEX IF NOT EXISTS idx_cw20_to ON tokens.cw20_transfers (contract, to_addr, height DESC);
CREATE INDEX IF NOT EXISTS idx_cw20_brin ON tokens.cw20_transfers USING BRIN (height);
CREATE INDEX IF NOT EXISTS idx_cw20_tx ON tokens.cw20_transfers (tx_hash);

-- Optional snapshots: maintain via periodic job
CREATE TABLE IF NOT EXISTS tokens.cw20_balances_current (
    contract TEXT           NOT NULL,
    account  TEXT           NOT NULL,
    balance  NUMERIC(80, 0) NOT NULL,
    PRIMARY KEY (contract, account)
);

-- ============================================================================
-- 9) NETWORK PARAMS
-- ============================================================================
CREATE TABLE core.network_params (
    height    BIGINT PRIMARY KEY,
    time      TIMESTAMPTZ NOT NULL,
    module    TEXT        NOT NULL,
    param_key TEXT        NOT NULL,
    old_value JSONB       NULL,
    new_value JSONB       NOT NULL
) PARTITION BY RANGE (height);

-- ============================================================================
-- 10) QUERY-PATTERN INDEXES (For 5.4M+ scale)
-- ============================================================================
-- Transfers by address (common API query)
CREATE INDEX IF NOT EXISTS idx_transfers_from ON bank.transfers (from_addr, height DESC);
CREATE INDEX IF NOT EXISTS idx_transfers_to ON bank.transfers (to_addr, height DESC);

-- Delegation events by delegator (staking dashboard)
CREATE INDEX IF NOT EXISTS idx_delegation_delegator ON stake.delegation_events (delegator_address, height DESC);
CREATE INDEX IF NOT EXISTS idx_distribution_delegator ON stake.distribution_events (delegator_address, height DESC);

-- Messages by type (explorer query)
CREATE INDEX IF NOT EXISTS idx_messages_type ON core.messages (type_url, height DESC);
CREATE INDEX IF NOT EXISTS idx_messages_signer ON core.messages (signer, height DESC);

-- WASM by contract (dApp queries)
CREATE INDEX IF NOT EXISTS idx_wasm_exec_contract ON wasm.executions (contract, height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_events_contract ON wasm.events (contract, height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_event_attrs_contract ON wasm.event_attrs (contract, height DESC);

-- IBC by channel (relayer/bridge queries)
CREATE INDEX IF NOT EXISTS idx_ibc_channel ON ibc.packets (channel_id_src, status);
CREATE INDEX IF NOT EXISTS idx_ibc_transfers_sender ON ibc.transfers (sender, sequence DESC);
CREATE INDEX IF NOT EXISTS idx_ibc_transfers_receiver ON ibc.transfers (receiver, sequence DESC);
CREATE INDEX IF NOT EXISTS idx_ibc_transfers_denom ON ibc.transfers (denom, sequence DESC);

-- Gov by proposal (governance dashboard)
CREATE INDEX IF NOT EXISTS idx_gov_votes_proposal ON gov.votes (proposal_id, height DESC);
CREATE INDEX IF NOT EXISTS idx_gov_deposits_proposal ON gov.deposits (proposal_id, height DESC);
