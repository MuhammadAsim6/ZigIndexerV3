-- initdb/051-analytics-functions.sql
-- Materialization Functions for Analytics Tables
-- Run these periodically via pg_cron or external scheduler

-- ============================================================================
-- 1. REFRESH SWAP VOLUME HOURLY
-- ============================================================================
CREATE OR REPLACE FUNCTION util.refresh_swap_volume_hourly(p_hours INT DEFAULT 24)
RETURNS void AS $$
BEGIN
    -- Delete existing data for the refresh window
    DELETE FROM analytics.swap_volume_hourly
    WHERE hour >= date_trunc('hour', NOW() - (p_hours || ' hours')::INTERVAL);

    -- Insert fresh aggregates
    INSERT INTO analytics.swap_volume_hourly (hour, contract, offer_asset, ask_asset, swap_count, total_offer, total_return, total_commission, avg_price)
    SELECT
        date_trunc('hour', timestamp) AS hour,
        contract,
        offer_asset,
        ask_asset,
        COUNT(*) AS swap_count,
        SUM(COALESCE(offer_amount, 0)) AS total_offer,
        SUM(COALESCE(return_amount, 0)) AS total_return,
        SUM(COALESCE(commission_amount, 0)) AS total_commission,
        AVG(NULLIF(effective_price, 0)) AS avg_price
    FROM wasm.dex_swaps
    WHERE timestamp >= date_trunc('hour', NOW() - (p_hours || ' hours')::INTERVAL)
    GROUP BY 1, 2, 3, 4
    ON CONFLICT (hour, contract, offer_asset, ask_asset) DO UPDATE SET
        swap_count = EXCLUDED.swap_count,
        total_offer = EXCLUDED.total_offer,
        total_return = EXCLUDED.total_return,
        total_commission = EXCLUDED.total_commission,
        avg_price = EXCLUDED.avg_price;

    RAISE NOTICE '[analytics] Refreshed swap_volume_hourly for last % hours', p_hours;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 2. REFRESH TRANSFER VOLUME DAILY
-- ============================================================================
CREATE OR REPLACE FUNCTION util.refresh_transfer_volume_daily(p_days INT DEFAULT 7)
RETURNS void AS $$
DECLARE
    v_start_date DATE;
BEGIN
    v_start_date := CURRENT_DATE - p_days;

    DELETE FROM analytics.transfer_volume_daily
    WHERE day >= v_start_date;

    INSERT INTO analytics.transfer_volume_daily (day, denom, transfer_count, total_amount, unique_senders, unique_receivers)
    SELECT
        b.time::DATE AS day,
        t.denom,
        COUNT(*) AS transfer_count,
        SUM(t.amount) AS total_amount,
        COUNT(DISTINCT t.from_addr) AS unique_senders,
        COUNT(DISTINCT t.to_addr) AS unique_receivers
    FROM bank.transfers t
    JOIN core.blocks b ON t.height = b.height
    WHERE b.time >= v_start_date
    GROUP BY 1, 2
    ON CONFLICT (day, denom) DO UPDATE SET
        transfer_count = EXCLUDED.transfer_count,
        total_amount = EXCLUDED.total_amount,
        unique_senders = EXCLUDED.unique_senders,
        unique_receivers = EXCLUDED.unique_receivers;

    RAISE NOTICE '[analytics] Refreshed transfer_volume_daily for last % days', p_days;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- 3. REFRESH ACTIVE WALLETS DAILY
-- ============================================================================
CREATE OR REPLACE FUNCTION util.refresh_active_wallets_daily(p_days INT DEFAULT 7)
RETURNS void AS $$
DECLARE
    v_start_date DATE;
BEGIN
    v_start_date := CURRENT_DATE - p_days;

    DELETE FROM analytics.active_wallets_daily
    WHERE day >= v_start_date;

    INSERT INTO analytics.active_wallets_daily (day, active_wallets, tx_count, avg_gas_used)
    SELECT
        b.time::DATE AS day,
        COUNT(DISTINCT s.signer) AS active_wallets,
        COUNT(DISTINCT t.tx_hash) AS tx_count,
        AVG(t.gas_used) AS avg_gas_used
    FROM core.transactions t
    JOIN core.blocks b ON t.height = b.height
    LEFT JOIN LATERAL unnest(t.signers) AS s(signer) ON TRUE
    WHERE b.time >= v_start_date
    GROUP BY 1
    ON CONFLICT (day) DO UPDATE SET
        active_wallets = EXCLUDED.active_wallets,
        tx_count = EXCLUDED.tx_count,
        avg_gas_used = EXCLUDED.avg_gas_used;

    RAISE NOTICE '[analytics] Refreshed active_wallets_daily for last % days', p_days;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 4. REFRESH TOKEN HOLDER COUNTS
-- ============================================================================
CREATE OR REPLACE FUNCTION util.refresh_token_holder_counts()
RETURNS void AS $$
BEGIN
    TRUNCATE analytics.token_holder_counts;

    INSERT INTO analytics.token_holder_counts (denom, holder_count, total_supply, last_updated)
    SELECT
        key AS denom,
        COUNT(*) FILTER (WHERE (value::NUMERIC) > 0) AS holder_count,
        SUM((value::NUMERIC)) AS total_supply,
        NOW() AS last_updated
    FROM bank.balances_current, jsonb_each_text(balances)
    GROUP BY key;

    RAISE NOTICE '[analytics] Refreshed token_holder_counts';
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 5. REFRESH GAS STATS DAILY
-- ============================================================================
CREATE OR REPLACE FUNCTION util.refresh_gas_stats_daily(p_days INT DEFAULT 7)
RETURNS void AS $$
DECLARE
    v_start_date DATE;
BEGIN
    v_start_date := CURRENT_DATE - p_days;

    DELETE FROM analytics.gas_stats_daily
    WHERE day >= v_start_date;

    INSERT INTO analytics.gas_stats_daily (day, total_gas_used, avg_gas_per_tx, block_count)
    SELECT
        b.time::DATE AS day,
        SUM(t.gas_used) AS total_gas_used,
        AVG(t.gas_used) AS avg_gas_per_tx,
        COUNT(DISTINCT b.height) AS block_count
    FROM core.transactions t
    JOIN core.blocks b ON t.height = b.height
    WHERE b.time >= v_start_date
    GROUP BY 1
    ON CONFLICT (day) DO UPDATE SET
        total_gas_used = EXCLUDED.total_gas_used,
        avg_gas_per_tx = EXCLUDED.avg_gas_per_tx,
        block_count = EXCLUDED.block_count;

    RAISE NOTICE '[analytics] Refreshed gas_stats_daily for last % days', p_days;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 6. REFRESH MESSAGE TYPE STATS DAILY
-- ============================================================================
CREATE OR REPLACE FUNCTION util.refresh_msg_type_stats_daily(p_days INT DEFAULT 7)
RETURNS void AS $$
DECLARE
    v_start_date DATE;
BEGIN
    v_start_date := CURRENT_DATE - p_days;

    DELETE FROM analytics.msg_type_stats_daily
    WHERE day >= v_start_date;

    INSERT INTO analytics.msg_type_stats_daily (day, msg_type, msg_count)
    SELECT
        b.time::DATE AS day,
        m.type_url AS msg_type,
        COUNT(*) AS msg_count
    FROM core.messages m
    JOIN core.blocks b ON m.height = b.height
    WHERE b.time >= v_start_date
    GROUP BY 1, 2
    ON CONFLICT (day, msg_type) DO UPDATE SET
        msg_count = EXCLUDED.msg_count;

    RAISE NOTICE '[analytics] Refreshed msg_type_stats_daily for last % days', p_days;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 7. MASTER REFRESH FUNCTION
-- ============================================================================
CREATE OR REPLACE FUNCTION util.refresh_all_analytics(p_days INT DEFAULT 7)
RETURNS void AS $$
BEGIN
    PERFORM util.refresh_swap_volume_hourly(p_days * 24);
    PERFORM util.refresh_transfer_volume_daily(p_days);
    PERFORM util.refresh_active_wallets_daily(p_days);
    PERFORM util.refresh_token_holder_counts();
    PERFORM util.refresh_gas_stats_daily(p_days);
    PERFORM util.refresh_msg_type_stats_daily(p_days);
    RAISE NOTICE '[analytics] All analytics refreshed successfully.';
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 8. SCHEDULE WITH PG_CRON (Optional - Run Manually if pg_cron not installed)
-- ============================================================================
-- Uncomment below if pg_cron extension is available:
-- SELECT cron.schedule('refresh_analytics_hourly', '0 * * * *', 'SELECT util.refresh_all_analytics(1)');
-- SELECT cron.schedule('refresh_analytics_daily', '0 0 * * *', 'SELECT util.refresh_all_analytics(7)');
