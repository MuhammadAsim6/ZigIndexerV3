-- initdb/051-analytics-functions.sql
-- Materialization Functions for Analytics Tables
-- Run these periodically via pg_cron or external scheduler

-- [DELETED] REFRESH SWAP VOLUME HOURLY
-- [DELETED] REFRESH TRANSFER VOLUME DAILY
-- [DELETED] REFRESH ACTIVE WALLETS DAILY

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

-- [DELETED] REFRESH GAS STATS DAILY
-- [DELETED] REFRESH MESSAGE TYPE STATS DAILY

-- ============================================================================
-- 7. MASTER REFRESH FUNCTION
-- ============================================================================
CREATE OR REPLACE FUNCTION util.refresh_all_analytics(p_days INT DEFAULT 7)
RETURNS void AS $$
BEGIN
    -- Only one analytic remains
    PERFORM util.refresh_token_holder_counts();
    RAISE NOTICE '[analytics] All analytics refreshed successfully.';
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 8. SCHEDULE WITH PG_CRON (Optional - Run Manually if pg_cron not installed)
-- ============================================================================
-- Uncomment below if pg_cron extension is available:
-- SELECT cron.schedule('refresh_analytics_daily', '0 0 * * *', 'SELECT util.refresh_all_analytics(7)');
