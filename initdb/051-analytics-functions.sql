-- initdb/051-analytics-functions.sql
-- Materialization Functions for Analytics Tables
-- Run these periodically via pg_cron or external scheduler

-- [DELETED] REFRESH SWAP VOLUME HOURLY
-- [DELETED] REFRESH TRANSFER VOLUME DAILY
-- [DELETED] REFRESH ACTIVE WALLETS DAILY

-- [DELETED] REFRESH TOKEN HOLDER COUNTS

-- ============================================================================
-- 7. MASTER REFRESH FUNCTION (STUB)
-- ============================================================================
CREATE OR REPLACE FUNCTION util.refresh_all_analytics(p_days INT DEFAULT 7)
RETURNS void AS $$
BEGIN
    -- This function is now a stub as all analytics tables have been removed.
    RAISE NOTICE '[analytics] All analytics logic has been decommissioned.';
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 8. SCHEDULE WITH PG_CRON (Optional - Run Manually if pg_cron not installed)
-- ============================================================================
-- Uncomment below if pg_cron extension is available:
-- SELECT cron.schedule('refresh_analytics_daily', '0 0 * * *', 'SELECT util.refresh_all_analytics(7)');
