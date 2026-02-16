-- initdb/046-validator-uptime-view.sql
-- Rolling uptime calculation for the last 10,000 blocks

CREATE OR REPLACE VIEW analytics.validator_uptime AS
WITH block_range AS (
    -- Get the range of the last 10,000 blocks to calculate a rolling average
    SELECT 
        COALESCE(MAX(height), 0) - 10000 as start_height,
        COALESCE(MAX(height), 0) as end_height
    FROM core.blocks
),
validator_stats AS (
    SELECT 
        vs.consensus_address,
        COUNT(vs.height) as expected_blocks,
        COUNT(mb.height) as missed_blocks
    FROM core.validator_set vs
    JOIN block_range br ON vs.height BETWEEN br.start_height AND br.end_height
    LEFT JOIN core.validator_missed_blocks mb 
        ON vs.consensus_address = mb.consensus_address AND vs.height = mb.height
    GROUP BY vs.consensus_address
)
SELECT 
    v.moniker,
    v.operator_address,
    v.consensus_address,
    s.expected_blocks,
    s.missed_blocks,
    CASE 
        WHEN s.expected_blocks > 0 THEN 
            ROUND((1.0 - (s.missed_blocks::numeric / s.expected_blocks::numeric)) * 100, 4)
        ELSE 0 
    END as uptime_percentage
FROM validator_stats s
JOIN core.validators v ON s.consensus_address = v.consensus_address
ORDER BY uptime_percentage DESC;
