-- Target State: Optimized & Governed

-- 1. Optimized Storage: Clustering
CREATE TABLE raw_events (
    event_id VARCHAR(36),
    event_timestamp TIMESTAMP_NTZ,
    event_type VARCHAR(50),
    user_id VARCHAR(36),
    payload VARIANT,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (TO_DATE(event_timestamp));

-- 2. Governance: Tags
ALTER TABLE raw_events SET TAG cost_center = 'analytics_prod';

-- 3. Modern Pipelines: Dynamic Table
CREATE DYNAMIC TABLE daily_metrics
    TARGET_LAG = '1 hour'
    WAREHOUSE = 'compute_wh'
AS
SELECT
    TO_DATE(event_timestamp) AS metric_date,
    event_type,
    COUNT(*) AS event_count
FROM raw_events
GROUP BY 1, 2;
