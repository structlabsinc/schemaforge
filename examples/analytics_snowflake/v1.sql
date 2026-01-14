-- Initial State: Raw ingestion table
CREATE TABLE raw_events (
    event_id VARCHAR(36),
    event_timestamp TIMESTAMP_NTZ,
    event_type VARCHAR(50),
    user_id VARCHAR(36),
    payload VARIANT,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
