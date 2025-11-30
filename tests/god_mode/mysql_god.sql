/********************************************************************************
 * FILENAME: mysql_god.sql
 * DIALECT: MySQL
 * COMPLEXITY: GOD LEVEL (Tier 1)
 * FEATURES: 
 * - JSON Data Type
 * - PARTITIONING (Range)
 * - GENERATED COLUMNS (Virtual/Stored)
 * - SPATIAL TYPES (Point, Polygon)
 * - WINDOW FUNCTIONS
 * - CTEs (Recursive)
 ********************************************************************************/

-- ==============================================================================
-- 1. BASE TABLES
-- ==============================================================================

-- Table 1: Events with JSON and Partitioning
CREATE TABLE events_omniverse (
    event_uuid CHAR(36) NOT NULL,
    timestamp DATETIME(6) NOT NULL,
    actor_id VARCHAR(32),
    payload JSON,
    geo_location POINT, -- Spatial type
    PRIMARY KEY (event_uuid, timestamp)
) 
PARTITION BY RANGE (YEAR(timestamp)) (
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Table 2: Hierarchy with Generated Columns
CREATE TABLE dim_hyper_entity (
    entity_id INT AUTO_INCREMENT PRIMARY KEY,
    parent_entity_id INT,
    attributes JSON,
    -- Virtual Generated Column extracting from JSON
    entity_name VARCHAR(100) GENERATED ALWAYS AS (attributes->>'$.name') VIRTUAL,
    -- Stored Generated Column
    created_at_ts BIGINT GENERATED ALWAYS AS (attributes->>'$.created_ts') STORED,
    INDEX idx_entity_name (entity_name)
);

-- Table 3: Ledger
CREATE TABLE fact_ledger_atomic (
    txn_id CHAR(36) PRIMARY KEY,
    account_id INT,
    amount DECIMAL(20, 4),
    currency CHAR(3),
    txn_date DATE,
    INDEX idx_account_date (account_id, txn_date)
);

-- ==============================================================================
-- 2. VIEWS
-- ==============================================================================

-- VIEW 001: Recursive CTE (MySQL 8.0+)
CREATE OR REPLACE VIEW v_complex_hierarchy AS
WITH RECURSIVE entity_path (entity_id, parent_entity_id, path, depth) AS (
    SELECT 
        entity_id, 
        parent_entity_id, 
        CAST(entity_id AS CHAR(200)), 
        1
    FROM dim_hyper_entity
    WHERE parent_entity_id IS NULL
    
    UNION ALL
    
    SELECT 
        c.entity_id, 
        c.parent_entity_id, 
        CONCAT(p.path, ',', c.entity_id), 
        p.depth + 1
    FROM dim_hyper_entity c
    JOIN entity_path p ON c.parent_entity_id = p.entity_id
    WHERE p.depth < 20
)
SELECT * FROM entity_path;

-- VIEW 002: Window Functions
CREATE OR REPLACE VIEW v_financial_stats AS
SELECT 
    account_id,
    currency,
    amount,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_date) as running_total,
    LEAD(amount, 1) OVER (PARTITION BY account_id ORDER BY txn_date) as next_txn_amount
FROM fact_ledger_atomic;

-- VIEW 003: JSON Table Function (Simulated via JSON_EXTRACT)
CREATE OR REPLACE VIEW v_json_props AS
SELECT 
    entity_id,
    JSON_UNQUOTE(JSON_EXTRACT(attributes, '$.type')) as type,
    JSON_LENGTH(attributes) as prop_count
FROM dim_hyper_entity
WHERE JSON_CONTAINS(attributes, '"active"', '$.status');
