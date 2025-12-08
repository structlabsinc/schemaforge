/********************************************************************************
 * FILENAME: sqlite_god.sql
 * DIALECT: SQLite
 * COMPLEXITY: GOD LEVEL (Tier 1)
 * FEATURES: 
 * - STRICT Tables
 * - WITHOUT ROWID
 * - GENERATED COLUMNS
 * - PARTIAL INDEXES
 * - RECURSIVE CTEs
 * - JSON Functions (Check Constraints)
 * - FTS5 (Virtual Tables)
 ********************************************************************************/

-- ==============================================================================
-- 1. BASE TABLES
-- ==============================================================================

-- Table 1: Strict Mode & Generated Columns
CREATE TABLE events_omniverse (
    event_uuid TEXT NOT NULL PRIMARY KEY,
    timestamp TEXT NOT NULL, -- ISO8601
    actor_id TEXT,
    payload TEXT, -- JSON stored as text
    -- Generated Column extracting from JSON
    action_type TEXT GENERATED ALWAYS AS (json_extract(payload, '$.action')) VIRTUAL,
    CONSTRAINT check_json CHECK (json_valid(payload))
) STRICT;

-- Table 2: Without RowID & Partial Index
CREATE TABLE dim_hyper_entity (
    entity_id INTEGER PRIMARY KEY,
    parent_entity_id INTEGER,
    attributes TEXT,
    is_active INTEGER DEFAULT 1,
    FOREIGN KEY (parent_entity_id) REFERENCES dim_hyper_entity(entity_id)
) WITHOUT ROWID;

CREATE INDEX idx_active_entities ON dim_hyper_entity(entity_id) WHERE is_active = 1;

-- Table 3: Virtual Table (FTS5)
CREATE VIRTUAL TABLE search_index USING fts5(
    content, 
    tokenize='porter'
);

-- ==============================================================================
-- 2. VIEWS
-- ==============================================================================

-- VIEW 001: Recursive CTE
CREATE VIEW v_complex_hierarchy AS
WITH RECURSIVE entity_path(entity_id, parent_entity_id, path, depth) AS (
    SELECT 
        entity_id, 
        parent_entity_id, 
        CAST(entity_id AS TEXT), 
        1
    FROM dim_hyper_entity
    WHERE parent_entity_id IS NULL
    
    UNION ALL
    
    SELECT 
        c.entity_id, 
        c.parent_entity_id, 
        p.path || ',' || c.entity_id, 
        p.depth + 1
    FROM dim_hyper_entity c
    JOIN entity_path p ON c.parent_entity_id = p.entity_id
    WHERE p.depth < 20
)
SELECT * FROM entity_path;

-- VIEW 002: Window Functions
CREATE VIEW v_financial_stats AS
SELECT 
    actor_id,
    action_type,
    COUNT(*) OVER (PARTITION BY actor_id) as actor_event_count,
    ROW_NUMBER() OVER (PARTITION BY action_type ORDER BY timestamp DESC) as recent_rank
FROM events_omniverse;

-- VIEW 003: JSON Extraction
CREATE VIEW v_json_props AS
SELECT 
    entity_id,
    json_extract(attributes, '$.name') as name,
    json_type(attributes, '$.tags') as tags_type
FROM dim_hyper_entity;
