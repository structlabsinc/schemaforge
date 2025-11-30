-- God Mode Adversarial Schema
-- Dialect: Snowflake (with mixed dialect traits)

-- ==== OLD_SCHEMA START ====

/* 
   Complex Schema with nested comments 
   /* Inner comment level 1 /* Level 2 */ */ 
*/

-- Table 1: Unicode and ZWSP
CREATE TABLE "u_table_Ａ" ( -- Fullwidth A
    id INT IDENTITY(1,1),
    "col​1" VARCHAR(100), -- ZWSP in name
    val_greek_α INT, -- Greek alpha
    val_latin_a INT  -- Latin a
);

-- Table 2: Mixed Quoting and Dialect Options
CREATE TABLE t_mixed_quotes (
    `backtick_col` STRING,
    "double_quoted" TEXT,
    unquoted_col INT DEFAULT 42
)
CLUSTER BY (unquoted_col)
COMMENT = $$ Multi-line 
comment with 'quotes' and "quotes" $$;

-- Table 3: Masking Policy & Tags
CREATE MASKING POLICY email_mask AS (val string) RETURNS string ->
  CASE
    WHEN current_role() IN ('ANALYST') THEN val
    ELSE '***MASKED***'
  END;

CREATE TAG cost_center;

CREATE TABLE t_sensitive (
    id INT,
    email STRING WITH MASKING POLICY email_mask,
    data VARIANT
)
WITH TAG (cost_center = 'finance');

-- Table 4: Ambiguous Types
CREATE TABLE t_ambiguous (
    c1 DOUBLE PRECISION, -- FLOAT8
    c2 CHARACTER VARYING(50), -- VARCHAR
    c3 BYTEINT, -- TINYINT
    c4 BOOLISH -- Custom alias
);

-- View Chain: V1 -> V2 -> V3 -> V4 -> V5 -> V6
CREATE VIEW v1 AS SELECT * FROM t_mixed_quotes;
CREATE VIEW v2 AS SELECT * FROM v1;
CREATE VIEW v3 AS SELECT * FROM v2;
CREATE VIEW v4 AS SELECT * FROM v3;
CREATE VIEW v5 AS SELECT * FROM v4;

-- V6: Deeply nested CTEs
CREATE VIEW v6 AS 
WITH RECURSIVE cte1 AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte1 WHERE n < 10),
cte2 AS (SELECT * FROM cte1),
cte3 AS (SELECT * FROM cte2),
cte4 AS (SELECT * FROM cte3),
cte5 AS (SELECT * FROM cte4),
cte6 AS (SELECT * FROM cte5),
cte7 AS (SELECT * FROM cte6),
cte8 AS (SELECT * FROM cte7),
cte9 AS (SELECT * FROM cte8),
cte10 AS (SELECT * FROM cte9)
SELECT * FROM cte10;

-- Procedures
CREATE PROCEDURE proc_sql()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    RETURN 'Simple SQL';
END;
$$;

CREATE PROCEDURE proc_js_dynamic()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var sql = "CREATE OR REPLACE TABLE dynamic_t (id INT);";
    // Embedded SQL in string
    var complex_str = "SELECT 'string with ; inside' FROM t";
    return {status: "done"};
$$;

-- UDFs
CREATE FUNCTION udf_sql(x INT) RETURNS INT AS 'x * 2';

CREATE FUNCTION udf_js_variant()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS $$
    return {key: "value", "sql": "SELECT 1"};
$$;

-- Tasks
CREATE TASK task_1
    WAREHOUSE = compute_wh
    SCHEDULE = '1 minute'
AS
    CALL proc_sql();

-- Streams (Case Insensitive Collision)
CREATE TABLE Orders (id INT);
CREATE TABLE orders (id INT); -- Same name in CI systems
CREATE STREAM s_Orders ON TABLE Orders;
CREATE STREAM s_orders ON TABLE orders;

-- Materialized View
CREATE MATERIALIZED VIEW mv_v6 AS SELECT * FROM v6;

-- External Table (Stub)
CREATE STAGE s1;
CREATE FILE FORMAT f1 TYPE = CSV;
CREATE EXTERNAL TABLE ext_t (col1 varchar) LOCATION=@s1 FILE_FORMAT=f1;

-- Sequences
CREATE SEQUENCE seq_1 START = 1 INCREMENT = 1;
CREATE TABLE t_seq (
    id INT DEFAULT seq_1.NEXTVAL
);

-- ==== NEW_SCHEMA START ====

-- Table 1: Unicode and ZWSP (Renamed col with ZWSP)
CREATE TABLE "u_table_Ａ" (
    id INT IDENTITY(1,1),
    "col​1" VARCHAR(100), -- ZWSP still there
    "new_col_​zwsp" INT, -- New ZWSP column
    val_greek_α INT,
    val_latin_a INT
);

-- Table 2: Changed Cluster By
CREATE TABLE t_mixed_quotes (
    `backtick_col` STRING,
    "double_quoted" TEXT,
    unquoted_col INT DEFAULT 42
)
CLUSTER BY (`backtick_col`) -- Changed
COMMENT = $$ Multi-line 
comment with 'quotes' and "quotes" $$;

-- Table 3: Tag Change
CREATE TABLE t_sensitive (
    id INT,
    email STRING WITH MASKING POLICY email_mask,
    data VARIANT
)
WITH TAG (cost_center = 'marketing'); -- Changed tag value

-- Table 4: Type Evolution
CREATE TABLE t_ambiguous (
    c1 FLOAT, -- Normalized from DOUBLE PRECISION
    c2 VARCHAR(100), -- Length change
    c3 TINYINT, -- Normalized
    c4 BOOLEAN -- Resolved alias
);

-- View Chain: V1 -> V2 -> V3 -> V4 -> V5 -> V6
-- V1: Whitespace change only
CREATE VIEW v1 AS 
    SELECT * 
    FROM t_mixed_quotes;

CREATE VIEW v2 AS SELECT * FROM v1;
CREATE VIEW v3 AS SELECT * FROM v2;
CREATE VIEW v4 AS SELECT * FROM v3;
CREATE VIEW v5 AS SELECT * FROM v4;

-- V6: CTE reordering (Semantic equivalent)
CREATE VIEW v6 AS 
WITH RECURSIVE cte1 AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte1 WHERE n < 10),
cte2 AS (SELECT * FROM cte1),
cte3 AS (SELECT * FROM cte2),
cte4 AS (SELECT * FROM cte3),
cte5 AS (SELECT * FROM cte4),
cte6 AS (SELECT * FROM cte5),
cte7 AS (SELECT * FROM cte6),
cte8 AS (SELECT * FROM cte7),
cte9 AS (SELECT * FROM cte8),
cte10 AS (SELECT * FROM cte9)
SELECT * FROM cte10;

-- Procedures
CREATE PROCEDURE proc_sql()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    RETURN 'Updated SQL'; -- Body change
END;
$$;

CREATE PROCEDURE proc_js_dynamic()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var sql = "CREATE OR REPLACE TABLE dynamic_t (id INT);";
    // Embedded SQL in string
    var complex_str = "SELECT 'string with ; inside' FROM t";
    return {status: "updated"};
$$;

-- UDFs
CREATE FUNCTION udf_sql(x INT) RETURNS INT AS 'x * 3'; -- Logic change

CREATE FUNCTION udf_js_variant()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS $$
    return {key: "value", "sql": "SELECT 1"};
$$;

-- Tasks
CREATE TASK task_1
    WAREHOUSE = compute_wh
    SCHEDULE = '5 minutes' -- Schedule change
AS
    CALL proc_sql();

-- Streams
CREATE TABLE Orders (id INT);
CREATE TABLE orders (id INT);
CREATE STREAM s_Orders ON TABLE Orders;
CREATE STREAM s_orders ON TABLE orders;

-- Materialized View
CREATE MATERIALIZED VIEW mv_v6 AS SELECT * FROM v6;

-- External Table
CREATE STAGE s1;
CREATE FILE FORMAT f1 TYPE = CSV;
CREATE EXTERNAL TABLE ext_t (col1 varchar) LOCATION=@s1 FILE_FORMAT=f1;

-- Sequences
CREATE SEQUENCE seq_1 START = 1 INCREMENT = 1;
CREATE TABLE t_seq (
    id INT DEFAULT seq_1.NEXTVAL
);

-- New Objects
CREATE TABLE t_new_feature (
    id INT PRIMARY KEY,
    info VARIANT
);
