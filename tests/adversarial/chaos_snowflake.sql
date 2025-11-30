-- Chaos Snowflake Schema
-- Contains: ZWSP, mixed quotes, deep nesting, malformed SQL, and more.

/* 
   Block comment with nested /* stuff */ 
   and mixed line endings
*/

-- Table 1: Mixed quotes and ZWSP
CREATE TABLE "weird​name" ( -- U+200B inside quotes
    id INT,
    "col​1" VARCHAR(100), -- ZWSP in column name
    `backtick_col` STRING,
    'single_quoted_col' TEXT -- Invalid in standard SQL but maybe parsed?
);

-- Table 2: Snowflake features + Comments with $$
CREATE TABLE t_complex (
    id INT,
    data VARIANT,
    secret STRING MASKING POLICY email_mask
)
COMMENT = $$ This is a "comment" with 'quotes' and 
newlines $$
CLUSTER BY (id)
WITH TAG (cost_center = 'finance');

-- Table 3: Row Access Policy
CREATE TABLE t_policy (
    id INT
)
ROW ACCESS POLICY rap_1 ON (id);

-- Table 4: Unicode identifiers
CREATE TABLE Ａ ( -- Fullwidth A
    id INT
);

CREATE TABLE A ( -- Normal A
    id INT
);

-- Table 5: Malformed SQL (Missing comma)
CREATE TABLE t_malformed_1 (
    id INT
    name VARCHAR -- Missing comma
    age INT
);

-- Table 6: Malformed SQL (Extra parenthesis)
CREATE TABLE t_malformed_2 (
    id INT
));

-- Table 7: Malformed SQL (Typos)
CREAT TABLE t_typo (
    id INT
);

SELCT * FROM t_typo;

-- Table 8: Unknown types and dialect options
CREATE TABLE t_options (
    id INT,
    flag BOOLISH -- Unknown type
)
ORGANIZE BY COLUMN
PARTITION BY HASH(id);

-- View A -> B -> C -> D
CREATE VIEW view_d AS SELECT * FROM t_complex;

CREATE VIEW view_c AS 
WITH cte1 AS (SELECT * FROM view_d),
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

CREATE VIEW view_b AS SELECT * FROM view_c;

CREATE VIEW view_a AS 
SELECT 
    *, 
    'string with ​ ZWSP' AS weird_str 
FROM view_b;

-- Stored Procedures
CREATE PROCEDURE proc_sql()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    RETURN 'Hello';
END;
$$;

CREATE PROCEDURE proc_js()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var sql = "CREATE OR REPLACE TABLE dynamic_t (id INT)";
    snowflake.execute({sqlText: sql});
    return {status: "done"};
$$;

CREATE PROCEDURE proc_mixed()
RETURNS VARCHAR
LANGUAGE SQL
AS
'BEGIN RETURN ''Mixed quotes''; END';

-- UDFs
CREATE FUNCTION udf_sql(x INT)
RETURNS INT
AS 'x + 1';

CREATE FUNCTION udf_js()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS $$
    return {"sql": "SELECT 1"};
$$;

-- Tasks
CREATE TASK task_proc
    WAREHOUSE = compute_wh
    SCHEDULE = '1 minute'
AS
    CALL proc_sql();

CREATE TASK task_view
    AFTER task_proc
AS
    INSERT INTO t_complex SELECT * FROM view_a;

-- Materialized View
CREATE MATERIALIZED VIEW mv_a AS SELECT * FROM view_a;

-- Stream
CREATE STREAM stream_a ON TABLE "weird​name";

-- Stream collision (Case insensitive)
CREATE STREAM STREAM_A ON TABLE A;

-- Alternative Spellings
CREATE TABLE t_aliases (
    c1 DOUBLE PRECISION, -- FLOAT8
    c2 CHARACTER VARYING, -- VARCHAR
    c3 BYTEINT -- TINYINT
);

-- Gigantic Statement
SELECT '
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
' AS giant_text;
