# Adversarial SQL Scenarios - Batch 1

## Scenario 1
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    col1 VARCHAR(100)
);
```
• New DDL:
```sql
CREATE   TABLE   t1(
	id  INT  PRIMARY  KEY ,
	col1  VARCHAR ( 100 )
) ;
```
• Why: Extreme whitespace variation (tabs, multiple spaces) to test tokenizer normalization.

## Scenario 2
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t2 (
    id SERIAL PRIMARY KEY,
    data JSONB
);
```
• New DDL:
```sql
CREATE TABLE t2 (
    id SERIAL PRIMARY KEY,
    data JSONB -- This is a comment
);
```
• Why: Trailing comment on the last column definition, testing if parser handles comments before closing parenthesis.

## Scenario 3
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t3 (
    id NUMBER,
    val VARIANT
);
```
• New DDL:
```sql
CREATE TABLE t3 (
    id NUMBER,
    val VARIANT COMMENT 'This is a tricky comment with ) inside'
);
```
• Why: Comment string containing a closing parenthesis `)` which might prematurely close the column list in regex parsers.

## Scenario 4
• Dialect: SQLite
• Old DDL:
```sql
CREATE TABLE t4 (
    id INTEGER PRIMARY KEY,
    name TEXT
);
```
• New DDL:
```sql
CREATE TABLE "t4" (
    "id" INTEGER PRIMARY KEY,
    "name" TEXT
);
```
• Why: Quoted identifiers vs unquoted. Should be treated as identical.

## Scenario 5
• Dialect: Oracle
• Old DDL:
```sql
CREATE TABLE t5 (
    id NUMBER(10, 0)
);
```
• New DDL:
```sql
CREATE TABLE t5 (
    id INTEGER
);
```
• Why: `INTEGER` in Oracle is an alias for `NUMBER(38)`. Tool might flag this as a change if not aware of type aliases.

## Scenario 6
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t6 (
    id INT,
    tags TEXT[]
);
```
• New DDL:
```sql
CREATE TABLE t6 (
    id INT,
    tags TEXT ARRAY
);
```
• Why: `TEXT[]` vs `TEXT ARRAY` syntax equivalence.

## Scenario 7
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t7 (
    id INT AUTO_INCREMENT,
    PRIMARY KEY (id)
);
```
• New DDL:
```sql
CREATE TABLE t7 (
    id INT AUTO_INCREMENT PRIMARY KEY
);
```
• Why: Inline Primary Key vs Out-of-line Primary Key constraint. Semantically identical.

## Scenario 8
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t8 (
    col1 VARCHAR
);
```
• New DDL:
```sql
CREATE TABLE t8 (
    col1 STRING
);
```
• Why: `STRING` is a synonym for `VARCHAR` in Snowflake.

## Scenario 9
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t9 (
    val NUMERIC(10, 2) DEFAULT 0.00
);
```
• New DDL:
```sql
CREATE TABLE t9 (
    val NUMERIC(10, 2) DEFAULT 0
);
```
• Why: Default value formatting `0.00` vs `0`.

## Scenario 10
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t10 (
    id INT
) ENGINE=InnoDB;
```
• New DDL:
```sql
CREATE TABLE t10 (
    id INT
) ENGINE = InnoDB;
```
• Why: Whitespace around table options (`ENGINE=InnoDB` vs `ENGINE = InnoDB`).

## Scenario 11
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t11 (
    id INT
);
CREATE INDEX idx_t11 ON t11(id);
```
• New DDL:
```sql
CREATE TABLE t11 (
    id INT
);
CREATE INDEX idx_t11 ON t11 ( id );
```
• Why: Whitespace inside index column list.

## Scenario 12
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t12 (
    data VARIANT
);
```
• New DDL:
```sql
CREATE TABLE t12 (
    data VARIANT
) DATA_RETENTION_TIME_IN_DAYS=1;
```
• Why: Snowflake specific table property `DATA_RETENTION_TIME_IN_DAYS`.

## Scenario 13
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t13 (
    col1 INT,
    col2 INT
);
```
• New DDL:
```sql
CREATE TABLE t13 (
    col2 INT,
    col1 INT
);
```
• Why: Column reordering. Should be detected as a change (or not, depending on strictness), but often tools fail to map renamed vs reordered columns.

## Scenario 14
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t14 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE public.t14 (
    id INT
);
```
• Why: Schema qualification `public.t14` vs `t14`.

## Scenario 15
• Dialect: Oracle
• Old DDL:
```sql
CREATE TABLE t15 (
    name VARCHAR2(50 BYTE)
);
```
• New DDL:
```sql
CREATE TABLE t15 (
    name VARCHAR2(50 CHAR)
);
```
• Why: Byte vs Char semantics in Oracle VARCHAR2.

## Scenario 16
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t16 (
    id INT /* Primary Key */
);
```
• New DDL:
```sql
CREATE TABLE t16 (
    id INT
);
```
• Why: Inline block comment removal.

## Scenario 17
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t17 (
    id INT
);
```
• New DDL:
```sql
/* Header Comment */
CREATE TABLE t17 (
    id INT
);
```
• Why: Leading block comment before CREATE statement.

## Scenario 18
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t18 (
    id INT
);
```
• New DDL:
```sql
CREATE OR REPLACE TABLE t18 (
    id INT
);
```
• Why: `CREATE OR REPLACE` syntax.

## Scenario 19
• Dialect: SQLite
• Old DDL:
```sql
CREATE TABLE t19 (
    id INTEGER PRIMARY KEY AUTOINCREMENT
);
```
• New DDL:
```sql
CREATE TABLE t19 (
    id INTEGER PRIMARY KEY
);
```
• Why: `AUTOINCREMENT` keyword presence.

## Scenario 20
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t20 (
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
• New DDL:
```sql
CREATE TABLE t20 (
    ts TIMESTAMP DEFAULT NOW()
);
```
• Why: `CURRENT_TIMESTAMP` vs `NOW()` synonym.

## Scenario 21
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t21 (
    id INT,
    CONSTRAINT pk_t21 PRIMARY KEY (id)
);
```
• New DDL:
```sql
CREATE TABLE t21 (
    id INT,
    PRIMARY KEY (id)
);
```
• Why: Named constraint vs anonymous constraint.

## Scenario 22
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t22 (
    id INT
);
```
• New DDL:
```sql
CREATE TRANSIENT TABLE t22 (
    id INT
);
```
• Why: `TRANSIENT` keyword.

## Scenario 23
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t23 (
    col1 INT UNSIGNED
);
```
• New DDL:
```sql
CREATE TABLE t23 (
    col1 INT
);
```
• Why: `UNSIGNED` attribute.

## Scenario 24
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t24 (
    col1 VARCHAR(50)
);
```
• New DDL:
```sql
CREATE TABLE t24 (
    col1 CHARACTER VARYING(50)
);
```
• Why: `VARCHAR` vs `CHARACTER VARYING`.

## Scenario 25
• Dialect: Oracle
• Old DDL:
```sql
CREATE TABLE t25 (
    id NUMBER
);
```
• New DDL:
```sql
CREATE TABLE t25 (
    id NUMBER(*, 0)
);
```
• Why: `NUMBER` vs `NUMBER(*, 0)`.

## Scenario 26
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t26 (
    col1 ENUM('a', 'b')
);
```
• New DDL:
```sql
CREATE TABLE t26 (
    col1 ENUM('a', 'b', 'c')
);
```
• Why: Enum value modification.

## Scenario 27
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t27 (
    id UUID
);
```
• New DDL:
```sql
CREATE TABLE t27 (
    id UUID DEFAULT gen_random_uuid()
);
```
• Why: Function call in default value.

## Scenario 28
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t28 (
    v VARIANT
);
```
• New DDL:
```sql
CREATE TABLE t28 (
    v VARIANT,
    v_flat AS (v:name::varchar)
);
```
• Why: Computed column / Virtual column syntax in Snowflake.

## Scenario 29
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t29 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE `t29` (
    `id` INT
);
```
• Why: Backtick quoting in MySQL.

## Scenario 30
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t30 (
    id INT
);
```
• New DDL:
```sql
create table T30 (
    ID int
);
```
• Why: Case sensitivity of keywords and unquoted identifiers (Postgres folds to lower, but SQL is case-insensitive).

## Scenario 31
• Dialect: SQLite
• Old DDL:
```sql
CREATE TABLE t31 (
    col1 INT
);
```
• New DDL:
```sql
CREATE TABLE t31 (
    col1 INT CHECK(col1 > 0)
);
```
• Why: Inline CHECK constraint.

## Scenario 32
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t32 (
    id INT
);
CREATE INDEX idx_t32 ON t32(id) USING BTREE;
```
• New DDL:
```sql
CREATE TABLE t32 (
    id INT
);
CREATE INDEX idx_t32 ON t32(id);
```
• Why: Index algorithm specification `USING BTREE` (often default).

## Scenario 33
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t33 (
    col1 TEXT COLLATE "en_US"
);
```
• New DDL:
```sql
CREATE TABLE t33 (
    col1 TEXT
);
```
• Why: Collation specification.

## Scenario 34
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t34 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t34 (
    id INT
) CLUSTER BY (id);
```
• Why: `CLUSTER BY` clause.

## Scenario 35
• Dialect: Oracle
• Old DDL:
```sql
CREATE TABLE t35 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t35 (
    id INT
) TABLESPACE users;
```
• Why: `TABLESPACE` clause.

## Scenario 36
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t36 (
    id INT NOT NULL
);
```
• New DDL:
```sql
CREATE TABLE t36 (
    id INT NULL
);
```
• Why: `NOT NULL` vs `NULL` (explicit).

## Scenario 37
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t37 (
    id INT
);
```
• New DDL:
```sql
CREATE UNLOGGED TABLE t37 (
    id INT
);
```
• Why: `UNLOGGED` table type.

## Scenario 38
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t38 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t38 (
    id INT
) COPY GRANTS;
```
• Why: `COPY GRANTS` syntax.

## Scenario 39
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t39 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t39 (
    id INT COMMENT 'Column Comment'
);
```
• Why: Inline column comment.

## Scenario 40
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t40 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t40 (
    id INT
);
COMMENT ON COLUMN t40.id IS 'Column Comment';
```
• Why: Out-of-line comment syntax.

## Scenario 41
• Dialect: SQLite
• Old DDL:
```sql
CREATE TABLE t41 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t41 (
    id INT DEFAULT (datetime('now'))
);
```
• Why: Expression in default value with parentheses.

## Scenario 42
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t42 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t42 (
    id INT
) CHARACTER SET utf8mb4;
```
• Why: Table character set definition.

## Scenario 43
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t43 (
    range INT4RANGE
);
```
• New DDL:
```sql
CREATE TABLE t43 (
    range INT4RANGE
);
```
• Why: Range types (Postgres specific).

## Scenario 44
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t44 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t44 (
    id INT
) TAG (confidentiality = 'high');
```
• Why: Tagging syntax.

## Scenario 45
• Dialect: Oracle
• Old DDL:
```sql
CREATE TABLE t45 (
    id INT
);
```
• New DDL:
```sql
CREATE GLOBAL TEMPORARY TABLE t45 (
    id INT
) ON COMMIT DELETE ROWS;
```
• Why: Global Temporary Table syntax.

## Scenario 46
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t46 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t46 (
    id INT
) PARTITION BY HASH(id);
```
• Why: Partitioning syntax.

## Scenario 47
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t47 (
    id INT GENERATED ALWAYS AS IDENTITY
);
```
• New DDL:
```sql
CREATE TABLE t47 (
    id INT GENERATED BY DEFAULT AS IDENTITY
);
```
• Why: Identity column generation options (`ALWAYS` vs `BY DEFAULT`).

## Scenario 48
• Dialect: Snowflake
• Old DDL:
```sql
CREATE TABLE t48 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t48 (
    id INT MASKING POLICY my_policy
);
```
• Why: Masking policy syntax.

## Scenario 49
• Dialect: MySQL
• Old DDL:
```sql
CREATE TABLE t49 (
    id INT,
    INDEX idx (id)
);
```
• New DDL:
```sql
CREATE TABLE t49 (
    id INT,
    KEY idx (id)
);
```
• Why: `INDEX` vs `KEY` synonym.

## Scenario 50
• Dialect: PostgreSQL
• Old DDL:
```sql
CREATE TABLE t50 (
    id INT
);
```
• New DDL:
```sql
CREATE TABLE t50 (
    id INT
) WITH (fillfactor=70);
```
• Why: Storage parameters in `WITH` clause.
