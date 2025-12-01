import sys
import os
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.comparator import Comparator
from schemaforge.main import _handle_output

# Mock args
class Args:
    plan = True
    no_color = True
    json = False
    json_out = False
    sql_out = False

args = Args()

def run_test(name, source_sql, target_sql):
    print(f"--- Running {name} ---")
    parser = SnowflakeParser()
    source_schema = parser.parse(source_sql)
    target_schema = parser.parse(target_sql)
    
    comparator = Comparator()
    plan = comparator.compare(source_schema, target_schema)
    
    # Capture output
    output = _handle_output(args, plan)
    print(f"OUTPUT:\n{output}")
    print("---------------------------------------------------")

# 1. Composite Primary Key (Table Constraint)
run_test('Composite Primary Key (Table Constraint)', 
         "CREATE TABLE T (COL1 INT, COL2 INT);",
         """CREATE TABLE T (
    COL1 INT,
    COL2 INT,
    PRIMARY KEY (COL1, COL2)
);""")

# 2. Named Constraints
run_test('Named Constraints',
         "CREATE TABLE T (ID INT);",
         """CREATE TABLE T (
    ID INT,
    CONSTRAINT pk_custom_name PRIMARY KEY (ID)
);""")

# 3. Identity Column (Creation)
run_test('Identity Column (Creation)',
         "CREATE TABLE T (ID INT);",
         "CREATE TABLE T (ID INT IDENTITY(1,1));")

# 4. Alter Identity
run_test('Alter Identity',
         "CREATE TABLE T (ID INT IDENTITY(1,1));",
         "CREATE TABLE T (ID INT);") # Should show DROP IDENTITY

# 5. Self-Referencing Foreign Key
run_test('Self-Referencing FK',
         "CREATE TABLE EMPLOYEES (ID INT PRIMARY KEY, MANAGER_ID INT);",
         """CREATE TABLE EMPLOYEES (
    ID INT PRIMARY KEY,
    MANAGER_ID INT,
    FOREIGN KEY (MANAGER_ID) REFERENCES EMPLOYEES(ID)
);""")

# 6. UNDROP Table
run_test('UNDROP Table',
         "", # Empty source
         "UNDROP TABLE MY_TABLE;")

# 7. SWAP WITH
run_test('SWAP WITH',
         "CREATE TABLE T1 (ID INT); CREATE TABLE T2 (ID INT);",
         "CREATE TABLE T1 (ID INT); CREATE TABLE T2 (ID INT); ALTER TABLE T1 SWAP WITH T2;")

# 8. ALTER PIPE
run_test('ALTER PIPE',
         "CREATE PIPE P AS COPY INTO T FROM @S;",
         "CREATE PIPE P AS COPY INTO T FROM @S; ALTER PIPE P REFRESH;")

# 9. ALTER FILE FORMAT
run_test('ALTER FILE FORMAT',
         "CREATE FILE FORMAT FF TYPE=CSV;",
         "CREATE FILE FORMAT FF TYPE=CSV; ALTER FILE FORMAT FF SET COMPRESSION = 'GZIP';")
