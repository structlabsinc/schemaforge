import pytest
from schemaforge.parsers.postgres import PostgresParser
from schemaforge.generators.postgres import PostgresGenerator
from schemaforge.parsers.sqlite import SQLiteParser
from schemaforge.generators.sqlite import SQLiteGenerator
from schemaforge.parsers.mysql import MySQLParser
from schemaforge.generators.mysql import MySQLGenerator
from schemaforge.comparator import Comparator
from schemaforge.models import Schema, Table, Column, ForeignKey

def test_bug_001_fk_reference_columns():
    """BUG-001: Missing FK Reference Columns in Generator"""
    source_sql = """
    CREATE TABLE departments (id INT PRIMARY KEY);
    CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT);
    """
    target_sql = """
    CREATE TABLE departments (id INT PRIMARY KEY);
    CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT, CONSTRAINT fk_dept FOREIGN KEY (dept_id) REFERENCES departments);
    """
    
    parser = PostgresParser()
    source_schema = parser.parse(source_sql)
    target_schema = parser.parse(target_sql)
    
    # Verify parser captured [] for ref_column_names
    emp = target_schema.get_table('employees')
    fk = emp.foreign_keys[0]
    assert fk.ref_column_names == []
    
    comparator = Comparator()
    plan = comparator.compare(source_schema, target_schema)
    
    generator = PostgresGenerator()
    sql = generator.generate_migration(plan)
    
    # Should NOT have empty parens
    assert 'REFERENCES "departments";' in sql
    assert 'REFERENCES "departments"();' not in sql

def test_bug_002_postgres_drop_constraint():
    """BUG-002: Postgres generator should use DROP CONSTRAINT, not DROP FOREIGN KEY"""
    source_sql = """
    CREATE TABLE t1 (id INT PRIMARY KEY, f1 INT, CONSTRAINT fk_t1 FOREIGN KEY (f1) REFERENCES t2(id));
    CREATE TABLE t2 (id INT PRIMARY KEY);
    """
    target_sql = """
    CREATE TABLE t1 (id INT PRIMARY KEY, f1 INT);
    CREATE TABLE t2 (id INT PRIMARY KEY);
    """
    
    parser = PostgresParser()
    source_schema = parser.parse(source_sql)
    target_schema = parser.parse(target_sql)
    
    comparator = Comparator()
    plan = comparator.compare(source_schema, target_schema)
    
    generator = PostgresGenerator()
    sql = generator.generate_migration(plan)
    
    assert 'DROP CONSTRAINT "fk_t1"' in sql
    assert 'DROP FOREIGN KEY' not in sql

def test_bug_003_sqlite_strict():
    """BUG-003: SQLite STRICT table parsing regression"""
    sql = "CREATE TABLE t1 (c1 INT) STRICT;"
    parser = SQLiteParser()
    schema = parser.parse(sql)
    
    table = schema.get_table('t1')
    assert table is not None
    assert table.is_strict is True

def test_mysql_table_properties():
    """MySQL Table Properties (ENGINE, ROW_FORMAT)"""
    sql = "CREATE TABLE t1 (c1 INT) ENGINE=InnoDB ROW_FORMAT=DYNAMIC AUTO_INCREMENT=100;"
    parser = MySQLParser()
    schema = parser.parse(sql)
    
    table = schema.get_table('t1')
    assert table.engine == 'InnoDB'
    assert table.row_format == 'DYNAMIC'
    assert table.auto_increment == 100

def test_postgres_types_domains():
    """Postgres CREATE TYPE and CREATE DOMAIN support"""
    sql = """
    CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
    CREATE DOMAIN us_postal_code AS TEXT CHECK (VALUE ~ '^\\d{5}$');
    """
    parser = PostgresParser()
    schema = parser.parse(sql)
    
    assert len(schema.types) == 1
    assert schema.types[0].name == 'mood'
    assert schema.types[0].obj_type == 'TYPE'
    
    assert len(schema.domains) == 1
    assert schema.domains[0].name == 'us_postal_code'
    assert schema.domains[0].obj_type == 'DOMAIN'
