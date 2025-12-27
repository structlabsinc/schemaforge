"""
Tests for SQL generators.
"""
import pytest
from schemaforge.generators.postgres import PostgresGenerator
from schemaforge.generators.mysql import MySQLGenerator
from schemaforge.generators.sqlite import SQLiteGenerator
from schemaforge.generators.oracle import OracleGenerator
from schemaforge.generators.db2 import DB2Generator
from schemaforge.generators.snowflake import SnowflakeGenerator
from schemaforge.generators.generic import GenericGenerator
from schemaforge.comparator import MigrationPlan, TableDiff
from schemaforge.models import Table, Column, Index, ForeignKey, CheckConstraint


class TestPostgresGenerator:
    """PostgreSQL-specific generator tests."""
    
    def test_create_table_basic(self):
        table = Table(name='users', columns=[
            Column('id', 'SERIAL', is_primary_key=True),
            Column('email', 'VARCHAR(255)', is_nullable=False),
            Column('created_at', 'TIMESTAMP')
        ])
        plan = MigrationPlan(new_tables=[table])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        assert '"users"' in sql
        assert 'SERIAL' in sql
        
    def test_drop_table(self):
        table = Table(name='old_table', columns=[])
        plan = MigrationPlan(dropped_tables=[table])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'DROP TABLE' in sql
        assert '"old_table"' in sql
        
    def test_add_column(self):
        diff = TableDiff(
            table_name='users',
            added_columns=[Column('phone', 'VARCHAR(20)')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'ALTER TABLE' in sql
        assert 'ADD COLUMN' in sql
        assert '"phone"' in sql
        
    def test_drop_column(self):
        diff = TableDiff(
            table_name='users',
            dropped_columns=[Column('old_col', 'TEXT')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'ALTER TABLE' in sql
        assert 'DROP COLUMN' in sql
        
    def test_modify_column_type(self):
        diff = TableDiff(
            table_name='users',
            modified_columns=[(
                Column('name', 'VARCHAR(50)'),
                Column('name', 'VARCHAR(200)')
            )]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'ALTER TABLE' in sql
        assert 'ALTER COLUMN' in sql or 'TYPE' in sql
        
    def test_add_index(self):
        diff = TableDiff(
            table_name='users',
            added_indexes=[Index('idx_email', ['email'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE INDEX' in sql
        assert 'idx_email' in sql
        
    def test_drop_index(self):
        diff = TableDiff(
            table_name='users',
            dropped_indexes=[Index('idx_old', ['old_col'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'DROP INDEX' in sql
        
    def test_add_foreign_key(self):
        diff = TableDiff(
            table_name='orders',
            added_fks=[ForeignKey('fk_customer', ['customer_id'], 'customers', ['id'], on_delete='CASCADE')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'ADD CONSTRAINT' in sql
        assert 'FOREIGN KEY' in sql
        assert 'REFERENCES' in sql
        assert 'CASCADE' in sql
        
    def test_drop_foreign_key(self):
        diff = TableDiff(
            table_name='orders',
            dropped_fks=[ForeignKey('fk_old', ['old_id'], 'old_table', ['id'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'DROP CONSTRAINT' in sql
        
    def test_modify_foreign_key(self):
        diff = TableDiff(
            table_name='orders',
            modified_fks=[(
                ForeignKey('fk_customer', ['customer_id'], 'customers', ['id'], on_delete='CASCADE'),
                ForeignKey('fk_customer', ['customer_id'], 'customers', ['id'], on_delete='SET NULL')
            )]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'DROP CONSTRAINT' in sql
        assert 'ADD CONSTRAINT' in sql
        assert 'SET NULL' in sql
        
    def test_quote_ident(self):
        gen = PostgresGenerator()
        assert gen.quote_ident('users') == '"users"'
        assert gen.quote_ident('User Table') == '"User Table"'


class TestMySQLGenerator:
    """MySQL-specific generator tests."""
    
    def test_create_table_basic(self):
        table = Table(name='products', columns=[
            Column('id', 'INT', is_primary_key=True),
            Column('name', 'VARCHAR(100)'),
            Column('price', 'DECIMAL(10,2)')
        ])
        plan = MigrationPlan(new_tables=[table])
        
        gen = MySQLGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        
    def test_drop_table(self):
        table = Table(name='old_table', columns=[])
        plan = MigrationPlan(dropped_tables=[table])
        
        gen = MySQLGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'DROP TABLE' in sql
        
    def test_add_column(self):
        diff = TableDiff(
            table_name='products',
            added_columns=[Column('description', 'TEXT')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = MySQLGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'ALTER TABLE' in sql
        assert 'ADD COLUMN' in sql
        
    def test_quote_ident(self):
        gen = MySQLGenerator()
        assert gen.quote_ident('users') == '`users`'


class TestSQLiteGenerator:
    """SQLite-specific generator tests."""
    
    def test_create_table_basic(self):
        table = Table(name='items', columns=[
            Column('id', 'INTEGER', is_primary_key=True),
            Column('name', 'TEXT')
        ])
        plan = MigrationPlan(new_tables=[table])
        
        gen = SQLiteGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        
    def test_drop_table(self):
        table = Table(name='old_table', columns=[])
        plan = MigrationPlan(dropped_tables=[table])
        
        gen = SQLiteGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'DROP TABLE' in sql


class TestOracleGenerator:
    """Oracle-specific generator tests."""
    
    def test_create_table_basic(self):
        table = Table(name='employees', columns=[
            Column('id', 'NUMBER', is_primary_key=True),
            Column('name', 'VARCHAR2(100)')
        ])
        plan = MigrationPlan(new_tables=[table])
        
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        
    def test_quote_ident(self):
        gen = OracleGenerator()
        # Oracle uses double quotes
        assert '"' in gen.quote_ident('USERS')


class TestDB2Generator:
    """DB2-specific generator tests."""
    
    def test_create_table_basic(self):
        table = Table(name='accounts', columns=[
            Column('id', 'INTEGER', is_primary_key=True),
            Column('balance', 'DECIMAL(15,2)')
        ])
        plan = MigrationPlan(new_tables=[table])
        
        gen = DB2Generator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        
    def test_drop_table(self):
        table = Table(name='old_table', columns=[])
        plan = MigrationPlan(dropped_tables=[table])
        
        gen = DB2Generator()
        sql = gen.generate_migration(plan)
        
        assert 'DROP TABLE' in sql


class TestSnowflakeGenerator:
    """Snowflake-specific generator tests."""
    
    def test_create_table_basic(self):
        table = Table(name='events', columns=[
            Column('id', 'NUMBER'),
            Column('event_data', 'VARIANT')
        ])
        plan = MigrationPlan(new_tables=[table])
        
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        
    def test_drop_table(self):
        table = Table(name='old_table', columns=[])
        plan = MigrationPlan(dropped_tables=[table])
        
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'DROP TABLE' in sql


class TestGenericGenerator:
    """Generic generator tests."""
    
    def test_create_table_basic(self):
        table = Table(name='generic_table', columns=[
            Column('id', 'INT'),
            Column('data', 'TEXT')
        ])
        plan = MigrationPlan(new_tables=[table])
        
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        
    def test_empty_plan(self):
        plan = MigrationPlan()
        
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        
        # Should return empty or minimal SQL
        assert sql == '' or 'Migration' in sql or sql.strip() == ''
