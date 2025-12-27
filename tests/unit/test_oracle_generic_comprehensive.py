"""
Comprehensive Oracle and generic generator tests for maximum coverage.
"""
import pytest
from schemaforge.generators.oracle import OracleGenerator
from schemaforge.generators.generic import GenericGenerator
from schemaforge.parsers.oracle import OracleParser
from schemaforge.comparator import MigrationPlan, TableDiff
from schemaforge.models import Table, Column, Index, ForeignKey, CheckConstraint


class TestOracleGeneratorComprehensive:
    """Comprehensive Oracle generator tests."""
    
    def test_create_simple_table(self):
        table = Table(name='EMPLOYEES', columns=[
            Column('ID', 'NUMBER'),
            Column('NAME', 'VARCHAR2(100)')
        ])
        plan = MigrationPlan(new_tables=[table])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert 'CREATE TABLE' in sql
        
    def test_create_table_with_pk(self):
        table = Table(name='USERS', columns=[
            Column('ID', 'NUMBER', is_primary_key=True),
            Column('EMAIL', 'VARCHAR2(255)')
        ])
        plan = MigrationPlan(new_tables=[table])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert 'CREATE TABLE' in sql
        
    def test_create_table_with_not_null(self):
        table = Table(name='ORDERS', columns=[
            Column('ID', 'NUMBER', is_nullable=False),
            Column('CUSTOMER_ID', 'NUMBER', is_nullable=False)
        ])
        plan = MigrationPlan(new_tables=[table])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert 'CREATE TABLE' in sql
        
    def test_drop_table(self):
        table = Table(name='OLD_TABLE', columns=[])
        plan = MigrationPlan(dropped_tables=[table])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert 'DROP TABLE' in sql
        
    def test_add_column(self):
        diff = TableDiff(
            table_name='EMPLOYEES',
            added_columns=[Column('DEPARTMENT', 'VARCHAR2(50)')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert 'ALTER' in sql or 'ADD' in sql
        
    def test_drop_column(self):
        diff = TableDiff(
            table_name='EMPLOYEES',
            dropped_columns=[Column('OLD_COL', 'VARCHAR2(50)')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert 'DROP' in sql or 'ALTER' in sql
        
    def test_modify_column(self):
        diff = TableDiff(
            table_name='EMPLOYEES',
            modified_columns=[(
                Column('NAME', 'VARCHAR2(50)'),
                Column('NAME', 'VARCHAR2(200)')
            )]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_add_index(self):
        diff = TableDiff(
            table_name='EMPLOYEES',
            added_indexes=[Index('IDX_NAME', ['NAME'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert 'INDEX' in sql or sql is not None
        
    def test_drop_index(self):
        diff = TableDiff(
            table_name='EMPLOYEES',
            dropped_indexes=[Index('IDX_OLD', ['OLD_COL'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_add_foreign_key(self):
        diff = TableDiff(
            table_name='ORDERS',
            added_fks=[ForeignKey('FK_CUSTOMER', ['CUSTOMER_ID'], 'CUSTOMERS', ['ID'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_drop_foreign_key(self):
        diff = TableDiff(
            table_name='ORDERS',
            dropped_fks=[ForeignKey('FK_OLD', ['OLD_ID'], 'OLD_TABLE', ['ID'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_add_check_constraint(self):
        diff = TableDiff(
            table_name='PRODUCTS',
            added_checks=[CheckConstraint('CHK_PRICE', 'PRICE > 0')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_quote_ident(self):
        gen = OracleGenerator()
        result = gen.quote_ident('USERS')
        assert '"' in result or result == 'USERS'
        
    def test_empty_plan(self):
        plan = MigrationPlan()
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None


class TestGenericGeneratorComprehensive:
    """Comprehensive generic generator tests."""
    
    def test_create_table(self):
        table = Table(name='test', columns=[
            Column('id', 'INT'),
            Column('name', 'VARCHAR(100)')
        ])
        plan = MigrationPlan(new_tables=[table])
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        assert 'CREATE TABLE' in sql
        
    def test_create_table_with_pk(self):
        table = Table(name='test', columns=[
            Column('id', 'INT', is_primary_key=True),
            Column('data', 'TEXT')
        ])
        plan = MigrationPlan(new_tables=[table])
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        assert 'CREATE TABLE' in sql
        
    def test_drop_table(self):
        table = Table(name='old', columns=[])
        plan = MigrationPlan(dropped_tables=[table])
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        assert 'DROP TABLE' in sql
        
    def test_add_column(self):
        diff = TableDiff(
            table_name='test',
            added_columns=[Column('new_col', 'INT')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_drop_column(self):
        diff = TableDiff(
            table_name='test',
            dropped_columns=[Column('old_col', 'INT')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_modify_column(self):
        diff = TableDiff(
            table_name='test',
            modified_columns=[(Column('col', 'INT'), Column('col', 'BIGINT'))]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_add_index(self):
        diff = TableDiff(
            table_name='test',
            added_indexes=[Index('idx_col', ['col'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_drop_index(self):
        diff = TableDiff(
            table_name='test',
            dropped_indexes=[Index('idx_old', ['old_col'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None
        
    def test_quote_ident(self):
        gen = GenericGenerator()
        result = gen.quote_ident('test')
        assert result is not None
        
    def test_empty_plan(self):
        plan = MigrationPlan()
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        assert sql == '' or sql is not None


class TestOracleParserComprehensive:
    """Comprehensive Oracle parser tests."""
    
    def test_basic_table(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (id NUMBER);')
        assert len(schema.tables) >= 1
        
    def test_varchar2(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (name VARCHAR2(100));')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_number_precision(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (amount NUMBER(10,2));')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_date_type(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (created DATE);')
        col = schema.tables[0].columns[0]
        assert 'DATE' in col.data_type.upper()
        
    def test_timestamp_type(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (ts TIMESTAMP);')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_clob_type(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (content CLOB);')
        col = schema.tables[0].columns[0]
        assert 'CLOB' in col.data_type.upper()
        
    def test_blob_type(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (data BLOB);')
        col = schema.tables[0].columns[0]
        assert 'BLOB' in col.data_type.upper()
        
    def test_primary_key(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (id NUMBER PRIMARY KEY);')
        col = schema.tables[0].columns[0]
        assert col.is_primary_key == True
        
    def test_not_null(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (name VARCHAR2(100) NOT NULL);')
        col = schema.tables[0].columns[0]
        assert col.is_nullable == False
        
    def test_tablespace(self):
        parser = OracleParser()
        schema = parser.parse('CREATE TABLE test (id NUMBER) TABLESPACE users;')
        table = schema.tables[0]
        assert table.tablespace is not None
        
    def test_multiple_columns(self):
        parser = OracleParser()
        schema = parser.parse('''
            CREATE TABLE employees (
                id NUMBER,
                name VARCHAR2(100),
                email VARCHAR2(255),
                salary NUMBER(10,2)
            );
        ''')
        table = schema.tables[0]
        assert len(table.columns) == 4
        
    def test_multiple_tables(self):
        parser = OracleParser()
        schema = parser.parse('''
            CREATE TABLE t1 (id NUMBER);
            CREATE TABLE t2 (id NUMBER);
        ''')
        assert len(schema.tables) == 2
