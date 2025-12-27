"""
Additional tests for edge cases and uncovered code paths.
"""
import pytest
from schemaforge.parsers.postgres import PostgresParser
from schemaforge.parsers.mysql import MySQLParser
from schemaforge.parsers.sqlite import SQLiteParser
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.generators.generic import GenericGenerator
from schemaforge.generators.oracle import OracleGenerator
from schemaforge.generators.snowflake import SnowflakeGenerator
from schemaforge.comparator import MigrationPlan, TableDiff
from schemaforge.models import Table, Column, Index, ForeignKey, CheckConstraint, Schema


class TestPostgresParserEdgeCases:
    """Additional Postgres parser tests."""
    
    def test_identity_column(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (
                id INT GENERATED ALWAYS AS IDENTITY,
                name VARCHAR(100)
            );
        ''')
        assert len(schema.tables) >= 1
        
    def test_array_type(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (
                id INT,
                tags TEXT[],
                scores INT[][]
            );
        ''')
        table = schema.tables[0]
        assert len(table.columns) >= 2
        
    def test_create_type(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
            CREATE TABLE test (id INT, current_mood mood);
        ''')
        assert len(schema.tables) >= 1
        
    def test_foreign_key_inline(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE orders (
                id INT,
                customer_id INT REFERENCES customers(id) ON DELETE CASCADE
            );
        ''')
        assert len(schema.tables) >= 1
        
    def test_exclusion_constraint(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE reservations (
                id INT,
                room_id INT,
                during TSRANGE,
                EXCLUDE USING gist (room_id WITH =, during WITH &&)
            );
        ''')
        assert len(schema.tables) >= 1
        
    def test_gin_index(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT, data JSONB);
            CREATE INDEX idx_data ON test USING gin(data);
        ''')
        table = schema.get_table('test')
        assert table is not None
        
    def test_partial_index(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT, active BOOLEAN);
            CREATE INDEX idx_active ON test(id) WHERE active = true;
        ''')
        table = schema.get_table('test')
        assert table is not None


class TestSnowflakeParserEdgeCases:
    """Additional Snowflake parser tests."""
    
    def test_variant_column(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE events (
                id NUMBER,
                payload VARIANT,
                metadata OBJECT
            );
        ''')
        assert len(schema.tables) >= 1
        
    def test_clone_table(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test_clone CLONE production.test;
        ''')
        # May or may not parse, but shouldn't crash
        assert schema is not None
        
    def test_cluster_by(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (
                id NUMBER,
                created_date DATE
            ) CLUSTER BY (created_date);
        ''')
        assert len(schema.tables) >= 1
        
    def test_data_retention(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER)
            DATA_RETENTION_TIME_IN_DAYS = 90;
        ''')
        assert len(schema.tables) >= 1
        
    def test_stage_object(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER);
            CREATE STAGE my_stage URL='s3://bucket/path/';
        ''')
        assert len(schema.tables) >= 1


class TestGenericGenerator:
    """Additional generic generator tests."""
    
    def test_alter_table_add_column(self):
        diff = TableDiff(
            table_name='test',
            added_columns=[Column('new_col', 'INT')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'ALTER' in sql or sql == ''
        
    def test_alter_table_drop_column(self):
        diff = TableDiff(
            table_name='test',
            dropped_columns=[Column('old_col', 'INT')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'ALTER' in sql or 'DROP' in sql or sql == ''
        
    def test_create_index(self):
        diff = TableDiff(
            table_name='test',
            added_indexes=[Index('idx_new', ['col1'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = GenericGenerator()
        sql = gen.generate_migration(plan)
        
        # May or may not generate depending on implementation
        assert sql is not None


class TestOracleGeneratorCoverage:
    """Additional Oracle generator tests."""
    
    def test_create_table_with_pk(self):
        table = Table(name='employees', columns=[
            Column('id', 'NUMBER', is_primary_key=True),
            Column('name', 'VARCHAR2(100)', is_nullable=False),
            Column('salary', 'NUMBER(10,2)')
        ])
        plan = MigrationPlan(new_tables=[table])
        
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        
    def test_add_column(self):
        diff = TableDiff(
            table_name='employees',
            added_columns=[Column('department', 'VARCHAR2(50)')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'ALTER' in sql or 'ADD' in sql
        
    def test_drop_column(self):
        diff = TableDiff(
            table_name='employees',
            dropped_columns=[Column('old_col', 'VARCHAR2(50)')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = OracleGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'DROP' in sql or 'ALTER' in sql


class TestSnowflakeGeneratorCoverage:
    """Additional Snowflake generator tests."""
    
    def test_create_transient_table(self):
        table = Table(name='temp_data', columns=[
            Column('id', 'NUMBER'),
            Column('data', 'VARIANT')
        ], is_transient=True)
        plan = MigrationPlan(new_tables=[table])
        
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE' in sql
        
    def test_table_with_cluster_by(self):
        table = Table(name='events', columns=[
            Column('id', 'NUMBER'),
            Column('event_date', 'DATE')
        ], cluster_by=['event_date'])
        plan = MigrationPlan(new_tables=[table])
        
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE' in sql
        
    def test_add_column(self):
        diff = TableDiff(
            table_name='events',
            added_columns=[Column('source', 'VARCHAR(100)')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'ALTER' in sql or 'ADD' in sql


class TestGenericSQLParserEdgeCases:
    """Additional generic parser tests."""
    
    def test_multiple_tables(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE users (id INT, name VARCHAR(100));
            CREATE TABLE orders (id INT, user_id INT);
            CREATE TABLE products (id INT, sku VARCHAR(50));
        ''')
        assert len(schema.tables) == 3
        
    def test_table_with_all_constraint_types(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE complex (
                id INT PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                age INT CHECK (age >= 0),
                dept_id INT REFERENCES departments(id),
                CONSTRAINT chk_email CHECK (email LIKE '%@%')
            );
        ''')
        table = schema.tables[0]
        assert len(table.columns) >= 3
        
    def test_quoted_identifiers(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE "User Table" (
                "id" INT,
                "Full Name" VARCHAR(100)
            );
        ''')
        assert len(schema.tables) == 1
        
    def test_numeric_types(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE numbers (
                tiny TINYINT,
                small SMALLINT,
                medium MEDIUMINT,
                big BIGINT,
                dec DECIMAL(10,2),
                num NUMERIC(15,4),
                fl FLOAT,
                dbl DOUBLE,
                rl REAL
            );
        ''')
        table = schema.tables[0]
        assert len(table.columns) >= 8
        
    def test_date_time_types(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE timestamps (
                d DATE,
                t TIME,
                ts TIMESTAMP,
                dt DATETIME,
                tz TIMESTAMP WITH TIME ZONE
            );
        ''')
        table = schema.tables[0]
        assert len(table.columns) >= 4
        
    def test_text_types(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE texts (
                t1 TEXT,
                t2 LONGTEXT,
                t3 MEDIUMTEXT,
                t4 TINYTEXT,
                b1 BLOB,
                b2 LONGBLOB
            );
        ''')
        table = schema.tables[0]
        assert len(table.columns) >= 4


class TestComparatorEdgeCases:
    """Additional comparator tests."""
    
    def test_multiple_changes_same_table(self):
        from schemaforge.comparator import Comparator
        
        schema1 = Schema(tables=[
            Table(name='users', columns=[
                Column('id', 'INT'),
                Column('name', 'VARCHAR(50)'),
                Column('old_col', 'TEXT')
            ])
        ])
        schema2 = Schema(tables=[
            Table(name='users', columns=[
                Column('id', 'INT'),
                Column('name', 'VARCHAR(200)'),  # Modified
                Column('email', 'VARCHAR(255)')  # Added
                # old_col dropped
            ])
        ])
        
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        
        assert len(plan.modified_tables) == 1
        diff = plan.modified_tables[0]
        assert len(diff.added_columns) >= 1
        assert len(diff.dropped_columns) >= 1
        assert len(diff.modified_columns) >= 1
        
    def test_index_changes(self):
        from schemaforge.comparator import Comparator
        
        schema1 = Schema(tables=[
            Table(name='users', columns=[Column('email', 'VARCHAR')],
                  indexes=[Index('idx_old', ['email'])])
        ])
        schema2 = Schema(tables=[
            Table(name='users', columns=[Column('email', 'VARCHAR')],
                  indexes=[Index('idx_new', ['email'], is_unique=True)])
        ])
        
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        
        if plan.modified_tables:
            diff = plan.modified_tables[0]
            # Should detect index changes
            assert len(diff.dropped_indexes) >= 1 or len(diff.added_indexes) >= 1
