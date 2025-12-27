"""
Tests for bug fixes and new functionality added during enterprise testing.
"""
import pytest
import logging
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.parsers.postgres import PostgresParser
from schemaforge.parsers.mysql import MySQLParser
from schemaforge.parsers.sqlite import SQLiteParser
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.generators.postgres import PostgresGenerator
from schemaforge.generators.mysql import MySQLGenerator
from schemaforge.generators.sqlite import SQLiteGenerator
from schemaforge.comparator import Comparator
from schemaforge.models import Schema, Table, Column, ForeignKey, Index


class TestDuplicateDetection:
    """Test for BUG: Silent Duplicate Overwrite"""
    
    def test_duplicate_table_logs_error(self, caplog):
        """Duplicate table definitions should log an ERROR."""
        parser = GenericSQLParser()
        with caplog.at_level(logging.ERROR):
            schema = parser.parse('''
                CREATE TABLE dup_table (id INT, col_a VARCHAR(10));
                CREATE TABLE dup_table (id INT, col_b INT);
            ''')
        
        # Should have only 1 table (latest)
        assert len(schema.tables) == 1
        assert schema.tables[0].name == 'dup_table'
        # Should have logged error
        assert 'Duplicate table definition' in caplog.text
        
    def test_duplicate_table_keeps_latest(self):
        """Duplicate should keep the latest definition."""
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE dup (col_a INT);
            CREATE TABLE dup (col_b VARCHAR(50));
        ''')
        
        assert len(schema.tables) == 1
        cols = [c.name for c in schema.tables[0].columns]
        assert 'col_b' in cols
        assert 'col_a' not in cols


class TestSyntaxWarnings:
    """Test for BUG: Silent Syntax Error Failure"""
    
    def test_garbage_statement_logs_warning(self, caplog):
        """Random garbage should log a WARNING."""
        parser = GenericSQLParser()
        with caplog.at_level(logging.WARNING):
            parser.parse('THIS IS NOT SQL;')
        
        assert 'Ignored statement' in caplog.text
        
    def test_missing_parenthesis_logs_warning(self, caplog):
        """Missing parenthesis should log WARNING."""
        parser = GenericSQLParser()
        with caplog.at_level(logging.WARNING):
            schema = parser.parse('''
                CREATE TABLE broken (
                    id INT
                -- missing closing paren
                ;
            ''')
        
        assert 'Failed to parse columns' in caplog.text or 'parenthesis' in caplog.text.lower()
        
    def test_malformed_create_logs_error(self, caplog):
        """Malformed CREATE should log ERROR."""
        parser = GenericSQLParser()
        with caplog.at_level(logging.ERROR):
            parser.parse('CREATE TABLE')  # Just keywords, no name
        
        assert 'Failed to parse statement' in caplog.text


class TestFKModificationSQL:
    """Test for BUG-001: FK Modification SQL Empty"""
    
    def test_fk_modification_generates_sql(self):
        """FK changes should generate DROP/ADD CONSTRAINT SQL."""
        source = Schema(tables=[
            Table(name='child', columns=[Column('id', 'INT')], 
                  foreign_keys=[ForeignKey('fk_child', ['parent_id'], 'parent', ['id'], on_delete='CASCADE')])
        ])
        target = Schema(tables=[
            Table(name='child', columns=[Column('id', 'INT')],
                  foreign_keys=[ForeignKey('fk_child', ['parent_id'], 'parent', ['id'], on_delete='SET NULL')])
        ])
        
        comparator = Comparator()
        plan = comparator.compare(source, target)
        
        generator = PostgresGenerator()
        sql = generator.generate_migration(plan)
        
        assert 'DROP CONSTRAINT' in sql
        assert 'ADD CONSTRAINT' in sql
        assert 'SET NULL' in sql


class TestBooleanType:
    """Test for BUG-002: BOOLEAN type handling"""
    
    def test_postgres_preserves_boolean(self):
        """PostgreSQL should keep BOOLEAN, not convert to TINYINT."""
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (is_active BOOLEAN);')
        
        assert len(schema.tables) == 1
        col = schema.tables[0].columns[0]
        assert col.data_type == 'BOOLEAN'
        assert 'TINYINT' not in col.data_type


class TestIndexColumns:
    """Test for BUG-003: Empty Index Columns"""
    
    def test_postgres_index_columns_parsed(self):
        """Index columns should be captured correctly."""
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE orders (id INT, created_at TIMESTAMP);
            CREATE INDEX idx_created ON orders(created_at);
        ''')
        
        table = schema.get_table('orders')
        assert table is not None
        assert len(table.indexes) == 1
        assert 'created_at' in table.indexes[0].columns


class TestCommentColumn:
    """Test for BUG-007: Column named 'comment' missing"""
    
    def test_comment_column_parsed(self):
        """Column named 'comment' should not be skipped."""
        parser = GenericSQLParser()
        schema = parser.parse('CREATE TABLE posts (id INT, comment TEXT);')
        
        table = schema.tables[0]
        cols = [c.name for c in table.columns]
        assert 'comment' in cols


class TestEncodingHandling:
    """Test for Minor: Encoding Error"""
    
    def test_binary_content_handled(self):
        """Binary content should not crash parser."""
        import tempfile
        import os
        from schemaforge.main import read_sql_source
        
        # Create a temp file with binary garbage
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.sql', delete=False) as f:
            f.write(b'\xff\xfe\x00\x00CREATE TABLE test (id INT);')
            temp_path = f.name
        
        try:
            content = read_sql_source(temp_path)
            assert 'CREATE TABLE' in content  # Should still find valid SQL parts
        finally:
            os.unlink(temp_path)


class TestGeneratorCoverage:
    """Tests to increase generator coverage."""
    
    def test_postgres_create_table(self):
        """Test PostgreSQL table creation SQL."""
        from schemaforge.comparator import MigrationPlan
        
        table = Table(name='users', columns=[
            Column('id', 'SERIAL', is_primary_key=True),
            Column('name', 'VARCHAR(100)', is_nullable=False),
            Column('email', 'TEXT')
        ])
        
        plan = MigrationPlan(new_tables=[table])
        
        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        assert '"users"' in sql
        
    def test_mysql_create_table(self):
        """Test MySQL table creation SQL."""
        from schemaforge.comparator import MigrationPlan
        
        table = Table(name='products', columns=[
            Column('id', 'INT', is_primary_key=True),
            Column('price', 'DECIMAL(10,2)')
        ])
        
        plan = MigrationPlan(new_tables=[table])
        
        gen = MySQLGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql
        
    def test_sqlite_create_table(self):
        """Test SQLite table creation SQL."""
        from schemaforge.comparator import MigrationPlan
        
        table = Table(name='items', columns=[
            Column('id', 'INTEGER', is_primary_key=True),
            Column('name', 'TEXT')
        ])
        
        plan = MigrationPlan(new_tables=[table])
        
        gen = SQLiteGenerator()
        sql = gen.generate_migration(plan)
        
        assert 'CREATE TABLE' in sql


class TestLongIdentifiers:
    """Test for DoS/Hang on long identifiers (regex fix)."""
    
    def test_long_identifier_no_hang(self):
        """Long identifiers should parse quickly without hanging."""
        import time
        parser = PostgresParser()
        
        long_name = 'a' * 200
        sql = f'CREATE TABLE "{long_name}" (id INT);'
        
        start = time.time()
        schema = parser.parse(sql)
        elapsed = time.time() - start
        
        assert elapsed < 5.0  # Should be instant, not hang
        assert len(schema.tables) == 1


class TestComparatorModifiedFKs:
    """Test modified_fks tracking in comparator."""
    
    def test_modified_fk_detected(self):
        """Changes to FK on_delete should be detected."""
        source = Schema(tables=[
            Table(name='orders', columns=[Column('id', 'INT')],
                  foreign_keys=[ForeignKey('fk_customer', ['customer_id'], 'customers', ['id'], on_delete='CASCADE')])
        ])
        target = Schema(tables=[
            Table(name='orders', columns=[Column('id', 'INT')],
                  foreign_keys=[ForeignKey('fk_customer', ['customer_id'], 'customers', ['id'], on_delete='SET NULL')])
        ])
        
        comparator = Comparator()
        plan = comparator.compare(source, target)
        
        assert len(plan.modified_tables) == 1
        diff = plan.modified_tables[0]
        assert len(diff.modified_fks) == 1
        old_fk, new_fk = diff.modified_fks[0]
        assert old_fk.on_delete == 'CASCADE'
        assert new_fk.on_delete == 'SET NULL'
