"""
Tests for test coverage boost - targeting uncovered modules.
This file targets: generic regex parser, exceptions, strict mode, rollback migrations.
"""
import pytest
from schemaforge.parsers.generic import GenericRegexParser
from schemaforge.exceptions import StrictModeError, SchemaForgeError, ValidationError, DialectError
from schemaforge.generators.generic import GenericGenerator
from schemaforge.comparator import MigrationPlan, TableDiff
from schemaforge.models import Table, Column, Index, ForeignKey


class TestGenericRegexParser:
    """Test the GenericRegexParser class."""
    
    def test_parse_simple_table(self):
        parser = GenericRegexParser()
        sql = "CREATE TABLE users (id INT, name VARCHAR(100));"
        schema = parser.parse(sql)
        assert len(schema.tables) == 1
        assert schema.tables[0].name == 'users'
        
    def test_parse_table_with_not_null(self):
        parser = GenericRegexParser()
        sql = "CREATE TABLE orders (id INT NOT NULL, total DECIMAL(10,2));"
        schema = parser.parse(sql)
        table = schema.tables[0]
        assert table.columns[0].is_nullable == False
        
    def test_parse_table_with_primary_key(self):
        parser = GenericRegexParser()
        sql = "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(50));"
        schema = parser.parse(sql)
        table = schema.tables[0]
        assert table.columns[0].is_primary_key == True
        
    def test_parse_if_not_exists(self):
        parser = GenericRegexParser()
        sql = "CREATE TABLE IF NOT EXISTS test (id INT);"
        schema = parser.parse(sql)
        assert len(schema.tables) == 1
        assert schema.tables[0].name == 'test'
        
    def test_parse_with_comments(self):
        parser = GenericRegexParser()
        sql = """
        -- This is a comment
        CREATE TABLE items (id INT);
        -- Another comment
        """
        schema = parser.parse(sql)
        assert len(schema.tables) == 1
        
    def test_parse_multiple_tables(self):
        parser = GenericRegexParser()
        sql = """
        CREATE TABLE users (id INT);
        CREATE TABLE orders (id INT, user_id INT);
        """
        schema = parser.parse(sql)
        assert len(schema.tables) == 2
        
    def test_parse_quoted_table_name(self):
        parser = GenericRegexParser()
        sql = 'CREATE TABLE "my_table" (id INT);'
        schema = parser.parse(sql)
        assert schema.tables[0].name == 'my_table'
        
    def test_parse_with_primary_key_constraint(self):
        parser = GenericRegexParser()
        sql = "CREATE TABLE test (id INT, PRIMARY KEY(id));"
        schema = parser.parse(sql)
        # Primary key constraint is recognized but skipped
        assert len(schema.tables) == 1
        
    def test_clean_name_removes_quotes(self):
        parser = GenericRegexParser()
        assert parser._clean_name('"table"') == 'table'
        assert parser._clean_name('`table`') == 'table'
        assert parser._clean_name('[table]') == 'table'
        assert parser._clean_name('  table  ') == 'table'
        
    def test_parse_empty_definition(self):
        parser = GenericRegexParser()
        sql = "CREATE TABLE test (id INT, , name TEXT);"  
        schema = parser.parse(sql)
        # Should handle empty definitions gracefully
        assert len(schema.tables) == 1


class TestExceptions:
    """Test custom exception classes."""
    
    def test_strict_mode_error_message(self):
        err = StrictModeError("SELECT * FROM users;", "Unknown statement type")
        assert "Unknown statement type" in str(err)
        assert "SELECT" in str(err)
        
    def test_strict_mode_error_long_statement(self):
        long_stmt = "A" * 200
        err = StrictModeError(long_stmt, "Parse failure")
        # Should truncate to 100 chars + "..."
        assert len(str(err)) < 200
        assert "..." in str(err)
        
    def test_strict_mode_error_attributes(self):
        err = StrictModeError("stmt", "reason")
        assert err.statement == "stmt"
        assert err.reason == "reason"
        
    def test_schemaforge_error_base(self):
        err = SchemaForgeError("base error")
        assert "base error" in str(err)
        
    def test_validation_error(self):
        err = ValidationError("invalid schema")
        assert "invalid schema" in str(err)
        
    def test_dialect_error(self):
        err = DialectError("unknown dialect: xyz")
        assert "xyz" in str(err)
        
    def test_exception_inheritance(self):
        assert issubclass(StrictModeError, SchemaForgeError)
        assert issubclass(ValidationError, SchemaForgeError)
        assert issubclass(DialectError, SchemaForgeError)
        assert issubclass(SchemaForgeError, Exception)


class TestRollbackMigrationGeneration:
    """Test rollback migration generation."""
    
    def test_rollback_new_table(self):
        gen = GenericGenerator()
        plan = MigrationPlan(new_tables=[
            Table('users', [Column('id', 'INT')])
        ])
        sql = gen.generate_rollback_migration(plan)
        assert 'DROP TABLE IF EXISTS' in sql
        assert 'users' in sql
        
    def test_rollback_dropped_table(self):
        gen = GenericGenerator()
        plan = MigrationPlan(dropped_tables=[
            Table('old_users', [Column('id', 'INT'), Column('name', 'TEXT')])
        ])
        sql = gen.generate_rollback_migration(plan)
        assert 'CREATE TABLE' in sql
        assert 'old_users' in sql
        
    def test_rollback_added_column(self):
        gen = GenericGenerator()
        diff = TableDiff(
            table_name='users',
            added_columns=[Column('email', 'TEXT')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        sql = gen.generate_rollback_migration(plan)
        assert 'DROP COLUMN' in sql
        assert 'email' in sql
        
    def test_rollback_dropped_column(self):
        gen = GenericGenerator()
        diff = TableDiff(
            table_name='users',
            dropped_columns=[Column('old_field', 'VARCHAR(100)', is_nullable=False)]
        )
        plan = MigrationPlan(modified_tables=[diff])
        sql = gen.generate_rollback_migration(plan)
        assert 'ADD COLUMN' in sql
        assert 'old_field' in sql
        assert 'NOT NULL' in sql
        
    def test_rollback_dropped_column_with_default(self):
        gen = GenericGenerator()
        diff = TableDiff(
            table_name='products',
            dropped_columns=[Column('price', 'DECIMAL(10,2)', default_value='0.00')]
        )
        plan = MigrationPlan(modified_tables=[diff])
        sql = gen.generate_rollback_migration(plan)
        assert 'DEFAULT' in sql
        assert '0.00' in sql
        
    def test_rollback_modified_column(self):
        gen = GenericGenerator()
        diff = TableDiff(
            table_name='users',
            modified_columns=[
                (Column('name', 'VARCHAR(50)'), Column('name', 'VARCHAR(200)'))
            ]
        )
        plan = MigrationPlan(modified_tables=[diff])
        sql = gen.generate_rollback_migration(plan)
        # Should revert to old type
        assert 'VARCHAR(50)' in sql
        
    def test_rollback_added_index(self):
        gen = GenericGenerator()
        diff = TableDiff(
            table_name='orders',
            added_indexes=[Index('idx_date', ['order_date'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        sql = gen.generate_rollback_migration(plan)
        assert 'DROP INDEX' in sql
        assert 'idx_date' in sql
        
    def test_rollback_dropped_index(self):
        gen = GenericGenerator()
        diff = TableDiff(
            table_name='orders',
            dropped_indexes=[Index('idx_old', ['old_col'], is_unique=True)]
        )
        plan = MigrationPlan(modified_tables=[diff])
        sql = gen.generate_rollback_migration(plan)
        assert 'CREATE' in sql
        assert 'UNIQUE' in sql
        
    def test_rollback_added_fk(self):
        gen = GenericGenerator()
        diff = TableDiff(
            table_name='orders',
            added_fks=[ForeignKey('fk_customer', ['customer_id'], 'customers', ['id'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        sql = gen.generate_rollback_migration(plan)
        assert 'DROP FOREIGN KEY' in sql
        
    def test_rollback_dropped_fk(self):
        gen = GenericGenerator()
        diff = TableDiff(
            table_name='orders',
            dropped_fks=[ForeignKey('fk_old', ['old_id'], 'old_table', ['id'])]
        )
        plan = MigrationPlan(modified_tables=[diff])
        sql = gen.generate_rollback_migration(plan)
        assert 'ADD CONSTRAINT' in sql
        assert 'FOREIGN KEY' in sql
        
    def test_rollback_empty_plan(self):
        gen = GenericGenerator()
        plan = MigrationPlan()
        sql = gen.generate_rollback_migration(plan)
        assert sql == ""
        
    def test_rollback_complex_migration(self):
        gen = GenericGenerator()
        table = Table('new_table', [
            Column('id', 'INT'),
            Column('data', 'TEXT')
        ])
        table.indexes = [Index('idx_data', ['data'])]
        
        diff = TableDiff(
            table_name='existing',
            added_columns=[Column('new_col', 'INT')],
            dropped_columns=[Column('old_col', 'TEXT')],
            added_indexes=[Index('idx_new', ['new_col'])]
        )
        
        plan = MigrationPlan(
            new_tables=[table],
            modified_tables=[diff]
        )
        sql = gen.generate_rollback_migration(plan)
        
        # Should have DROP TABLE for new table
        assert 'DROP TABLE' in sql
        # Should have DROP COLUMN for added column
        assert 'DROP COLUMN' in sql
        # Should have ADD COLUMN for dropped column
        assert 'ADD COLUMN' in sql


class TestStrictModeIntegration:
    """Test strict mode across different parsers."""
    
    def test_postgres_strict_mode_invalid(self):
        from schemaforge.parsers.postgres import PostgresParser
        parser = PostgresParser(strict=True)
        with pytest.raises(StrictModeError):
            parser.parse("INVALID STATEMENT;")
            
    def test_postgres_strict_mode_valid(self):
        from schemaforge.parsers.postgres import PostgresParser
        parser = PostgresParser(strict=True)
        schema = parser.parse("CREATE TABLE test (id INT);")
        assert len(schema.tables) == 1
        
    def test_mysql_strict_mode(self):
        from schemaforge.parsers.mysql import MySQLParser
        parser = MySQLParser(strict=True)
        schema = parser.parse("CREATE TABLE test (id INT);")
        assert len(schema.tables) == 1
        
    def test_sqlite_strict_mode(self):
        from schemaforge.parsers.sqlite import SQLiteParser
        parser = SQLiteParser(strict=True)
        schema = parser.parse("CREATE TABLE test (id INTEGER);")
        assert len(schema.tables) == 1
        
    def test_oracle_strict_mode(self):
        from schemaforge.parsers.oracle import OracleParser
        parser = OracleParser(strict=True)
        schema = parser.parse("CREATE TABLE test (id NUMBER);")
        assert len(schema.tables) == 1
        
    def test_snowflake_strict_mode(self):
        from schemaforge.parsers.snowflake import SnowflakeParser
        parser = SnowflakeParser(strict=True)
        schema = parser.parse("CREATE TABLE test (id NUMBER);")
        assert len(schema.tables) == 1
        
    def test_db2_strict_mode(self):
        from schemaforge.parsers.db2 import DB2Parser
        parser = DB2Parser(strict=True)
        schema = parser.parse("CREATE TABLE test (id INTEGER);")
        assert len(schema.tables) == 1
        
    def test_generic_sql_strict_mode(self):
        from schemaforge.parsers.generic_sql import GenericSQLParser
        parser = GenericSQLParser(strict=True)
        with pytest.raises(StrictModeError):
            parser.parse("INVALID STATEMENT;")


class TestMainCLIExtended:
    """Additional CLI tests for coverage."""
    
    def test_get_parser_with_strict(self):
        from schemaforge.main import get_parser
        parser = get_parser('postgres', strict=True)
        assert parser.strict == True
        
    def test_get_parser_without_strict(self):
        from schemaforge.main import get_parser
        parser = get_parser('mysql', strict=False)
        assert parser.strict == False
        
    def test_get_parser_all_dialects_strict(self):
        from schemaforge.main import get_parser
        for dialect in ['mysql', 'postgres', 'sqlite', 'oracle', 'db2', 'snowflake']:
            parser = get_parser(dialect, strict=True)
            assert parser.strict == True
