"""
Tests for main.py CLI functionality.
"""
import pytest
import tempfile
import os
import sys
from io import StringIO
from unittest.mock import patch, MagicMock

# Import CLI functions
from schemaforge.main import get_parser, get_generator, read_sql_source, main, _handle_output
from schemaforge.comparator import MigrationPlan, TableDiff
from schemaforge.models import Table, Column, Index, ForeignKey, CheckConstraint


class TestCLIHelpers:
    """Test helper functions in main.py"""
    
    def test_get_parser_mysql(self):
        from schemaforge.parsers.mysql import MySQLParser
        parser = get_parser('mysql')
        assert isinstance(parser, MySQLParser)
        
    def test_get_parser_postgres(self):
        from schemaforge.parsers.postgres import PostgresParser
        parser = get_parser('postgres')
        assert isinstance(parser, PostgresParser)
        
    def test_get_parser_sqlite(self):
        from schemaforge.parsers.sqlite import SQLiteParser
        parser = get_parser('sqlite')
        assert isinstance(parser, SQLiteParser)
        
    def test_get_parser_oracle(self):
        from schemaforge.parsers.oracle import OracleParser
        parser = get_parser('oracle')
        assert isinstance(parser, OracleParser)
        
    def test_get_parser_db2(self):
        from schemaforge.parsers.db2 import DB2Parser
        parser = get_parser('db2')
        assert isinstance(parser, DB2Parser)
        
    def test_get_parser_snowflake(self):
        from schemaforge.parsers.snowflake import SnowflakeParser
        parser = get_parser('snowflake')
        assert isinstance(parser, SnowflakeParser)
        
    def test_get_parser_invalid(self):
        with pytest.raises(ValueError, match="Unknown dialect"):
            get_parser('invalid_dialect')
            
    def test_get_generator_mysql(self):
        from schemaforge.generators.mysql import MySQLGenerator
        gen = get_generator('mysql')
        assert isinstance(gen, MySQLGenerator)
        
    def test_get_generator_postgres(self):
        from schemaforge.generators.postgres import PostgresGenerator
        gen = get_generator('postgres')
        assert isinstance(gen, PostgresGenerator)
        
    def test_get_generator_invalid(self):
        with pytest.raises(ValueError, match="Unknown dialect"):
            get_generator('invalid_dialect')


class TestReadSqlSource:
    """Test read_sql_source function"""
    
    def test_read_single_file(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write('CREATE TABLE test (id INT);')
            temp_path = f.name
        
        try:
            content = read_sql_source(temp_path)
            assert 'CREATE TABLE test' in content
        finally:
            os.unlink(temp_path)
            
    def test_read_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create SQL files
            with open(os.path.join(tmpdir, '01_users.sql'), 'w') as f:
                f.write('CREATE TABLE users (id INT);')
            with open(os.path.join(tmpdir, '02_orders.sql'), 'w') as f:
                f.write('CREATE TABLE orders (id INT);')
            
            content = read_sql_source(tmpdir)
            assert 'CREATE TABLE users' in content
            assert 'CREATE TABLE orders' in content
            
    def test_read_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="No .sql files found"):
                read_sql_source(tmpdir)
                
    def test_read_nonexistent_path(self):
        with pytest.raises(ValueError, match="Path not found"):
            read_sql_source('/nonexistent/path/file.sql')
            
    def test_read_binary_file(self):
        """Test that binary files don't crash (encoding fix)."""
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.sql', delete=False) as f:
            f.write(b'\xff\xfe\x00\x00SELECT 1;')
            temp_path = f.name
        
        try:
            content = read_sql_source(temp_path)
            # Should not crash, content may have replacement chars
            assert isinstance(content, str)
        finally:
            os.unlink(temp_path)


class TestHandleOutput:
    """Test _handle_output function"""
    
    def test_plan_output_new_tables(self, capsys):
        plan = MigrationPlan(
            new_tables=[Table(name='users', columns=[
                Column('id', 'INT'),
                Column('name', 'VARCHAR(100)')
            ])]
        )
        
        args = MagicMock()
        args.plan = True
        args.json_out = None
        args.sql_out = None
        args.no_color = True
        args.generate_rollback = False
        
        _handle_output(args, plan)
        
        captured = capsys.readouterr()
        assert 'Create' in captured.out
        assert 'users' in captured.out
        
    def test_plan_output_dropped_tables(self, capsys):
        plan = MigrationPlan(
            dropped_tables=[Table(name='old_table', columns=[])]
        )
        
        args = MagicMock()
        args.plan = True
        args.json_out = None
        args.sql_out = None
        args.no_color = True
        args.generate_rollback = False
        
        _handle_output(args, plan)
        
        captured = capsys.readouterr()
        assert 'Drop' in captured.out
        assert 'old_table' in captured.out
        
    def test_plan_output_modified_tables(self, capsys):
        diff = TableDiff(
            table_name='users',
            added_columns=[Column('email', 'TEXT')],
            dropped_columns=[Column('old_col', 'INT')],
            modified_columns=[(Column('name', 'VARCHAR(50)'), Column('name', 'VARCHAR(200)'))],
            added_indexes=[Index('idx_email', ['email'])],
            dropped_indexes=[Index('idx_old', ['old_col'])],
            added_fks=[ForeignKey('fk_dept', ['dept_id'], 'departments', ['id'])],
            dropped_fks=[ForeignKey('fk_old', ['old_id'], 'old_table', ['id'])],
            modified_fks=[(
                ForeignKey('fk_customer', ['cust_id'], 'customers', ['id'], on_delete='CASCADE'),
                ForeignKey('fk_customer', ['cust_id'], 'customers', ['id'], on_delete='SET NULL')
            )],
            added_checks=[CheckConstraint('chk_email', "email LIKE '%@%'")],
            dropped_checks=[CheckConstraint('chk_old', 'old > 0')],
            property_changes=['Comment: None -> Updated']
        )
        
        plan = MigrationPlan(modified_tables=[diff])
        
        args = MagicMock()
        args.plan = True
        args.json_out = None
        args.sql_out = None
        args.no_color = True
        args.generate_rollback = False
        
        _handle_output(args, plan)
        
        captured = capsys.readouterr()
        assert 'Modify Table' in captured.out
        assert 'Add Column' in captured.out
        assert 'Drop Column' in captured.out
        
    def test_json_output(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            plan = MigrationPlan(new_tables=[Table(name='test', columns=[])])
            
            args = MagicMock()
            args.plan = False
            args.json_out = temp_path
            args.sql_out = None
            args.no_color = True
            args.generate_rollback = False
            
            _handle_output(args, plan)
            
            import json
            with open(temp_path) as f:
                data = json.load(f)
            assert 'new_tables' in data
        finally:
            os.unlink(temp_path)
            
    def test_sql_output(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            temp_path = f.name
        
        try:
            plan = MigrationPlan(new_tables=[
                Table(name='test', columns=[Column('id', 'INT')])
            ])
            
            args = MagicMock()
            args.plan = False
            args.json_out = None
            args.sql_out = temp_path
            args.no_color = True
            args.generate_rollback = False
            args.dialect = 'postgres'
            
            _handle_output(args, plan)
            
            with open(temp_path) as f:
                sql = f.read()
            assert 'CREATE TABLE' in sql
        finally:
            os.unlink(temp_path)
            
    def test_no_output_specified(self, capsys):
        plan = MigrationPlan()
        
        args = MagicMock()
        args.plan = False
        args.json_out = None
        args.sql_out = None
        args.generate_rollback = False
        
        _handle_output(args, plan)
        
        captured = capsys.readouterr()
        assert 'No output action specified' in captured.out
        
    def test_no_changes_detected(self, capsys):
        plan = MigrationPlan()  # Empty plan
        
        args = MagicMock()
        args.plan = True
        args.json_out = None
        args.sql_out = None
        args.no_color = True
        args.generate_rollback = False
        
        _handle_output(args, plan)
        
        captured = capsys.readouterr()
        assert 'No changes detected' in captured.out


class TestMainFunction:
    """Test main() CLI entry point"""
    
    def test_compare_command(self, capsys):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write('CREATE TABLE test (id INT);')
            temp_path = f.name
        
        try:
            with patch('sys.argv', [
                'schemaforge', 'compare',
                '--source', temp_path,
                '--target', temp_path,
                '--dialect', 'postgres',
                '--plan'
            ]):
                main()
            
            captured = capsys.readouterr()
            assert 'No changes detected' in captured.out
        finally:
            os.unlink(temp_path)
            
    def test_compare_with_changes(self, capsys):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f1:
            f1.write('CREATE TABLE test (id INT);')
            source_path = f1.name
            
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f2:
            f2.write('CREATE TABLE test (id INT, name VARCHAR(100));')
            target_path = f2.name
        
        try:
            with patch('sys.argv', [
                'schemaforge', 'compare',
                '--source', source_path,
                '--target', target_path,
                '--dialect', 'postgres',
                '--plan'
            ]):
                main()
            
            captured = capsys.readouterr()
            assert 'Modify Table' in captured.out or 'name' in captured.out
        finally:
            os.unlink(source_path)
            os.unlink(target_path)
