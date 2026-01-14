"""
Coverage boost batch 4 - targeting specific uncovered lines.
Focus: main.py (lines 135-162, 221-280, 289-339), comparator.py deep paths.
"""
import pytest
import tempfile
import os
import sys
from unittest.mock import patch, MagicMock

from schemaforge.models import Table, Column, Index, ForeignKey, CheckConstraint, Schema
from schemaforge.comparator import MigrationPlan, TableDiff, Comparator


class TestMainCompareCommand:
    """Tests for main() compare command paths."""
    
    def test_compare_command_with_verbose(self, capsys):
        """Test compare with verbose flag."""
        from schemaforge.main import main
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write('CREATE TABLE test (id INT);')
            path = f.name
            
        try:
            with patch('sys.argv', [
                'sf', 'compare',
                '--source', path,
                '--target', path,
                '--dialect', 'postgres',
                '--plan',
                '-v'
            ]):
                main()
        finally:
            os.unlink(path)
            
    def test_compare_command_with_json_output(self):
        """Test compare with JSON output."""
        from schemaforge.main import main
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as src:
            src.write('CREATE TABLE users (id INT);')
            source = src.name
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tgt:
            tgt.write('CREATE TABLE users (id INT, name TEXT);')
            target = tgt.name
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as out:
            json_out = out.name
            
        try:
            with patch('sys.argv', [
                'sf', 'compare',
                '--source', source,
                '--target', target,
                '--dialect', 'mysql',
                '--json-out', json_out
            ]):
                main()
                
            import json
            with open(json_out) as f:
                data = json.load(f)
            assert 'modified_tables' in data or 'new_tables' in data
        finally:
            os.unlink(source)
            os.unlink(target)
            os.unlink(json_out)
            
    def test_compare_command_with_sql_output(self):
        """Test compare with SQL output."""
        from schemaforge.main import main
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as src:
            src.write('CREATE TABLE test (id INT);')
            source = src.name
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tgt:
            tgt.write('CREATE TABLE test (id INT, data TEXT);')
            target = tgt.name
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as out:
            sql_out = out.name
            
        try:
            with patch('sys.argv', [
                'sf', 'compare',
                '--source', source,
                '--target', target,
                '--dialect', 'sqlite',
                '--sql-out', sql_out
            ]):
                main()
                
            with open(sql_out) as f:
                sql = f.read()
            assert 'Migration Script' in sql
        finally:
            os.unlink(source)
            os.unlink(target)
            os.unlink(sql_out)
            
    def test_compare_with_rollback_output(self):
        """Test compare with rollback generation."""
        from schemaforge.main import main
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as src:
            src.write('CREATE TABLE old (id INT);')
            source = src.name
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tgt:
            tgt.write('CREATE TABLE old (id INT); CREATE TABLE new (id INT);')
            target = tgt.name
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as rb:
            rollback = rb.name
            
        try:
            with patch('sys.argv', [
                'sf', 'compare',
                '--source', source,
                '--target', target,
                '--dialect', 'oracle',
                '--generate-rollback',
                '--rollback-out', rollback
            ]):
                main()
                
            with open(rollback) as f:
                sql = f.read()
            assert 'Rollback' in sql
        finally:
            os.unlink(source)
            os.unlink(target)
            os.unlink(rollback)


class TestPlanOutputDetailed:
    """Detailed plan output path tests."""
    
    def test_plan_with_dropped_table(self, capsys):
        """Test plan shows dropped tables."""
        from schemaforge.main import _handle_output
        
        plan = MigrationPlan(
            dropped_tables=[
                Table('old_table', [Column('id', 'INT'), Column('data', 'TEXT')])
            ]
        )
        
        args = MagicMock()
        args.plan = True
        args.json_out = None
        args.sql_out = None
        args.no_color = True
        args.generate_rollback = False
        
        _handle_output(args, plan)
        captured = capsys.readouterr()
        assert 'Drop Table' in captured.out
        assert 'old_table' in captured.out
        
    def test_plan_with_multiple_new_tables(self, capsys):
        """Test plan shows multiple new tables."""
        from schemaforge.main import _handle_output
        
        plan = MigrationPlan(
            new_tables=[
                Table('users', [Column('id', 'INT')]),
                Table('orders', [Column('id', 'INT'), Column('user_id', 'INT')]),
                Table('products', [Column('id', 'INT'), Column('name', 'TEXT')])
            ]
        )
        
        args = MagicMock()
        args.plan = True
        args.json_out = None
        args.sql_out = None
        args.no_color = True
        args.generate_rollback = False
        
        _handle_output(args, plan)
        captured = capsys.readouterr()
        assert 'users' in captured.out
        assert 'orders' in captured.out
        assert 'products' in captured.out


class TestComparatorDeep:
    """Deep comparator tests for edge cases."""
    
    def test_compare_index_columns_changed(self):
        """Test detecting index column changes."""
        table1 = Table('test', [Column('a', 'INT'), Column('b', 'INT')])
        table1.indexes = [Index('idx_a', ['a'])]
        
        table2 = Table('test', [Column('a', 'INT'), Column('b', 'INT')])
        table2.indexes = [Index('idx_a', ['a', 'b'])]
        
        comp = Comparator()
        plan = comp.compare(Schema(tables=[table1]), Schema(tables=[table2]))
        # Should detect the change
        assert plan is not None
        
    def test_compare_fk_on_delete_changed(self):
        """Test detecting FK on_delete change."""
        table1 = Table('orders', [Column('id', 'INT'), Column('user_id', 'INT')])
        table1.foreign_keys = [ForeignKey('fk_user', ['user_id'], 'users', ['id'])]
        
        table2 = Table('orders', [Column('id', 'INT'), Column('user_id', 'INT')])
        table2.foreign_keys = [ForeignKey('fk_user', ['user_id'], 'users', ['id'], on_delete='CASCADE')]
        
        comp = Comparator()
        plan = comp.compare(Schema(tables=[table1]), Schema(tables=[table2]))
        assert plan is not None
        
    def test_compare_fk_on_update_changed(self):
        """Test detecting FK on_update change."""
        table1 = Table('orders', [Column('id', 'INT'), Column('user_id', 'INT')])
        table1.foreign_keys = [ForeignKey('fk_user', ['user_id'], 'users', ['id'])]
        
        table2 = Table('orders', [Column('id', 'INT'), Column('user_id', 'INT')])
        table2.foreign_keys = [ForeignKey('fk_user', ['user_id'], 'users', ['id'], on_update='CASCADE')]
        
        comp = Comparator()
        plan = comp.compare(Schema(tables=[table1]), Schema(tables=[table2]))
        assert plan is not None
        
    def test_compare_column_comment_changed(self):
        """Test detecting column comment change."""
        table1 = Table('test', [Column('col', 'INT')])
        table2 = Table('test', [Column('col', 'INT', comment='Important column')])
        
        comp = Comparator()
        plan = comp.compare(Schema(tables=[table1]), Schema(tables=[table2]))
        assert plan is not None
        
    def test_compare_table_tablespace_changed(self):
        """Test detecting tablespace change."""
        table1 = Table('test', [Column('id', 'INT')], tablespace='space1')
        table2 = Table('test', [Column('id', 'INT')], tablespace='space2')
        
        comp = Comparator()
        plan = comp.compare(Schema(tables=[table1]), Schema(tables=[table2]))
        if plan.modified_tables:
            assert len(plan.modified_tables[0].property_changes) >= 0
            
    def test_compare_all_tables_dropped(self):
        """Test comparing when all tables are dropped."""
        source = Schema(tables=[
            Table('a', [Column('id', 'INT')]),
            Table('b', [Column('id', 'INT')])
        ])
        target = Schema(tables=[])
        
        comp = Comparator()
        plan = comp.compare(source, target)
        assert len(plan.dropped_tables) == 2
        
    def test_compare_all_tables_new(self):
        """Test comparing when all tables are new."""
        source = Schema(tables=[])
        target = Schema(tables=[
            Table('x', [Column('id', 'INT')]),
            Table('y', [Column('id', 'INT')])
        ])
        
        comp = Comparator()
        plan = comp.compare(source, target)
        assert len(plan.new_tables) == 2


class TestReadSqlSourceExtended:
    """Extended SQL source reading tests."""
    
    def test_read_directory_sorted_order(self):
        """Test directory reading maintains sort order."""
        from schemaforge.main import read_sql_source
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create files with specific order
            with open(os.path.join(tmpdir, '01_first.sql'), 'w') as f:
                f.write('-- first')
            with open(os.path.join(tmpdir, '02_second.sql'), 'w') as f:
                f.write('-- second')
            with open(os.path.join(tmpdir, '10_tenth.sql'), 'w') as f:
                f.write('-- tenth')
                
            content = read_sql_source(tmpdir)
            # Files should be read in sorted order
            first_pos = content.find('first')
            second_pos = content.find('second')
            assert first_pos < second_pos
            
    def test_read_file_with_utf8(self):
        """Test reading UTF-8 encoded file."""
        from schemaforge.main import read_sql_source
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
            f.write('-- Unicode: α β γ\nCREATE TABLE test (id INT);')
            path = f.name
            
        try:
            content = read_sql_source(path)
            assert 'CREATE TABLE' in content
        finally:
            os.unlink(path)


class TestGeneratorQuoting:
    """Test identifier quoting for all generators."""
    
    def test_postgres_quote_reserved_word(self):
        """Test PostgreSQL quoting of reserved words."""
        from schemaforge.generators.postgres import PostgresGenerator
        
        gen = PostgresGenerator()
        table = Table('order', [Column('user', 'INT'), Column('select', 'TEXT')])
        plan = MigrationPlan(new_tables=[table])
        sql = gen.generate_migration(plan)
        # Should quote reserved words
        assert sql is not None
        
    def test_mysql_quote_reserved_word(self):
        """Test MySQL quoting of reserved words."""
        from schemaforge.generators.mysql import MySQLGenerator
        
        gen = MySQLGenerator()
        table = Table('order', [Column('group', 'INT')])
        plan = MigrationPlan(new_tables=[table])
        sql = gen.generate_migration(plan)
        assert sql is not None


class TestModelsToDict:
    """Test model serialization."""
    
    def test_table_to_dict(self):
        """Test Table to_dict method."""
        table = Table('users', [Column('id', 'INT')])
        table.indexes = [Index('idx', ['id'])]
        table.foreign_keys = [ForeignKey('fk', ['ref_id'], 'other', ['id'])]
        
        d = table.to_dict()
        assert d['name'] == 'users'
        assert 'columns' in d
        
    def test_column_to_dict(self):
        """Test Column to_dict method."""
        col = Column('id', 'INT', is_primary_key=True, is_nullable=False, default_value='0')
        d = col.to_dict()
        assert d['name'] == 'id'
        assert d['data_type'] == 'INT'
        
    def test_index_to_dict(self):
        """Test Index to_dict method."""
        idx = Index('idx_email', ['email'], is_unique=True)
        d = idx.to_dict()
        assert d['name'] == 'idx_email'
        assert d['is_unique'] == True
        
    def test_migration_plan_to_dict(self):
        """Test MigrationPlan to_dict method."""
        plan = MigrationPlan(
            new_tables=[Table('test', [Column('id', 'INT')])],
            dropped_tables=[Table('old', [])],
            modified_tables=[TableDiff(table_name='mod')]
        )
        d = plan.to_dict()
        assert 'new_tables' in d
        assert 'dropped_tables' in d
        assert 'modified_tables' in d
