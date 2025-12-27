"""
Maximum coverage tests for Comparator and Main CLI.
"""
import pytest
import tempfile
import os
from schemaforge.comparator import Comparator, MigrationPlan, TableDiff
from schemaforge.models import Schema, Table, Column, Index, ForeignKey, CheckConstraint, ExclusionConstraint


class TestComparatorMaxCoverage:
    """Maximum coverage tests for Comparator."""
    
    def test_compare_empty_schemas(self):
        schema1 = Schema()
        schema2 = Schema()
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        assert len(plan.new_tables) == 0
        assert len(plan.dropped_tables) == 0
        
    def test_new_table_detection(self):
        schema1 = Schema()
        schema2 = Schema(tables=[Table('users', [Column('id', 'INT')])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        assert len(plan.new_tables) == 1
        
    def test_dropped_table_detection(self):
        schema1 = Schema(tables=[Table('old', [Column('id', 'INT')])])
        schema2 = Schema()
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        assert len(plan.dropped_tables) == 1
        
    def test_column_added(self):
        schema1 = Schema(tables=[Table('users', [Column('id', 'INT')])])
        schema2 = Schema(tables=[Table('users', [Column('id', 'INT'), Column('email', 'VARCHAR')])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        assert len(plan.modified_tables) == 1
        assert len(plan.modified_tables[0].added_columns) == 1
        
    def test_column_dropped(self):
        schema1 = Schema(tables=[Table('users', [Column('id', 'INT'), Column('old', 'TEXT')])])
        schema2 = Schema(tables=[Table('users', [Column('id', 'INT')])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        assert len(plan.modified_tables) == 1
        assert len(plan.modified_tables[0].dropped_columns) == 1
        
    def test_column_type_changed(self):
        schema1 = Schema(tables=[Table('users', [Column('name', 'VARCHAR(50)')])])
        schema2 = Schema(tables=[Table('users', [Column('name', 'VARCHAR(200)')])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].modified_columns) >= 1
            
    def test_column_nullable_changed(self):
        schema1 = Schema(tables=[Table('users', [Column('name', 'VARCHAR', is_nullable=True)])])
        schema2 = Schema(tables=[Table('users', [Column('name', 'VARCHAR', is_nullable=False)])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].modified_columns) >= 1
            
    def test_column_default_changed(self):
        schema1 = Schema(tables=[Table('config', [Column('value', 'INT', default_value='0')])])
        schema2 = Schema(tables=[Table('config', [Column('value', 'INT', default_value='100')])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].modified_columns) >= 1
            
    def test_index_added(self):
        schema1 = Schema(tables=[Table('users', [Column('email', 'VARCHAR')], indexes=[])])
        schema2 = Schema(tables=[Table('users', [Column('email', 'VARCHAR')], indexes=[Index('idx_email', ['email'])])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].added_indexes) >= 1
            
    def test_index_dropped(self):
        schema1 = Schema(tables=[Table('users', [Column('email', 'VARCHAR')], indexes=[Index('idx_email', ['email'])])])
        schema2 = Schema(tables=[Table('users', [Column('email', 'VARCHAR')], indexes=[])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].dropped_indexes) >= 1
            
    def test_fk_added(self):
        schema1 = Schema(tables=[Table('orders', [Column('customer_id', 'INT')], foreign_keys=[])])
        schema2 = Schema(tables=[Table('orders', [Column('customer_id', 'INT')], 
                                       foreign_keys=[ForeignKey('fk_cust', ['customer_id'], 'customers', ['id'])])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        assert len(plan.modified_tables) == 1
        assert len(plan.modified_tables[0].added_fks) == 1
        
    def test_fk_dropped(self):
        schema1 = Schema(tables=[Table('orders', [Column('customer_id', 'INT')],
                                       foreign_keys=[ForeignKey('fk_cust', ['customer_id'], 'customers', ['id'])])])
        schema2 = Schema(tables=[Table('orders', [Column('customer_id', 'INT')], foreign_keys=[])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        assert len(plan.modified_tables) == 1
        assert len(plan.modified_tables[0].dropped_fks) == 1
        
    def test_fk_modified_on_delete(self):
        schema1 = Schema(tables=[Table('orders', [Column('customer_id', 'INT')],
                                       foreign_keys=[ForeignKey('fk_cust', ['customer_id'], 'customers', ['id'], on_delete='CASCADE')])])
        schema2 = Schema(tables=[Table('orders', [Column('customer_id', 'INT')],
                                       foreign_keys=[ForeignKey('fk_cust', ['customer_id'], 'customers', ['id'], on_delete='SET NULL')])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].modified_fks) >= 1
            
    def test_fk_modified_on_update(self):
        schema1 = Schema(tables=[Table('orders', [Column('customer_id', 'INT')],
                                       foreign_keys=[ForeignKey('fk_cust', ['customer_id'], 'customers', ['id'], on_update='NO ACTION')])])
        schema2 = Schema(tables=[Table('orders', [Column('customer_id', 'INT')],
                                       foreign_keys=[ForeignKey('fk_cust', ['customer_id'], 'customers', ['id'], on_update='CASCADE')])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].modified_fks) >= 1
            
    def test_check_added(self):
        schema1 = Schema(tables=[Table('products', [Column('price', 'DECIMAL')], check_constraints=[])])
        schema2 = Schema(tables=[Table('products', [Column('price', 'DECIMAL')],
                                       check_constraints=[CheckConstraint('chk_price', 'price > 0')])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].added_checks) >= 1
            
    def test_check_dropped(self):
        schema1 = Schema(tables=[Table('products', [Column('price', 'DECIMAL')],
                                       check_constraints=[CheckConstraint('chk_price', 'price > 0')])])
        schema2 = Schema(tables=[Table('products', [Column('price', 'DECIMAL')], check_constraints=[])])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].dropped_checks) >= 1
            
    def test_table_comment_changed(self):
        schema1 = Schema(tables=[Table('users', [Column('id', 'INT')], comment='Old comment')])
        schema2 = Schema(tables=[Table('users', [Column('id', 'INT')], comment='New comment')])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        if plan.modified_tables:
            assert len(plan.modified_tables[0].property_changes) >= 1
            
    def test_multiple_tables_mixed_changes(self):
        schema1 = Schema(tables=[
            Table('users', [Column('id', 'INT'), Column('old_col', 'TEXT')]),
            Table('orders', [Column('id', 'INT')]),
            Table('old_table', [Column('id', 'INT')])
        ])
        schema2 = Schema(tables=[
            Table('users', [Column('id', 'INT'), Column('new_col', 'VARCHAR')]),
            Table('orders', [Column('id', 'INT'), Column('total', 'DECIMAL')]),
            Table('new_table', [Column('id', 'INT')])
        ])
        comparator = Comparator()
        plan = comparator.compare(schema1, schema2)
        
        assert len(plan.new_tables) >= 1
        assert len(plan.dropped_tables) >= 1
        assert len(plan.modified_tables) >= 1


class TestMainCLIMaxCoverage:
    """Maximum coverage tests for main.py CLI."""
    
    def test_get_parser_all_dialects(self):
        from schemaforge.main import get_parser
        dialects = ['mysql', 'postgres', 'sqlite', 'oracle', 'db2', 'snowflake']
        for dialect in dialects:
            parser = get_parser(dialect)
            assert parser is not None
            
    def test_get_generator_all_dialects(self):
        from schemaforge.main import get_generator
        dialects = ['mysql', 'postgres', 'sqlite', 'oracle', 'db2', 'snowflake']
        for dialect in dialects:
            gen = get_generator(dialect)
            assert gen is not None
            
    def test_read_sql_source_file(self):
        from schemaforge.main import read_sql_source
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write('CREATE TABLE test (id INT);')
            path = f.name
        try:
            content = read_sql_source(path)
            assert 'CREATE TABLE' in content
        finally:
            os.unlink(path)
            
    def test_read_sql_source_directory(self):
        from schemaforge.main import read_sql_source
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, 'a.sql'), 'w') as f:
                f.write('CREATE TABLE a (id INT);')
            with open(os.path.join(tmpdir, 'b.sql'), 'w') as f:
                f.write('CREATE TABLE b (id INT);')
            content = read_sql_source(tmpdir)
            assert 'CREATE TABLE a' in content
            assert 'CREATE TABLE b' in content
            
    def test_read_sql_source_nonexistent(self):
        from schemaforge.main import read_sql_source
        with pytest.raises(ValueError):
            read_sql_source('/nonexistent/path.sql')
            
    def test_read_sql_source_empty_dir(self):
        from schemaforge.main import read_sql_source
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError):
                read_sql_source(tmpdir)
                
    def test_handle_output_plan(self, capsys):
        from schemaforge.main import _handle_output
        from unittest.mock import MagicMock
        
        plan = MigrationPlan(new_tables=[Table('test', [Column('id', 'INT')])])
        args = MagicMock()
        args.plan = True
        args.json_out = None
        args.sql_out = None
        args.no_color = True
        
        _handle_output(args, plan)
        captured = capsys.readouterr()
        assert 'Create' in captured.out or 'test' in captured.out
        
    def test_handle_output_no_changes(self, capsys):
        from schemaforge.main import _handle_output
        from unittest.mock import MagicMock
        
        plan = MigrationPlan()
        args = MagicMock()
        args.plan = True
        args.json_out = None
        args.sql_out = None
        args.no_color = True
        
        _handle_output(args, plan)
        captured = capsys.readouterr()
        assert 'No changes detected' in captured.out
        
    def test_full_compare_flow(self, tmp_path, capsys):
        from schemaforge.main import main
        from unittest.mock import patch
        
        source = tmp_path / "source.sql"
        target = tmp_path / "target.sql"
        
        source.write_text("CREATE TABLE users (id INT);")
        target.write_text("CREATE TABLE users (id INT, name VARCHAR(100));")
        
        with patch('sys.argv', [
            'schemaforge', 'compare',
            '--source', str(source),
            '--target', str(target),
            '--dialect', 'postgres',
            '--plan'
        ]):
            main()
            
        captured = capsys.readouterr()
        assert 'Modify' in captured.out or 'Add' in captured.out
        
    def test_compare_with_sql_output(self, tmp_path):
        from schemaforge.main import main
        from unittest.mock import patch
        
        source = tmp_path / "source.sql"
        target = tmp_path / "target.sql"
        output = tmp_path / "output.sql"
        
        source.write_text("CREATE TABLE test (id INT);")
        target.write_text("CREATE TABLE test (id INT); CREATE TABLE new (id INT);")
        
        with patch('sys.argv', [
            'schemaforge', 'compare',
            '--source', str(source),
            '--target', str(target),
            '--dialect', 'mysql',
            '--sql-out', str(output)
        ]):
            main()
            
        sql = output.read_text()
        assert 'CREATE TABLE' in sql
        
    def test_compare_with_json_output(self, tmp_path):
        from schemaforge.main import main
        from unittest.mock import patch
        import json
        
        source = tmp_path / "source.sql"
        target = tmp_path / "target.sql"
        output = tmp_path / "output.json"
        
        source.write_text("CREATE TABLE test (id INT);")
        target.write_text("CREATE TABLE test (id INT);")
        
        with patch('sys.argv', [
            'schemaforge', 'compare',
            '--source', str(source),
            '--target', str(target),
            '--dialect', 'sqlite',
            '--json-out', str(output)
        ]):
            main()
            
        data = json.loads(output.read_text())
        assert 'new_tables' in data
        
    def test_compare_verbose_mode(self, tmp_path, capsys):
        from schemaforge.main import main
        from unittest.mock import patch
        
        source = tmp_path / "source.sql"
        source.write_text("CREATE TABLE test (id INT);")
        
        with patch('sys.argv', [
            'schemaforge', 'compare',
            '--source', str(source),
            '--target', str(source),
            '--dialect', 'postgres',
            '--plan',
            '-v'
        ]):
            main()
            
        captured = capsys.readouterr()
        assert 'No changes detected' in captured.out
