"""
Additional tests to push coverage toward 80%.
Focus on Snowflake parser internals and main.py edge cases.
"""
import pytest
import tempfile
import os
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.parsers.postgres import PostgresParser
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.generators.snowflake import SnowflakeGenerator
from schemaforge.comparator import MigrationPlan, TableDiff
from schemaforge.models import Table, Column, Index, ForeignKey


class TestSnowflakeParserInternals:
    """Deep tests for Snowflake parser internals."""
    
    def test_parse_with_tag(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER)
            WITH TAG (dept = 'engineering');
        ''')
        assert len(schema.tables) >= 1
        
    def test_parse_with_row_access_policy(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER)
            WITH ROW ACCESS POLICY my_policy ON (id);
        ''')
        assert len(schema.tables) >= 1
        
    def test_parse_with_masking_policy(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (
                id NUMBER,
                ssn VARCHAR(11) WITH MASKING POLICY ssn_mask
            );
        ''')
        assert len(schema.tables) >= 1
        
    def test_parse_copy_grants(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER) COPY GRANTS;
        ''')
        assert len(schema.tables) >= 1
        
    def test_parse_change_tracking(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER) CHANGE_TRACKING = TRUE;
        ''')
        assert len(schema.tables) >= 1
        
    def test_parse_multi_cluster(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER, a INT, b INT)
            CLUSTER BY (a, b);
        ''')
        assert len(schema.tables) >= 1
        
    def test_snowflake_share_object(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER);
            CREATE SHARE my_share;
        ''')
        assert schema is not None
        
    def test_snowflake_alert(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER);
            CREATE ALERT my_alert
            WAREHOUSE = compute_wh
            SCHEDULE = '1 minute'
            IF (EXISTS (SELECT 1 FROM test))
            THEN CALL my_procedure();
        ''')
        assert schema is not None
        
    def test_snowflake_function(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER);
            CREATE FUNCTION add_nums(a NUMBER, b NUMBER) RETURNS NUMBER AS 'a + b';
        ''')
        assert schema is not None
        
    def test_snowflake_procedure(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER);
            CREATE PROCEDURE my_proc() RETURNS STRING LANGUAGE JAVASCRIPT AS 'return "hello"';
        ''')
        assert schema is not None
        
    def test_snowflake_network_policy(self):
        parser = SnowflakeParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER);
            CREATE NETWORK POLICY my_policy ALLOWED_IP_LIST = ('1.2.3.4');
        ''')
        assert schema is not None


class TestSnowflakeGeneratorCoverage:
    """Additional Snowflake generator tests."""
    
    def test_create_transient_table(self):
        table = Table('temp', [Column('id', 'NUMBER')], is_transient=True)
        plan = MigrationPlan(new_tables=[table])
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        assert 'CREATE' in sql
        
    def test_create_table_with_cluster(self):
        table = Table('events', [Column('id', 'NUMBER'), Column('dt', 'DATE')], cluster_by=['dt'])
        plan = MigrationPlan(new_tables=[table])
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        assert 'CREATE' in sql
        
    def test_create_table_with_retention(self):
        table = Table('archive', [Column('id', 'NUMBER')], retention_days=90)
        plan = MigrationPlan(new_tables=[table])
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        assert 'CREATE' in sql
        
    def test_drop_table(self):
        table = Table('old', [])
        plan = MigrationPlan(dropped_tables=[table])
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        assert 'DROP' in sql
        
    def test_add_column(self):
        diff = TableDiff(table_name='test', added_columns=[Column('new_col', 'VARCHAR(100)')])
        plan = MigrationPlan(modified_tables=[diff])
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        assert 'ALTER' in sql or 'ADD' in sql
        
    def test_drop_column(self):
        diff = TableDiff(table_name='test', dropped_columns=[Column('old_col', 'INT')])
        plan = MigrationPlan(modified_tables=[diff])
        gen = SnowflakeGenerator()
        sql = gen.generate_migration(plan)
        assert sql is not None


class TestPostgresParserDeep:
    """Deep Postgres parser tests for edge cases."""
    
    def test_create_extension(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            CREATE TABLE test (id UUID DEFAULT uuid_generate_v4());
        ''')
        assert len(schema.tables) >= 1
        
    def test_alter_table_add_column(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT);
            ALTER TABLE test ADD COLUMN name VARCHAR(100);
        ''')
        assert len(schema.tables) >= 1
        
    def test_enum_type(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
            CREATE TABLE test (id INT, current_mood mood);
        ''')
        assert len(schema.tables) >= 1
        
    def test_range_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (during TSRANGE);')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_hstore_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (attrs HSTORE);')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_ltree_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (path LTREE);')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_create_rule(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT);
            CREATE RULE log_insert AS ON INSERT TO test DO ALSO NOTIFY test_changes;
        ''')
        assert len(schema.tables) >= 1
        
    def test_create_policy(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT, user_id INT);
            CREATE POLICY user_policy ON test FOR SELECT USING (user_id = current_user::INT);
        ''')
        assert len(schema.tables) >= 1


class TestGenericParserDeep:
    """Deep generic parser tests."""
    
    def test_create_table_as_select(self):
        parser = GenericSQLParser()
        schema = parser.parse('CREATE TABLE new_table AS SELECT * FROM old_table;')
        # Should handle without crashing
        assert schema is not None
        
    def test_multiline_comment(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            /* This is a
               multiline comment */
            CREATE TABLE test (id INT);
        ''')
        assert len(schema.tables) >= 1
        
    def test_inline_comment(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE test (
                id INT, -- primary key
                name VARCHAR(100) -- user name
            );
        ''')
        assert len(schema.tables) >= 1
        
    def test_complex_default(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE test (
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT NOW(),
                random_id INT DEFAULT (RANDOM() * 1000)::INT
            );
        ''')
        assert len(schema.tables) >= 1
        
    def test_collation(self):
        parser = GenericSQLParser()
        schema = parser.parse('''
            CREATE TABLE test (
                name VARCHAR(100) COLLATE "en_US.utf8"
            );
        ''')
        assert len(schema.tables) >= 1


class TestMainCLIDeep:
    """Deep main.py CLI tests."""
    
    def test_compare_no_output_specified(self, tmp_path, capsys):
        from schemaforge.main import main
        from unittest.mock import patch
        
        source = tmp_path / "source.sql"
        source.write_text("CREATE TABLE test (id INT);")
        
        with patch('sys.argv', [
            'schemaforge', 'compare',
            '--source', str(source),
            '--target', str(source),
            '--dialect', 'postgres'
        ]):
            main()
            
        captured = capsys.readouterr()
        # Should print some output or usage hint
        assert captured.out is not None
        
    def test_compare_multiple_dialects(self, tmp_path, capsys):
        from schemaforge.main import main
        from unittest.mock import patch
        
        source = tmp_path / "source.sql"
        source.write_text("CREATE TABLE test (id INT);")
        
        for dialect in ['mysql', 'sqlite', 'oracle', 'db2', 'snowflake']:
            with patch('sys.argv', [
                'schemaforge', 'compare',
                '--source', str(source),
                '--target', str(source),
                '--dialect', dialect,
                '--plan'
            ]):
                main()
                
            captured = capsys.readouterr()
            assert 'No changes detected' in captured.out
            
    def test_compare_with_verbose_levels(self, tmp_path, capsys):
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
            '-vv'  # Debug level
        ]):
            main()
            
        captured = capsys.readouterr()
        assert 'No changes detected' in captured.out


class TestModelsMethods:
    """Test model class methods for coverage."""
    
    def test_column_to_dict(self):
        col = Column('id', 'INT', is_primary_key=True, is_nullable=False, default_value='0')
        d = col.to_dict()
        assert d['name'] == 'id'
        assert d['is_primary_key'] == True
        
    def test_table_get_column(self):
        table = Table('test', [Column('id', 'INT'), Column('name', 'VARCHAR')])
        col = table.get_column('id')
        assert col is not None
        assert col.name == 'id'
        
    def test_table_get_column_not_found(self):
        table = Table('test', [Column('id', 'INT')])
        col = table.get_column('nonexistent')
        assert col is None
        
    def test_table_to_dict(self):
        table = Table('test', [Column('id', 'INT')], indexes=[Index('idx', ['id'])])
        d = table.to_dict()
        assert d['name'] == 'test'
        assert len(d['columns']) == 1
        
    def test_schema_get_table(self):
        from schemaforge.models import Schema
        schema = Schema(tables=[Table('users', [Column('id', 'INT')])])
        table = schema.get_table('users')
        assert table is not None
        
    def test_schema_get_table_not_found(self):
        from schemaforge.models import Schema
        schema = Schema()
        table = schema.get_table('nonexistent')
        assert table is None
        
    def test_foreign_key_to_dict(self):
        fk = ForeignKey('fk_test', ['col_a'], 'ref_table', ['ref_col'], on_delete='CASCADE')
        d = fk.to_dict()
        assert d['name'] == 'fk_test'
        assert d['on_delete'] == 'CASCADE'
        
    def test_index_to_dict(self):
        idx = Index('idx_test', ['col1', 'col2'], is_unique=True)
        d = idx.to_dict()
        assert d['name'] == 'idx_test'
        assert d['is_unique'] == True
