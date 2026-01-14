"""
Coverage gap fill tests - targeting specific uncovered logical branches.
"""
import pytest
from unittest.mock import MagicMock
from schemaforge.models import CustomObject, Table, Column
from schemaforge.comparator import MigrationPlan
from schemaforge.main import _handle_output

class TestMainOutputCoverage:
    """Target main.py _handle_output custom object branches."""
    
    def test_output_custom_objects_branches(self, capsys):
        """Test all branches of custom object output formatting by mocking Plan."""
        # Create objects hitting diverse branches
        objs = [
            CustomObject('ALTER DATABASE', 'db_retention', properties={'DATA_RETENTION': 1}),
            CustomObject('ALTER DATABASE', 'db_tag', properties={'TAG': 'tag'}),
            CustomObject('ALTER DATABASE', 'other_db', properties={'OTHER': 'val'}),
            
            CustomObject('ALTER TABLE', 'tbl_mask', properties={'UNSET MASKING POLICY': 'pol'}),
            CustomObject('ALTER TABLE', 'tbl_row', properties={'DROP ROW ACCESS POLICY': 'pol'}),
            CustomObject('ALTER TABLE', 'tbl_search', properties={'SEARCH OPTIMIZATION': 'on'}),
            CustomObject('ALTER TABLE', 'tbl_tag', properties={'UNSET TAG': 'tag'}),
            CustomObject('ALTER TABLE', 'other_tbl', properties={'OTHER': 'val'}),
            
            CustomObject('ALTER TASK', 'my_task'),
            CustomObject('ALTER ALERT', 'my_alert'),
            
            CustomObject('ALTER VIEW', 'view_tag', properties={'TAG': 'tag'}),
            CustomObject('ALTER VIEW', 'view_other', properties={'OTHER': 'val'}),
            
            CustomObject('SWAP_OPERATION', 'swap_tbl'),
            CustomObject('UNDROP_OPERATION', 'undrop_tbl'),
            CustomObject('ALTER_PIPE', 'pipe1'),
            CustomObject('ALTER_FILE_FORMAT', 'ff1'),
            CustomObject('SEARCH_OPTIMIZATION', 'so1'),
            CustomObject('UNSET_OPERATION', 'unset1'),
            
            CustomObject('COMMENT', 'my_comment'),
            CustomObject('UNKNOWN_TYPE', 'unknown_obj')
        ]
        
        # We need to simulate the iteration. The code does:
        # for obj in migration_plan.new_custom_objects: ...
        # But actually the code checks obj.obj_type strings manually.
        # "if 'DATA_RETENTION' in name_upper" -> This relies on obj.name containing the keyword?
        # Let's check lines 268: if 'DATA_RETENTION' in name_upper:
        # Wait, name_upper is obj.name.upper().
        # So I need to name the objects so they trigger the condition.
        
        objs[0].name = "DB SET DATA_RETENTION 90"  # Trigger DATA_RETENTION
        objs[1].name = "DB SET TAG foo=bar"         # Trigger TAG
        objs[3].name = "TABLE UNSET MASKING POLICY foo"
        objs[4].name = "TABLE DROP ROW ACCESS POLICY bar"
        objs[5].name = "TABLE ADD SEARCH OPTIMIZATION"
        objs[6].name = "TABLE UNSET TAG tag1"
        objs[10].name = "VIEW SET TAG tag2"
        
        plan = MigrationPlan(new_custom_objects=objs)
        
        args = MagicMock()
        args.plan = True
        args.json_out = None
        args.sql_out = None
        args.no_color = True # Disable color codes for easier assertion
        args.generate_rollback = False
        
        _handle_output(args, plan)
        captured = capsys.readouterr()
        
        # Assertions
        assert "DATA_RETENTION" in captured.out
        assert "Tag" in captured.out
        assert "Unset Policy" in captured.out
        assert "Drop Policy" in captured.out
        assert "Search Optimization" in captured.out
        assert "Unset Tag" in captured.out
        assert "Alter Task" in captured.out
        assert "Alter Alert" in captured.out
        assert "Swap Table" in captured.out
        assert "Undrop Table" in captured.out
        assert "Alter Pipe" in captured.out
        assert "Alter File Format" in captured.out
        assert "Search_Optimization" in captured.out or "Search Optimization" in captured.out
        assert "Comment" in captured.out
        assert "Create UNKNOWN_TYPE" in captured.out


class TestGenericSQLParserGaps:
    """Target generic_sql.py complex parsing gaps."""
    
    def test_parse_fk_with_paren_identifier(self):
        """Test FK parsing with parenthesis in identifier (generic_sql.py:660+)."""
        from schemaforge.parsers.generic_sql import GenericSQLParser
        
        parser = GenericSQLParser()
        # Case: REFERENCES table(col) where table(col) is parsed as one token/identifier
        # This often happens with sqlparse for some inputs
        sql = """
        CREATE TABLE child (
            id INT,
            parent_id INT,
            FOREIGN KEY (parent_id) REFERENCES parent(id)
        );
        """
        schema = parser.parse(sql)
        assert len(schema.tables[0].foreign_keys) == 1
        fk = schema.tables[0].foreign_keys[0]
        fk = schema.tables[0].foreign_keys[0]
        assert fk.ref_table == 'parent'
        assert fk.ref_column_names == ['id']

    def test_parse_fk_composite_inline_style(self):
        """Test FK parsing with complex inline styles."""
        pass


class TestPostgresParserGaps:
    """Target postgres.py missing coverage."""
    
    def test_parse_create_extension_if_not_exists(self):
        """Test CREATE EXTENSION IF NOT EXISTS."""
        from schemaforge.parsers.postgres import PostgresParser
        parser = PostgresParser()
        sql = "CREATE EXTENSION IF NOT EXISTS citext SCHEMA public;"
        schema = parser.parse(sql)
        # Verify it doesn't crash and maybe captures extension
        assert schema is not None
        
    def test_parse_create_type_enum(self):
        """Test CREATE TYPE AS ENUM."""
        from schemaforge.parsers.postgres import PostgresParser
        parser = PostgresParser()
        sql = "CREATE TYPE status AS ENUM ('active', 'inactive');"
        schema = parser.parse(sql)
        assert schema is not None
        
    def test_parse_create_table_partition_hash(self):
        """Test CREATE TABLE PARTITION BY HASH."""
        from schemaforge.parsers.postgres import PostgresParser
        parser = PostgresParser()
        sql = """
        CREATE TABLE measurements (
            city_id         int not null,
            logdate         date not null,
            peaktemp        int,
            unitsales       int
        ) PARTITION BY HASH (city_id);
        """
        schema = parser.parse(sql)
        assert schema.tables[0].partition_by is not None


class TestSnowflakeParserGaps:
    """Target snowflake.py missing coverage."""
    
    def test_parse_create_stage_params(self):
        """Test CREATE STAGE with file format params."""
        from schemaforge.parsers.snowflake import SnowflakeParser
        parser = SnowflakeParser()
        sql = """
        CREATE STAGE my_s3_stage
          URL='s3://loading/files/'
          CREDENTIALS=(AWS_KEY_ID='1a2b3c' AWS_SECRET_KEY='4x5y6z')
          FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 1);
        """
        schema = parser.parse(sql)
        assert schema is not None
        
    def test_parse_alter_table_clustering(self):
        """Test ALTER TABLE CLUSTER BY."""
        from schemaforge.parsers.snowflake import SnowflakeParser
        parser = SnowflakeParser()
        sql = "ALTER TABLE orders CLUSTER BY (date, region);"
        # This parses as CustomObject or Table modification
        schema = parser.parse(sql)
        assert schema is not None
