import unittest
import os
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.generators.snowflake import SnowflakeGenerator
from schemaforge.comparator import Comparator
from schemaforge.models import Schema

class TestSnowflakeAdvanced(unittest.TestCase):
    def setUp(self):
        self.parser = SnowflakeParser()
        self.generator = SnowflakeGenerator()
        self.comparator = Comparator()

    def test_transient_table(self):
        sql = "CREATE TRANSIENT TABLE t1 (id INT);"
        schema = self.parser.parse(sql)
        self.assertEqual(len(schema.tables), 1)
        self.assertTrue(schema.tables[0].is_transient)
        
        gen_sql = self.generator.create_table_sql(schema.tables[0])
        self.assertIn("TRANSIENT TABLE", gen_sql)

    def test_cluster_by(self):
        sql = "CREATE TABLE t1 (id INT, date DATE) CLUSTER BY (date, id);"
        schema = self.parser.parse(sql)
        # Normalize to upper for comparison as Snowflake is case-insensitive
        cluster_cols = [c.upper() for c in schema.tables[0].cluster_by]
        self.assertEqual(cluster_cols, ['DATE', 'ID'])
        
        gen_sql = self.generator.create_table_sql(schema.tables[0])
        self.assertIn("CLUSTER BY (date, id)", gen_sql) # Generator uses original case currently

    def test_data_retention(self):
        sql = "CREATE TABLE t1 (id INT) DATA_RETENTION_TIME_IN_DAYS = 5;"
        schema = self.parser.parse(sql)
        self.assertEqual(schema.tables[0].retention_days, 5)
        
        gen_sql = self.generator.create_table_sql(schema.tables[0])
        self.assertIn("DATA_RETENTION_TIME_IN_DAYS = 5", gen_sql)

    def test_custom_objects_parsing(self):
        sql = """
        CREATE OR REPLACE STAGE my_stage URL='s3://bucket';
        CREATE PIPE my_pipe AS COPY INTO t1 FROM @my_stage;
        CREATE TASK my_task AS SELECT 1;
        CREATE STREAM my_stream ON TABLE t1;
        CREATE FILE FORMAT my_format TYPE = 'CSV';
        CREATE SEQUENCE my_seq;
        CREATE PROCEDURE my_proc() RETURNS STRING LANGUAGE JAVASCRIPT AS $$ return 'hi'; $$;
        """
        schema = self.parser.parse(sql)
        
        types = [obj.obj_type for obj in schema.custom_objects]
        names = [obj.name for obj in schema.custom_objects]
        
        self.assertIn('STAGE', types)
        self.assertIn('my_stage', names)
        self.assertIn('PIPE', types)
        self.assertIn('TASK', types)
        self.assertIn('STREAM', types)
        self.assertIn('FILE FORMAT', types)
        self.assertIn('SEQUENCE', types)
        self.assertIn('PROCEDURE', types)

    def test_idempotency(self):
        # Using a simplified version of God Mode for unit testing
        sql = """
        CREATE TRANSIENT TABLE t1 (id INT) COMMENT = 'test';
        CREATE SEQUENCE seq1;
        CREATE VIEW v1 AS SELECT * FROM t1;
        """
        schema1 = self.parser.parse(sql)
        
        # Generate SQL from schema1
        # We need to simulate a migration from empty to schema1 to get the full DDL
        empty_schema = Schema()
        plan = self.comparator.compare(empty_schema, schema1)
        generated_sql = self.generator.generate_migration(plan)
        
        print(f"DEBUG: Generated SQL:\n{generated_sql}")
        
        # Parse generated SQL
        schema2 = self.parser.parse(generated_sql)
        
        # Compare schema1 and schema2
        # Tables
        self.assertEqual(len(schema1.tables), len(schema2.tables))
        self.assertEqual(schema1.tables[0].name, schema2.tables[0].name)
        self.assertEqual(schema1.tables[0].is_transient, schema2.tables[0].is_transient)
        
        # Custom Objects
        self.assertEqual(len(schema1.custom_objects), len(schema2.custom_objects))
        obj_types1 = sorted([o.obj_type for o in schema1.custom_objects])
        obj_types2 = sorted([o.obj_type for o in schema2.custom_objects])
        self.assertEqual(obj_types1, obj_types2)

if __name__ == '__main__':
    unittest.main()
