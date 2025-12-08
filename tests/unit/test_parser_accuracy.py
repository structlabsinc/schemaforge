import unittest
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.models import CustomObject

class TestParserAccuracy(unittest.TestCase):
    def setUp(self):
        self.parser = SnowflakeParser()

    def test_snowflake_view_normalization(self):
        """Test that Snowflake views are normalized to ignore cosmetic differences."""
        
        # Original View
        sql_original = """
        CREATE VIEW v_test AS
        SELECT 
            col1, 
            col2 
        FROM my_table
        WHERE col1 > 100;
        """
        
        # Variant 1: Different Whitespace
        sql_whitespace = """
        CREATE   VIEW   v_test   AS
          SELECT 
              col1, 
              col2 
          FROM    my_table
          WHERE   col1 > 100;
        """
        
        # Variant 2: Different Case (Keywords)
        sql_case = """
        create view v_test as
        select 
            col1, 
            col2 
        from my_table
        where col1 > 100;
        """
        
        # Variant 3: Comments
        sql_comments = """
        CREATE VIEW v_test AS -- This is a view
        SELECT 
            col1, -- Column 1
            col2 
        FROM my_table
        WHERE col1 > 100; /* Filter */
        """
        
        # Parse all
        schema_orig = self.parser.parse(sql_original)
        schema_white = self.parser.parse(sql_whitespace)
        schema_case = self.parser.parse(sql_case)
        schema_comments = self.parser.parse(sql_comments)
        
        # Extract raw_sql property from the CustomObject (View)
        def get_raw_sql(schema):
            # Find the view
            for obj in schema.custom_objects:
                if obj.name.upper() == 'V_TEST':
                    return obj.properties['raw_sql']
            return None

        raw_orig = get_raw_sql(schema_orig)
        raw_white = get_raw_sql(schema_white)
        raw_case = get_raw_sql(schema_case)
        raw_comments = get_raw_sql(schema_comments)
        
        self.assertIsNotNone(raw_orig)
        
        # Assertions
        self.assertEqual(raw_orig, raw_white, "Whitespace difference failed normalization")
        self.assertEqual(raw_orig, raw_case, "Case difference failed normalization")
        self.assertEqual(raw_orig, raw_comments, "Comment difference failed normalization")

    def test_quoted_identifiers_preserved(self):
        """Test that quoted identifiers are NOT normalized to uppercase."""
        sql = 'CREATE VIEW v_quote AS SELECT "mixedCaseCol" FROM t;'
        schema = self.parser.parse(sql)
        
        raw_sql = schema.custom_objects[0].properties['raw_sql']
        self.assertIn('"mixedCaseCol"', raw_sql)
        self.assertNotIn('MIXEDCASECOL', raw_sql)

if __name__ == '__main__':
    unittest.main()
