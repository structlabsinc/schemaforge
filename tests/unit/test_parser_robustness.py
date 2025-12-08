import unittest
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.parsers.postgres import PostgresParser
from schemaforge.parsers.mysql import MySQLParser
from schemaforge.parsers.sqlite import SQLiteParser
from schemaforge.parsers.oracle import OracleParser
from schemaforge.parsers.db2 import DB2Parser

class TestParserRobustness(unittest.TestCase):
    
    def _verify_normalization(self, parser, canonical_sql, messy_sql, obj_name):
        """Helper to verify that messy SQL normalizes to the same as canonical SQL."""
        schema_canon = parser.parse(canonical_sql)
        schema_messy = parser.parse(messy_sql)
        
        def get_raw_sql(schema, name):
            for obj in schema.custom_objects:
                if obj.name.upper() == name.upper():
                    return obj.properties['raw_sql']
            return None

        raw_canon = get_raw_sql(schema_canon, obj_name)
        raw_messy = get_raw_sql(schema_messy, obj_name)
        
        self.assertIsNotNone(raw_canon, f"Canonical object {obj_name} not found")
        self.assertIsNotNone(raw_messy, f"Messy object {obj_name} not found")
        
        # Debug output if failure
        if raw_canon != raw_messy:
            print(f"\nFAIL: {parser.__class__.__name__}")
            print(f"Canonical:\n{raw_canon}")
            print(f"Messy:\n{raw_messy}")
            
        self.assertEqual(raw_canon, raw_messy, f"Normalization failed for {parser.__class__.__name__}")

    def test_snowflake_robustness(self):
        canonical = "CREATE VIEW V1 AS SELECT COL1 FROM T1;"
        messy = """
        create   view   v1   as 
        select 
            col1 -- comment
        from t1;
        """
        self._verify_normalization(SnowflakeParser(), canonical, messy, "V1")

    def test_postgres_robustness(self):
        canonical = "CREATE VIEW v_pg AS SELECT id FROM users;"
        messy = """
        CREATE OR REPLACE VIEW v_pg AS
        SELECT 
            id 
        FROM users; /* Block Comment */
        """
        # Note: CREATE OR REPLACE vs CREATE might be an issue if we normalize strictly.
        # But normalize_sql normalizes the string. 
        # If the input string has OR REPLACE, it will be kept.
        # So we should compare messy vs messy_normalized? 
        # No, the goal is that if I have "CREATE VIEW" in file A and "create view" in file B, they match.
        # If file A has "CREATE VIEW" and file B has "CREATE OR REPLACE VIEW", that IS a diff.
        # So we should test that "create view" matches "CREATE VIEW".
        
        messy_same_semantics = """
        create view v_pg as
        select id from users;
        """
        self._verify_normalization(PostgresParser(), canonical, messy_same_semantics, "v_pg")

    def test_mysql_robustness(self):
        canonical = "CREATE VIEW v_mysql AS SELECT name FROM users;"
        messy = """
        CREATE   VIEW   v_mysql   AS
        SELECT name FROM users;
        """
        self._verify_normalization(MySQLParser(), canonical, messy, "v_mysql")

    def test_sqlite_robustness(self):
        canonical = "CREATE VIEW v_sqlite AS SELECT * FROM t1;"
        messy = """
        create view v_sqlite as select * from t1;
        """
        self._verify_normalization(SQLiteParser(), canonical, messy, "v_sqlite")

    def test_oracle_robustness(self):
        canonical = "CREATE PROCEDURE proc1 AS BEGIN NULL; END;"
        messy = """
        CREATE   PROCEDURE   proc1   AS   BEGIN   NULL;   END;
        """
        self._verify_normalization(OracleParser(), canonical, messy, "proc1")

    def test_db2_robustness(self):
        canonical = "CREATE ALIAS a1 FOR t1;"
        messy = """
        create alias a1 for t1;
        """
        self._verify_normalization(DB2Parser(), canonical, messy, "a1")

if __name__ == '__main__':
    unittest.main()
