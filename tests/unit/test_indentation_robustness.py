import unittest
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.parsers.postgres import PostgresParser
from schemaforge.parsers.mysql import MySQLParser
from schemaforge.parsers.sqlite import SQLiteParser
from schemaforge.parsers.oracle import OracleParser
from schemaforge.parsers.db2 import DB2Parser

class TestIndentationRobustness(unittest.TestCase):
    
    def _verify(self, parser, name, canonical, indented):
        print(f"Testing {parser.__class__.__name__} - {name}...")
        schema_c = parser.parse(canonical)
        schema_i = parser.parse(indented)
        
        def get_sql(schema, obj_name):
            for obj in schema.custom_objects:
                if obj.name.upper() == obj_name.upper():
                    return obj.properties['raw_sql']
            return None
            
        raw_c = get_sql(schema_c, name)
        raw_i = get_sql(schema_i, name)
        
        self.assertIsNotNone(raw_c, "Canonical object not found")
        self.assertIsNotNone(raw_i, "Indented object not found")
        
        # Normalize whitespace aggressively for comparison
        # (This ignores indentation differences inside function bodies, which is desired for schema diffing robustness)
        norm_c = " ".join(raw_c.split())
        norm_i = " ".join(raw_i.split())
        
        if norm_c != norm_i:
            print(f"FAIL: {name}")
            print(f"Canonical Normalized:\n{norm_c}")
            print(f"Indented Normalized:\n{norm_i}")
            
        self.assertEqual(norm_c, norm_i, f"Indentation caused false positive for {name}")

    def test_snowflake_view_indentation(self):
        canonical = "CREATE VIEW V_INDENT AS SELECT A, B FROM T WHERE A > 10;"
        indented = """
        CREATE VIEW V_INDENT AS 
            SELECT 
                    A, 
                    B 
            FROM 
                    T 
            WHERE 
                    A > 10;
        """
        self._verify(SnowflakeParser(), "V_INDENT", canonical, indented)

    def test_postgres_function_indentation(self):
        # Postgres functions often have bodies in strings ($$ ... $$)
        # Our normalizer strips comments and whitespace even inside $$ if it parses them as tokens?
        # sqlparse might treat $$ string content as a single token or generic.
        # If it's a single token, sqlparse.format might NOT format inside it unless we tell it to?
        # But wait, our normalize_sql collapses whitespace globally via regex!
        # So even inside string literals, whitespace is collapsed.
        # This is actually DESIRABLE for code comparison if we want to ignore indentation changes in function bodies.
        
        canonical = """
        CREATE FUNCTION add(a integer, b integer) RETURNS integer AS $$
        BEGIN
            RETURN a + b;
        END;
        $$ LANGUAGE plpgsql;
        """
        indented = """
        CREATE FUNCTION add(a integer, b integer) RETURNS integer AS $$
            BEGIN
                RETURN a + b;
            END;
        $$ LANGUAGE plpgsql;
        """
        self._verify(PostgresParser(), "add", canonical, indented)

    def test_mysql_procedure_indentation(self):
        canonical = """
        CREATE PROCEDURE GetUsers()
        BEGIN
            SELECT * FROM users;
        END
        """
        indented = """
        CREATE PROCEDURE GetUsers()
        BEGIN
            SELECT 
                * 
            FROM 
                users;
        END
        """
        self._verify(MySQLParser(), "GetUsers", canonical, indented)

    def test_sqlite_view_tabs(self):
        canonical = "CREATE VIEW V_TABS AS SELECT id FROM items;"
        indented = "CREATE\tVIEW\tV_TABS\tAS\tSELECT\t\tid\tFROM\titems;"
        self._verify(SQLiteParser(), "V_TABS", canonical, indented)

    def test_oracle_package_indentation(self):
        canonical = "CREATE PACKAGE BODY pkg AS PROCEDURE p1 IS BEGIN NULL; END; END;"
        indented = """
        CREATE PACKAGE BODY pkg AS 
            PROCEDURE p1 IS 
            BEGIN 
                NULL; 
            END; 
        END;
        """
        # Note: OracleParser currently looks for CREATE [OR REPLACE] KW ...
        # It supports PACKAGE.
        self._verify(OracleParser(), "pkg", canonical, indented)

    def test_db2_alias_indentation(self):
        canonical = "CREATE ALIAS a1 FOR t1;"
        indented = """
        CREATE 
            ALIAS 
                a1 
            FOR 
                t1;
        """
        self._verify(DB2Parser(), "a1", canonical, indented)

if __name__ == '__main__':
    unittest.main()
