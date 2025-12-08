import unittest
import random
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.parsers.postgres import PostgresParser
from schemaforge.parsers.mysql import MySQLParser
from schemaforge.parsers.sqlite import SQLiteParser
from schemaforge.parsers.oracle import OracleParser
from schemaforge.parsers.db2 import DB2Parser
from schemaforge.parsers.utils import normalize_sql

class SQLFuzzer:
    KEYWORDS = ["CREATE", "VIEW", "AS", "SELECT", "FROM", "WHERE", "AND", "OR", "JOIN", "ON", "GROUP", "BY", "ORDER", "HAVING", "LIMIT"]
    
    @staticmethod
    def fuzz(sql: str) -> str:
        """Applies random mutations to SQL string while preserving semantics."""
        # Simple tokenization that preserves punctuation as separate tokens would be better,
        # but for now let's just ensure we put whitespace back.
        # We'll use a regex to split but keep delimiters if we want to be fancy, 
        # but let's stick to split() and ensure we add a separator.
        
        # To be safe with punctuation like ',' or ';', we should probably pad them first
        # so split() treats them as tokens.
        for char in "(),;":
            sql = sql.replace(char, f" {char} ")
            
        tokens = sql.split()
        fuzzed_tokens = []
        
        for i, token in enumerate(tokens):
            # 1. Random Case Mutation for Keywords
            if token.upper() in SQLFuzzer.KEYWORDS:
                if random.random() < 0.5:
                    token = token.lower()
                elif random.random() < 0.5:
                    token = token.title()
            
            fuzzed_tokens.append(token)
            
            # 2. Add Separator (unless last token)
            if i < len(tokens) - 1:
                separator = " " # Default
                
                rand = random.random()
                if rand < 0.1:
                    separator = "   " # Multiple spaces
                elif rand < 0.2:
                    separator = "\n" # Newline
                elif rand < 0.3:
                    separator = "\n  " # Newline + indent
                elif rand < 0.35:
                    separator = f" /* {random.randint(1,99)} */ " # Block comment
                elif rand < 0.4:
                    separator = f" -- {random.randint(1,99)}\n" # Line comment
                
                fuzzed_tokens.append(separator)
            
        return "".join(fuzzed_tokens)

class TestParserFuzzing(unittest.TestCase):
    ITERATIONS = 50 # Per dialect -> 300 total tests
    
    def _run_fuzz_test(self, parser, canonical_sql, obj_name):
        print(f"\nTesting {parser.__class__.__name__} with {self.ITERATIONS} iterations...")
        
        # 1. Parse Canonical to get baseline normalized SQL
        schema_canon = parser.parse(canonical_sql)
        raw_canon = None
        for obj in schema_canon.custom_objects:
            if obj.name.upper() == obj_name.upper():
                raw_canon = obj.properties['raw_sql']
                break
        
        self.assertIsNotNone(raw_canon, "Failed to parse canonical SQL")
        
        # 2. Fuzz Loop
        for i in range(self.ITERATIONS):
            messy_sql = SQLFuzzer.fuzz(canonical_sql)
            
            # Ensure messy SQL is valid enough for sqlparse to find the object
            # Our fuzzer is simple, it might break syntax if it inserts comments inside strings, 
            # but we are splitting by space so it should be mostly safe for keywords.
            
            try:
                schema_messy = parser.parse(messy_sql)
                raw_messy = None
                for obj in schema_messy.custom_objects:
                    if obj.name.upper() == obj_name.upper():
                        raw_messy = obj.properties['raw_sql']
                        break
                
                if raw_messy is None:
                    print(f"FAIL: Parser failed to find object in iteration {i}")
                    print(f"Messy SQL:\n{messy_sql}")
                    self.fail("Parser failed to find object in messy SQL")
                
                if raw_canon != raw_messy:
                    print(f"FAIL: Normalization mismatch in iteration {i}")
                    print(f"Canonical Normalized:\n{raw_canon}")
                    print(f"Messy Normalized:\n{raw_messy}")
                    print(f"Messy Input:\n{messy_sql}")
                    self.fail("Normalization mismatch")
                    
            except Exception as e:
                print(f"CRASH: Parser crashed on iteration {i}")
                print(f"Messy SQL:\n{messy_sql}")
                raise e
                
        print(f"PASS: {self.ITERATIONS} iterations successful.")

    def test_snowflake_fuzz(self):
        sql = "CREATE VIEW V_SNOW AS SELECT COL1, COL2 FROM MY_TABLE WHERE COL1 > 100;"
        self._run_fuzz_test(SnowflakeParser(), sql, "V_SNOW")

    def test_postgres_fuzz(self):
        sql = "CREATE VIEW V_PG AS SELECT ID, NAME FROM USERS WHERE ACTIVE = TRUE;"
        self._run_fuzz_test(PostgresParser(), sql, "V_PG")

    def test_mysql_fuzz(self):
        sql = "CREATE VIEW V_MYSQL AS SELECT ID, EMAIL FROM CUSTOMERS ORDER BY ID DESC;"
        self._run_fuzz_test(MySQLParser(), sql, "V_MYSQL")

    def test_sqlite_fuzz(self):
        sql = "CREATE VIEW V_SQLITE AS SELECT * FROM ITEMS WHERE PRICE < 50.00;"
        self._run_fuzz_test(SQLiteParser(), sql, "V_SQLITE")

    def test_oracle_fuzz(self):
        sql = "CREATE PROCEDURE PROC_ORA AS BEGIN NULL; END;"
        self._run_fuzz_test(OracleParser(), sql, "PROC_ORA")

    def test_db2_fuzz(self):
        sql = "CREATE ALIAS A_DB2 FOR T_TARGET;"
        self._run_fuzz_test(DB2Parser(), sql, "A_DB2")

if __name__ == '__main__':
    unittest.main()
