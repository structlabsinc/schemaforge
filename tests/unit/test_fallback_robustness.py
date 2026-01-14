import unittest
from schemaforge.parsers.sqlglot_adapter import SqlglotParser
from schemaforge.models import Schema, Table

class TestFallbackRobustness(unittest.TestCase):
    def setUp(self):
        self.parser = SqlglotParser(dialect="mysql") # Dialect doesn't matter for fallback trigger usually, but we'll use generic

    def test_db2_messy_create_table(self):
        # DB2 example that might fail strict parsing (simulated with random syntax that forces fallback but has columns)
        # We rely on the fact that we force fallback for things sqlglot doesn't like.
        # But to be sure, we can mock the fallback path or just use a very weird syntax.
        # Actually, let's just assert on the _parse_fallback_columns method directly first if we make it public/internal,
        # OR we can pass a really broken string to .parse() and see if it picks up columns via fallback.
        # Given we want to test the END-TO-END fallback enhancement, let's try a string that we know triggers fallback.
        
        # A string with "CREATE TABLE" but some garbage that sqlglot hates
        sql = """
        CREATE TABLE complicated_db2 (
            id INT NOT NULL,
            name VARCHAR(100),
            salary DECIMAL(10, 2)
        ) IN DATABASE db1.ts1 AUDIT CHANGES;
        """
        # Sqlglot might actually parse this if it supports DB2 well enough. 
        # Let's try to verify if it falls back. If it parses correctly, good for sqlglot, 
        # but we want to test the fallback logic. 
        
        # We can simulate fallback by passing a string that is definitely invalid for the declared dialect but has CREATE TABLE.
        # e.g. using MySQL dialect but DB2 syntax.
        
        self.parser.dialect = "mysql" 
        schema = self.parser.parse(sql)
        
        table = schema.get_table("complicated_db2")
        self.assertIsNotNone(table)
        # CURRENTLY: This fails because we don't parse columns in fallback
        # GOAL: This should pass
        self.assertEqual(len(table.columns), 3) 
        self.assertEqual(table.columns[0].name, "id")
        self.assertEqual(table.columns[2].data_type, "DECIMAL(10, 2)") # verifying commas in parens match

    def test_mssql_messy_create_table(self):
        # MSSQL with some weird hints or legacy syntax
        sql = """
        CREATE TABLE legacy_mssql (
            [pk_col] INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
            [data_col] NVARCHAR(MAX)
        ) ON [PRIMARY] WITH (DATA_COMPRESSION = PAGE);
        """
        self.parser.dialect = "mysql" # Force likely fallback
        schema = self.parser.parse(sql)
        
        table = schema.get_table("legacy_mssql")
        self.assertIsNotNone(table)
        
        # CURRENTLY: Fails
        self.assertEqual(len(table.columns), 2)
        self.assertEqual(table.columns[0].name, "pk_col")
        self.assertEqual(table.columns[1].name, "data_col")
