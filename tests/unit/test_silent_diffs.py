import unittest
from schemaforge.models import Table, Column, Schema
from schemaforge.comparator import Comparator

class TestSilentDiffs(unittest.TestCase):
    def test_mysql_engine_diff(self):
        old_table = Table(name="users", columns=[Column("id", "INT")], engine="InnoDB")
        new_table = Table(name="users", columns=[Column("id", "INT")], engine="MyISAM")
        
        comp = Comparator()
        diff = comp._compare_tables(old_table, new_table)
        
        self.assertIsNotNone(diff)
        self.assertTrue(any("Engine" in p for p in diff.property_changes), "Should detect Engine change")

    def test_mysql_row_format_diff(self):
        old_table = Table(name="users", columns=[Column("id", "INT")], row_format="DEFAULT")
        new_table = Table(name="users", columns=[Column("id", "INT")], row_format="COMPRESSED")
        
        comp = Comparator()
        diff = comp._compare_tables(old_table, new_table)
        
        self.assertIsNotNone(diff)
        self.assertTrue(any("Row Format" in p for p in diff.property_changes), "Should detect Row Format change")

    def test_sqlite_strict_diff(self):
        old_table = Table(name="users", columns=[Column("id", "INT")], is_strict=False)
        new_table = Table(name="users", columns=[Column("id", "INT")], is_strict=True)
        
        comp = Comparator()
        diff = comp._compare_tables(old_table, new_table)
        
        self.assertIsNotNone(diff)
        self.assertTrue(any("Strict" in p for p in diff.property_changes), "Should detect STRICT mode change")

    def test_sqlite_without_rowid_diff(self):
        old_table = Table(name="users", columns=[Column("id", "INT")], without_rowid=False)
        new_table = Table(name="users", columns=[Column("id", "INT")], without_rowid=True)
        
        comp = Comparator()
        diff = comp._compare_tables(old_table, new_table)
        
        self.assertIsNotNone(diff)
        self.assertTrue(any("RowID" in p for p in diff.property_changes), "Should detect WITHOUT ROWID change")
