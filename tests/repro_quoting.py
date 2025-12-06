
import unittest
from schemaforge.generators.postgres import PostgresGenerator
from schemaforge.models import Table, Column
from schemaforge.comparator import MigrationPlan, TableDiff

class TestQuoting(unittest.TestCase):
    def test_quoting_special_chars(self):
        # Scenario: Modify a table with special chars
        # Old Table
        t1 = Table(name="tbl_🚀_516")
        t1.columns.append(Column(name="col-1", data_type="INT"))

        # New Table
        t2 = Table(name="tbl_🚀_516")
        t2.columns.append(Column(name="col-1", data_type="VARCHAR(50)"))

        plan = MigrationPlan()
        plan.modified_tables.append(TableDiff(
            table_name="tbl_🚀_516",
            modified_columns=[(t1.columns[0], t2.columns[0])]
        ))

        gen = PostgresGenerator()

        gen = PostgresGenerator()
        sql = gen.generate_migration(plan)
        
        print(f"Generated SQL: {sql}")
        
        # Expectation: Identifiers should be quoted
        self.assertIn('"tbl_🚀_516"', sql)
        self.assertIn('"col-1"', sql)

if __name__ == '__main__':
    unittest.main()
