import unittest
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.comparator import Comparator

class TestMasterBugReport(unittest.TestCase):
    def setUp(self):
        self.parser = SnowflakeParser()
        self.comparator = Comparator()

    def test_constraints_composite_and_named(self):
        print("\n--- Test: Constraints (Composite & Named) ---")
        sql = """
        CREATE TABLE t_constraints (
            id1 INT,
            id2 INT,
            parent_id1 INT,
            parent_id2 INT,
            CONSTRAINT pk_composite PRIMARY KEY (id1, id2),
            CONSTRAINT uk_composite UNIQUE (parent_id1, parent_id2),
            CONSTRAINT fk_self FOREIGN KEY (parent_id1, parent_id2) REFERENCES t_constraints (id1, id2)
        );
        """
        schema = self.parser.parse(sql)
        table = schema.tables[0]
        
        # Check PK
        self.assertTrue(any(c.name == 'id1' and c.is_primary_key for c in table.columns), "id1 should be PK")
        self.assertTrue(any(c.name == 'id2' and c.is_primary_key for c in table.columns), "id2 should be PK")
        
        # Check Unique (Not fully implemented in model yet as separate object, but check if parsed)
        # Currently GenericSQLParser adds unique constraints to table.indexes or similar? 
        # Let's check table.indexes for now as that's where unique constraints often end up in simple parsers
        # OR check if we have a specific list for it.
        # Looking at previous edits, we added check_constraints, but maybe not unique_constraints list?
        # We'll check if it was parsed at all.
        
        # Check FK
        self.assertTrue(len(table.foreign_keys) > 0, "Foreign Key not found")
        fk = table.foreign_keys[0]
        self.assertEqual(fk.column_names, ['parent_id1', 'parent_id2'])
        self.assertEqual(fk.ref_table, 't_constraints')

    def test_governance_lifecycle(self):
        print("\n--- Test: Governance Lifecycle ---")
        sql = """
        CREATE TABLE t_gov (id INT, data STRING);
        ALTER TABLE t_gov MODIFY COLUMN data SET MASKING POLICY mp_data;
        ALTER TABLE t_gov MODIFY COLUMN data UNSET MASKING POLICY;
        ALTER TABLE t_gov SET TAG cost_center = 'marketing';
        ALTER TABLE t_gov UNSET TAG cost_center;
        ALTER TABLE t_gov ADD ROW ACCESS POLICY rap_1 ON (id);
        ALTER TABLE t_gov DROP ROW ACCESS POLICY rap_1;
        """
        schema = self.parser.parse(sql)
        table = schema.tables[0]
        
        # We expect the final state to reflect the operations
        # But since we parse sequentially, we might want to check if the operations were captured
        # For now, let's check if the parser didn't crash and if it captured the *final* state correctly
        # or if we can inspect the 'policies' and 'tags' lists/dicts.
        
        # If we parse DDL, we usually want the final state.
        # UNSET should remove from the list/dict.
        
        print(f"Policies: {table.policies}")
        print(f"Tags: {table.tags}")
        
        self.assertEqual(len(table.policies), 0, "Policies should be empty after UNSET/DROP")
        self.assertEqual(len(table.tags), 0, "Tags should be empty after UNSET")

    def test_modern_features_iceberg(self):
        print("\n--- Test: Modern Features (Iceberg) ---")
        sql = """
        CREATE ICEBERG TABLE t_iceberg (id INT) 
        CATALOG = 'my_catalog'
        EXTERNAL_VOLUME = 'my_vol'
        BASE_LOCATION = 'my_loc';
        """
        schema = self.parser.parse(sql)
        # Should be detected as a Table or CustomObject
        self.assertTrue(len(schema.tables) > 0 or len(schema.custom_objects) > 0, "Iceberg table not detected")

if __name__ == '__main__':
    unittest.main()
