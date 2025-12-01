import unittest
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.comparator import Comparator
from schemaforge.models import Schema

class Test100Scenarios(unittest.TestCase):
    def setUp(self):
        self.parser = SnowflakeParser()
        self.comparator = Comparator()

    def _compare(self, source_sql, target_sql):
        source_schema = self.parser.parse(source_sql)
        target_schema = self.parser.parse(target_sql)
        return self.comparator.compare(source_schema, target_schema)

    def _assert_change(self, plan, expected_pattern):
        # Helper to check if the plan contains the expected change
        # We'll convert the plan to the string format used in main.py output for easy matching
        # Or just inspect the plan object directly which is more robust
        
        found = False
        
        # Check Tables
        for t in plan.new_tables:
            if f"Create Table: {t.name}" in expected_pattern or t.name in expected_pattern: found = True
        for t in plan.dropped_tables:
            if f"Drop Table: {t.name}" in expected_pattern or t.name in expected_pattern: found = True
            
        for diff in plan.modified_tables:
            if expected_pattern in str(diff): found = True
            for col in diff.added_columns:
                if f"Column: {col.name}" in expected_pattern or col.name in expected_pattern: found = True
            for col in diff.dropped_columns:
                if f"Drop Column: {col.name}" in expected_pattern or col.name in expected_pattern: found = True
            for old, new in diff.modified_columns:
                if f"Modify Column: {new.name}" in expected_pattern or new.name in expected_pattern: found = True
            for check in diff.added_checks:
                if "Constraint" in expected_pattern: found = True
            for p in diff.property_changes:
                if expected_pattern in p: found = True
                
        # Check Custom Objects
        for obj in plan.new_custom_objects:
            if f"Create {obj.obj_type}: {obj.name}" in expected_pattern or obj.name in expected_pattern: found = True
            if obj.obj_type in expected_pattern: found = True # e.g. "Create Dynamic Table"
            
        # Check Policies/Tags in modified tables
        for diff in plan.modified_tables:
             if "Policy" in expected_pattern and diff.property_changes: found = True
             if "Tag" in expected_pattern and diff.property_changes: found = True
             if "Comment" in expected_pattern and diff.property_changes: found = True

        return found

    def test_dynamic_table_regression(self):
        print("\n--- Test: Dynamic Table Regression ---")
        sql = "CREATE DYNAMIC TABLE DT_TEST TARGET_LAG = '1 minute' AS SELECT * FROM EVENTS_OMNIVERSE;"
        schema = self.parser.parse(sql)
        # Should be detected as CustomObject or Table
        found = False
        for obj in schema.custom_objects:
            if obj.obj_type == 'DYNAMIC TABLE' and obj.name == 'DT_TEST':
                found = True
        for tbl in schema.tables:
            if tbl.name == 'dt_test': # Unquoted -> lowercase
                found = True
        
        self.assertTrue(found, "Dynamic Table DT_TEST not detected")

    def test_constraints_composite_pk(self):
        print("\n--- Test: Composite PK ---")
        sql = "CREATE TABLE T (ID1 INT, ID2 INT, PRIMARY KEY (ID1, ID2));"
        schema = self.parser.parse(sql)
        t = schema.tables[0]
        pk_cols = [c.name for c in t.columns if c.is_primary_key]
        self.assertEqual(sorted(pk_cols), ['id1', 'id2'])

    def test_constraints_named(self):
        print("\n--- Test: Named Constraint ---")
        sql = "CREATE TABLE T (ID INT, CONSTRAINT pk_t PRIMARY KEY (ID));"
        schema = self.parser.parse(sql)
        t = schema.tables[0]
        self.assertTrue(t.columns[0].is_primary_key)

    def test_governance_unset_policy(self):
        print("\n--- Test: Unset Policy ---")
        # This requires comparing two states: one with policy, one without (UNSET)
        # But the parser just parses DDL.
        # If we parse "ALTER TABLE ... UNSET ...", it should result in a table WITHOUT the policy.
        # But we need a base table first.
        # The parser processes statements sequentially.
        sql = """
        CREATE TABLE T (C STRING);
        ALTER TABLE T MODIFY COLUMN C SET MASKING POLICY P;
        ALTER TABLE T MODIFY COLUMN C UNSET MASKING POLICY;
        """
        schema = self.parser.parse(sql)
        t = schema.tables[0]
        self.assertEqual(len(t.policies), 0, f"Policies should be empty, got: {t.policies}")

    def test_modern_features_iceberg(self):
        print("\n--- Test: Iceberg Table ---")
        sql = "CREATE ICEBERG TABLE IT_TEST (ID INT) EXTERNAL_VOLUME='v' CATALOG='c';"
        schema = self.parser.parse(sql)
        # Unquoted identifiers are lowercased by default
        self.assertTrue(any(t.name == 'it_test' for t in schema.tables), "Iceberg table not found")

    def test_modern_features_hybrid(self):
        print("\n--- Test: Hybrid Table ---")
        sql = "CREATE HYBRID TABLE HT_TEST (ID INT);"
        schema = self.parser.parse(sql)
        self.assertTrue(any(t.name == 'ht_test' for t in schema.tables), "Hybrid table not found")

    def test_modern_features_event(self):
        print("\n--- Test: Event Table ---")
        sql = "CREATE EVENT TABLE ET_TEST;"
        schema = self.parser.parse(sql)
        self.assertTrue(any(t.name == 'et_test' for t in schema.tables), "Event table not found")

    def test_constraints_named(self):
        print("\n--- Test: Named Constraint ---")
        sql = "CREATE TABLE T (ID INT, CONSTRAINT PK_TEST PRIMARY KEY (ID));"
        schema = self.parser.parse(sql)
        t = schema.tables[0]
        col = t.get_column('id')
        self.assertTrue(col.is_primary_key, "Named PK not detected")

    def test_governance_column_policy(self):
        print("\n--- Test: Column Masking Policy ---")
        sql = "CREATE TABLE T (ID INT, VAL STRING MASKING POLICY mask_val);"
        schema = self.parser.parse(sql)
        t = schema.tables[0]
        # Check if policy is captured in table.policies or column.comment or similar
        # Ideally, we want to know that 'mask_val' is applied.
        # Current parser puts it in table.policies if parsed via _process_create -> _extract_create_table
        # But _extract_create_table might not handle column-level policies yet.
        found = any('mask_val' in p for p in t.policies)
        self.assertTrue(found, "Column Masking Policy not detected")

    def test_governance_db_role(self):
        print("\n--- Test: Database Role ---")
        sql = "CREATE DATABASE ROLE DR_TEST;"
        schema = self.parser.parse(sql)
        found = any(o.obj_type == 'DATABASE ROLE' and o.name == 'CREATE DATABASE ROLE DR_TEST' for o in schema.custom_objects)
        # Note: Current implementation might capture the whole string as name if it falls into generic catch-all
        # Or it might not capture it at all if not explicitly handled.
        self.assertTrue(found, "Database Role not detected") 

    def test_modern_features_search_optimization(self):
        print("\n--- Test: Search Optimization ---")
        sql = "ALTER TABLE T ADD SEARCH OPTIMIZATION ON EQUALITY(ID);"
        schema = self.parser.parse(sql)
        # Should be captured as a CustomObject or property change?
        # Since it's an ALTER, it might be processed by _process_alter
        # If _process_alter doesn't handle it, it might be ignored.
        # Let's check if we have a custom object for it or if it modified the table.
        # Actually, ALTER usually modifies an existing table.
        # But if we parse just this SQL, there is no table to modify in the context unless we mock it.
        # However, the parser returns a Schema object.
        # If it's an ALTER on an unknown table, it might be skipped or added as CustomObject.
        found = any('SEARCH OPTIMIZATION' in o.name for o in schema.custom_objects)
        self.assertTrue(found, "Search Optimization statement not detected")

    def test_external_table(self):
        print("\n--- Test: External Table ---")
        sql = "CREATE EXTERNAL TABLE EXT_T (ID INT) LOCATION=@stage/path FILE_FORMAT=(TYPE=PARQUET);"
        schema = self.parser.parse(sql)
        t = schema.tables[0]
        self.assertEqual(t.name, 'ext_t')
        self.assertEqual(t.table_type, 'External Table') # Expect specific type


if __name__ == '__main__':
    unittest.main()
