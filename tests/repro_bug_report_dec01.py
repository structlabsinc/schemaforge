import unittest
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.comparator import Comparator

class TestDec01Bugs(unittest.TestCase):
    def test_constraints(self):
        print("\n--- Test: Constraints (BUG-001, 002, 003) ---")
        sql1 = "CREATE TABLE T (ID INT, PAYER_ID INT, AMT INT);"
        sql2 = """
        CREATE TABLE T (
            ID INT, 
            PAYER_ID INT, 
            AMT INT,
            CONSTRAINT ck_amt CHECK (AMT > 0),
            CONSTRAINT uk_id UNIQUE (ID),
            CONSTRAINT fk_payer FOREIGN KEY (PAYER_ID) REFERENCES P(ID)
        );
        """
        
        parser = SnowflakeParser()
        s1 = parser.parse(sql1)
        s2 = parser.parse(sql2)
        
        comp = Comparator()
        plan = comp.compare(s1, s2)
        
        found_check = False
        found_unique = False
        found_fk = False
        
        for diff in plan.modified_tables:
            # We expect these to be in some list in TableDiff
            # Currently TableDiff has added_fks, added_indexes (for unique?), but no checks
            if diff.added_fks: found_fk = True
            
            # Check for Unique Index or Constraint
            for idx in diff.added_indexes:
                if idx.is_unique: found_unique = True
                
            # Check for Check Constraint (Not implemented yet)
            if hasattr(diff, 'added_checks') and diff.added_checks:
                found_check = True
                
        print(f"Found FK: {found_fk}")
        print(f"Found Unique: {found_unique}")
        print(f"Found Check: {found_check}")
        
        self.assertTrue(found_fk, "Foreign Key not detected")
        self.assertTrue(found_unique, "Unique Constraint not detected")
        self.assertTrue(found_check, "Check Constraint not detected")

    def test_governance_policies(self):
        print("\n--- Test: Governance Policies (BUG-004, 005, 006) ---")
        # Policy Creation
        sql_create = """
        CREATE MASKING POLICY MP_TEST AS (val string) returns string -> val;
        CREATE ROW ACCESS POLICY RAP_TEST AS (val string) returns boolean -> true;
        """
        parser = SnowflakeParser()
        s = parser.parse(sql_create)
        
        found_mp = any(o.obj_type == 'MASKING POLICY' for o in s.custom_objects)
        found_rap = any(o.obj_type == 'ROW ACCESS POLICY' for o in s.custom_objects)
        
        print(f"Custom Objects Found: {[o.obj_type for o in s.custom_objects]}")
        print(f"Found Masking Policy Object: {found_mp}")
        print(f"Found Row Access Policy Object: {found_rap}")
        
        self.assertTrue(found_mp, "Masking Policy object not parsed")
        self.assertTrue(found_rap, "Row Access Policy object not parsed")
        
        # Policy Application (Table Property)
        sql1 = "CREATE TABLE T (ID INT);"
        sql2 = "CREATE TABLE T (ID INT) WITH MASKING POLICY MP_TEST ON (ID) ROW ACCESS POLICY RAP_TEST ON (ID) TAG (cost_center='marketing');"
        # Note: Snowflake syntax for applying policies is often ALTER TABLE, but for CREATE TABLE it can be inline or separate.
        # The bug report mentions ALTER TABLE. Our parser handles full scripts.
        # Let's try parsing the ALTER statements as part of the script.
        
        sql_alter = """
        CREATE TABLE T (ID INT);
        ALTER TABLE T MODIFY COLUMN ID SET MASKING POLICY MP_TEST;
        ALTER TABLE T ADD ROW ACCESS POLICY RAP_TEST ON (ID);
        ALTER TABLE T SET TAG cost_center = 'marketing';
        """
        
        s_alter = parser.parse(sql_alter)
        t = s_alter.get_table('T')
        if not t:
             t = s_alter.get_table('t')
             
        if not t:
            print(f"DEBUG: Table T/t not found. Tables in schema: {[table.name for table in s_alter.tables]}")
            self.fail("Table T not found in schema")
            
        print(f"Table Properties: {t.to_dict()}")
        # Currently not implemented
        print(f"Table Properties: {t.to_dict()}")
        
        # This is hard to assert without implementation, but let's fail if empty
        self.assertTrue(any("POLICY" in str(p).upper() for p in t.to_dict().values()) or t.comment, "Policies not captured on table")

    def test_udf(self):
        print("\n--- Test: UDF (BUG-007) ---")
        sql = "CREATE FUNCTION FUNC_TEST() RETURNS INT AS '1';"
        parser = SnowflakeParser()
        s = parser.parse(sql)
        
        found = any(o.obj_type == 'FUNCTION' for o in s.custom_objects)
        print(f"Found Function: {found}")
        self.assertTrue(found, "UDF not parsed")

    def test_metadata(self):
        print("\n--- Test: Metadata (BUG-009) ---")
        sql1 = "CREATE TABLE T (ID INT);"
        sql2 = "CREATE TABLE T (ID INT COMMENT 'New Comment');"
        
        parser = SnowflakeParser()
        s1 = parser.parse(sql1)
        s2 = parser.parse(sql2)
        
        comp = Comparator()
        plan = comp.compare(s1, s2)
        
        found = False
        for diff in plan.modified_tables:
            for old, new in diff.modified_columns:
                print(f"Checking Col: {old.name}, Old Comment: {old.comment}, New Comment: {new.comment}")
                # Need to check comment change
                if hasattr(old, 'comment') and old.comment != new.comment:
                    found = True
        
        print(f"Found Column Comment Change: {found}")
        self.assertTrue(found, "Column comment change not detected")
        
    def test_collation(self):
        print("\n--- Test: Collation (BUG-010) ---")
        sql1 = "CREATE TABLE T (ID VARCHAR(100));"
        sql2 = "CREATE TABLE T (ID VARCHAR(100) COLLATE 'en-ci');"
        
        parser = SnowflakeParser()
        comparator = Comparator()
        
        s1 = parser.parse(sql1)
        s2 = parser.parse(sql2)
        
        plan = comparator.compare(s1, s2)
        found = False
        for diff in plan.modified_tables:
            for old, new in diff.modified_columns:
                print(f"Checking Col: {old.name}, Old Collation: {old.collation}, New Collation: {new.collation}")
                if hasattr(old, 'collation') and old.collation != new.collation:
                    found = True
        
        self.assertTrue(found, "Column collation change not detected")

if __name__ == '__main__':
    unittest.main()
