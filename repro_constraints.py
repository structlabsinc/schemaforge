import sys
import os
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.comparator import Comparator
from schemaforge.models import Schema, Table, Column

def test_composite_pk():
    print("--- Testing Composite PK ---")
    sql1 = "CREATE TABLE T (ID1 INT, ID2 INT);"
    sql2 = "CREATE TABLE T (ID1 INT, ID2 INT, PRIMARY KEY (ID1, ID2));"
    parser = SnowflakeParser()
    s1 = parser.parse(sql1)
    s2 = parser.parse(sql2)
    
    comp = Comparator()
    diff = comp.compare(s1, s2)
    
    # Check if PK change is detected
    found = False
    for t in diff.modified_tables:
        for col_diff in t.modified_columns:
            # col_diff is (old_col, new_col)
            if col_diff[1].is_primary_key:
                found = True
                print(f"PK change detected for {col_diff[1].name}")
    
    if found:
        print("✅ Composite PK Change Detected")
    else:
        print("❌ Composite PK Change NOT Detected")

def test_identity():
    print("\n--- Testing Identity ---")
    sql1 = "CREATE TABLE T (ID INT);"
    sql2 = "CREATE TABLE T (ID INT IDENTITY(100, 2));"
    parser = SnowflakeParser()
    s2 = parser.parse(sql2)
    c = s2.tables[0].columns[0]
    print(f"Column: {c.name}, Identity: {c.is_identity}")
    # Check if start/step are captured (if supported)
    # Currently model only has is_identity boolean
    
    comp = Comparator()
    diff = comp.compare(parser.parse(sql1), s2)
    
    found = False
    for t in diff.modified_tables:
        for col_diff in t.modified_columns:
            if col_diff[1].is_identity:
                found = True
                print(f"Identity change detected: {col_diff[0].is_identity} -> {col_diff[1].is_identity}")
    
    if found:
        print("✅ Identity Change Detected")
    else:
        print("❌ Identity Change NOT Detected")

def test_named_pk():
    print("\n--- Testing Named PK ---")
    sql1 = "CREATE TABLE T (ID INT, PRIMARY KEY (ID));"
    sql2 = "CREATE TABLE T (ID INT, CONSTRAINT pk_test PRIMARY KEY (ID));"
    parser = SnowflakeParser()
    s1 = parser.parse(sql1)
    s2 = parser.parse(sql2)
    
    print(f"S1 PK Name: {s1.tables[0].primary_key_name}")
    print(f"S2 PK Name: {s2.tables[0].primary_key_name}")
    
    comp = Comparator()
    diff = comp.compare(s1, s2)
    
    found = False
    for t in diff.modified_tables:
        # Check property changes for table
        for prop in t.property_changes:
            if "Primary Key Name" in prop:
                found = True
                print(f"Property change: {prop}")
    
    if found:
        print("✅ Named PK Change Detected")
    else:
        print("❌ Named PK Change NOT Detected")

if __name__ == "__main__":
    test_composite_pk()
    test_identity()
    test_named_pk()
