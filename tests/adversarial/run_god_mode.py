import sys
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.comparator import Comparator

GOD_MODE_FILE = 'tests/adversarial/god_mode.sql'

def run_god_mode():
    print(f"Loading God Mode Schema from: {GOD_MODE_FILE}")
    with open(GOD_MODE_FILE, 'r') as f:
        content = f.read()
        
    # Split into OLD and NEW
    parts = content.split('-- ==== NEW_SCHEMA START ====')
    if len(parts) != 2:
        print("Error: Could not split OLD and NEW schemas.")
        sys.exit(1)
        
    old_sql = parts[0].replace('-- ==== OLD_SCHEMA START ====', '')
    new_sql = parts[1]
    
    print("Parsing OLD Schema...")
    parser = GenericSQLParser()
    old_schema = parser.parse(old_sql)
    print(f"OLD Tables: {[t.name for t in old_schema.tables]}")
    
    print("\nParsing NEW Schema...")
    new_schema = parser.parse(new_sql)
    print(f"NEW Tables: {[t.name for t in new_schema.tables]}")
    
    print("\nComparing Schemas...")
    comparator = Comparator()
    plan = comparator.compare(old_schema, new_schema)
    
    print("\nDiff Results:")
    for diff in plan.modified_tables:
        print(f"Modified Table: {diff.table_name}")
        for col in diff.added_columns:
            print(f"  + Add Column: {col.name}")
        for col in diff.dropped_columns:
            print(f"  - Drop Column: {col.name}")
        for old_c, new_c in diff.modified_columns:
            print(f"  ~ Modify Column: {new_c.name} ({old_c.data_type} -> {new_c.data_type})")
            
    for t in plan.new_tables:
        print(f"New Table: {t.name}")
        
    for t in plan.dropped_tables:
        print(f"Dropped Table: {t.name}")

if __name__ == "__main__":
    run_god_mode()
