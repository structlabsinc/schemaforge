import sys
import traceback
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.models import Schema

CHAOS_FILE = 'tests/adversarial/chaos_snowflake.sql'
DIALECTS = ['mysql', 'postgresql', 'sqlite', 'oracle', 'db2', 'snowflake']

def run_chaos_test():
    print(f"Loading Chaos Schema from: {CHAOS_FILE}")
    with open(CHAOS_FILE, 'r') as f:
        sql_content = f.read()

    parser = GenericSQLParser()
    
    for dialect in DIALECTS:
        print(f"\nRunning Chaos Test for Dialect: {dialect.upper()}")
        try:
            # In a real scenario, we might configure the parser based on dialect
            # For now, GenericSQLParser is used for all.
            # We just want to ensure it doesn't CRASH.
            schema = parser.parse(sql_content)
            
            print(f"  Status: PARSED (Tables: {len(schema.tables)})")
            print(f"  Tables found: {[t.name for t in schema.tables]}")
            
        except Exception as e:
            print(f"  Status: CRASHED")
            print(f"  Error: {e}")
            # traceback.print_exc()

if __name__ == "__main__":
    run_chaos_test()
