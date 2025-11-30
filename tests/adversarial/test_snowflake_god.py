import sys
import os
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.generators.snowflake import SnowflakeGenerator
from schemaforge.comparator import Comparator

GOD_MODE_FILE = 'tests/adversarial/snowflake_god_mode.sql'

def run_test():
    print(f"Testing Snowflake God Mode with {GOD_MODE_FILE}...")
    print(f"Absolute path: {os.path.abspath(GOD_MODE_FILE)}")
    print(f"File size: {os.path.getsize(GOD_MODE_FILE)}")
    
    with open(GOD_MODE_FILE, 'r') as f:
        sql_content = f.read()
    print(f"Read content length: {len(sql_content)}")
        
    # 1. Parse
    parser = SnowflakeParser()
    schema = parser.parse(sql_content)
    
    print(f"Parsed {len(schema.tables)} tables: {[t.name for t in schema.tables]}")
    print(f"Parsed {len(schema.custom_objects)} custom objects: {[(o.obj_type, o.name) for o in schema.custom_objects]}")
    
    # Verify specific objects
    raw_events = schema.get_table('raw_events')
    if raw_events:
        print("✅ Table 'raw_events' found.")
        if raw_events.is_transient:
            print("✅ 'raw_events' is TRANSIENT.")
        else:
            print("❌ 'raw_events' is NOT TRANSIENT.")
            
        payload_col = raw_events.get_column('payload')
        if payload_col and payload_col.data_type == 'VARIANT':
             print("✅ Column 'payload' is VARIANT.")
        else:
             print(f"❌ Column 'payload' check failed. Type: {payload_col.data_type if payload_col else 'None'}")
    else:
        print("❌ Table 'raw_events' NOT found.")

    clustered_orders = schema.get_table('clustered_orders')
    if clustered_orders:
        print("✅ Table 'clustered_orders' found.")
        if clustered_orders.cluster_by == ['order_date', 'customer_id']:
            print("✅ 'clustered_orders' CLUSTER BY is correct.")
        else:
            print(f"❌ 'clustered_orders' CLUSTER BY failed. Got: {clustered_orders.cluster_by}")
    else:
        print("❌ Table 'clustered_orders' NOT found.")
        
    # Verify Custom Objects
    custom_types = [o.obj_type for o in schema.custom_objects]
    expected_types = ['FILE FORMAT', 'SEQUENCE', 'VIEW', 'STAGE', 'PIPE', 'STREAM', 'TASK', 'PROCEDURE']
    
    for t in expected_types:
        if t in custom_types:
            print(f"✅ Custom Object type '{t}' found.")
        else:
            print(f"❌ Custom Object type '{t}' NOT found.")

    # 2. Generate
    generator = SnowflakeGenerator()
    # Create a dummy plan to generate SQL for everything as "New"
    from schemaforge.comparator import MigrationPlan
    plan = MigrationPlan()
    plan.new_tables = schema.tables
    plan.new_custom_objects = schema.custom_objects
    
    generated_sql = generator.generate_migration(plan)
    
    print("\n--- Generated SQL Preview (First 500 chars) ---")
    print(generated_sql[:500])
    print("-----------------------------------------------\n")
    
    # 3. Idempotency Check (Parse Generated SQL)
    schema2 = parser.parse(generated_sql)
    
    comparator = Comparator()
    diff = comparator.compare(schema, schema2)
    
    if not diff.new_tables and not diff.dropped_tables and not diff.modified_tables and \
       not diff.new_custom_objects and not diff.dropped_custom_objects and not diff.modified_custom_objects:
        print("✅ Idempotency Verified: Generated SQL produces identical schema.")
    else:
        print("❌ Idempotency Failed. Differences found:")
        print(diff.to_dict())

if __name__ == "__main__":
    run_test()
