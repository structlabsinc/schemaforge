#!/usr/bin/env python3
"""
Comprehensive Postgres Advanced Feature Test Suite
Tests advanced features specifically for PostgreSQL to ensure parity with Snowflake robustness.
"""
import subprocess
import sys
import os
import re
import shutil

SF_CMD = ["python3", "-m", "schemaforge.main"]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.join(BASE_DIR, "fixtures")

GOD_SOURCE = os.path.join(FIXTURES_DIR, "postgres_god_schema.sql")
GOD_TARGET = "postgres_god_schema_Copy.sql"

def reset_files():
    if os.path.exists(GOD_SOURCE):
        shutil.copy(GOD_SOURCE, GOD_TARGET)

def run_sf(source, target):
    try:
        cmd = SF_CMD + ["compare", "--source", source, "--target", target, "--dialect", "postgres", "--plan"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return result.stdout + result.stderr
    except Exception as e:
        return str(e)

def run_scenario(num, name, mod_func, expected_pattern):
    print(f"{num:3d}. {name:55} ", end='', flush=True)
    reset_files()
    
    try:
        with open(GOD_TARGET, 'r') as f:
            content = f.read()
        
        new_content = mod_func(content)
        
        with open(GOD_TARGET, 'w') as f:
            f.write(new_content)
        
        output = run_sf(GOD_SOURCE, GOD_TARGET)
        
        if re.search(expected_pattern, output, re.IGNORECASE | re.DOTALL):
            print("✅")
            return True
        else:
            print("❌")
            if os.getenv('VERBOSE'):
                print(f"    Expected: '{expected_pattern}'")
                print(f"    Got: {output[:300]}...")
            return False
    except Exception as e:
        print(f"💥 {str(e)[:50]}")
        return False

scenarios = []

# -----------------------------------------------------------------------------
# PARTITIONING
# -----------------------------------------------------------------------------
scenarios.append(('Add Partition',
    lambda c: c + "\nCREATE TABLE titan_db_core.events_y2025 PARTITION OF titan_db_core.events_omniverse FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');",
    r'Create Table.*events_y2025'))

scenarios.append(('Detach Partition',
    lambda c: c.replace("CREATE TABLE titan_db_core.events_y2024 PARTITION OF titan_db_core.events_omniverse", 
                        "CREATE TABLE titan_db_core.events_y2024"),
    r"(Detach Partition|Drop Table.*events_y2024|Create Table.*events_y2024|Partition Of: .* -> None)")) 
    # Logic might differ (drop/create vs detach). Expecting Create Table for now if parser sees it as new standalone table.

scenarios.append(('Modify Partition Key (Unsafe)',
    lambda c: c.replace("PARTITION BY RANGE (timestamp)", "PARTITION BY HASH (event_uuid)"),
    r"(Modify Table.*PARTITION BY|Partition: .* -> .*)"))

# -----------------------------------------------------------------------------
# INHERITANCE
# -----------------------------------------------------------------------------
scenarios.append(('Add Inherits',
    lambda c: c + "\nCREATE TABLE titan_db_core.legacy_entity (id int) INHERITS (titan_db_core.base_entity);",
    r"(Create Table.*legacy_entity|Inherits: None -> .*)"))

scenarios.append(('Remove Inherits',
    lambda c: c.replace("INHERITS (titan_db_core.base_entity)", ""),
    r"(Modify Table.*NO INHERITS|Drop Table|Inherits: .* -> None)"))

# -----------------------------------------------------------------------------
# ADVANCED COLUMNS (IDENTITY, GENERATED)
# -----------------------------------------------------------------------------
scenarios.append(('Add Identity Column',
    lambda c: c.replace("entity_id SERIAL PRIMARY KEY", "entity_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"),
    r'Modify Column.*entity_id'))

scenarios.append(('Modify Identity Options',
    lambda c: c.replace("START WITH 1000 INCREMENT BY 5", "START WITH 5000 INCREMENT BY 10"),
    r'Modify Column.*account_id'))

scenarios.append(('Add Generated Column',
    lambda c: c.replace("cost_center TEXT,", "cost_center TEXT,\n    upper_cc TEXT GENERATED ALWAYS AS (upper(cost_center)) STORED,"),
    r'Column.*upper_cc'))

# -----------------------------------------------------------------------------
# CONSTRAINTS & INDEXES
# -----------------------------------------------------------------------------
scenarios.append(('Add Exclusion Constraint',
    lambda c: c.replace("valid_range TSTRANGE,", "valid_range TSTRANGE,\n    EXCLUDE USING GIST (account_id WITH =, valid_range WITH &&),"),
    r'Constraint'))

scenarios.append(('Add GIN Index',
    lambda c: c + "\nCREATE INDEX idx_attr_gin ON titan_db_core.dim_hyper_entity USING GIN (attributes);",
    r"(Create Index.*idx_attr_gin|Add Index: idx_attr_gin)"))

scenarios.append(('Add JSONB Path Index',
    lambda c: c + "\nCREATE INDEX idx_json_path ON titan_db_core.events_omniverse USING GIN (payload jsonb_path_ops);",
    r"(Create Index.*idx_json_path|Add Index: idx_json_path)"))

scenarios.append(('Add Partial Index',
    lambda c: c + "\nCREATE INDEX idx_partial_active ON titan_db_core.events_omniverse (event_uuid) WHERE session_id IS NOT NULL;",
    r"(Create Index.*idx_partial_active|Add Index: idx_partial_active)"))

# -----------------------------------------------------------------------------
# SECURITY (RLS & POLICIES)
# -----------------------------------------------------------------------------
scenarios.append(('Enable RLS',
    lambda c: c + "\nALTER TABLE titan_db_core.fact_ledger_atomic ENABLE ROW LEVEL SECURITY;",
    r"(Enable RLS|Row Security: False -> True)"))

scenarios.append(('Create Policy',
    lambda c: c + "\nCREATE POLICY ledger_policy ON titan_db_core.fact_ledger_atomic USING (account_id > 1000);",
    r'Create Policy.*ledger_policy'))

scenarios.append(('Drop Policy',
    lambda c: c.replace("CREATE POLICY tenant_isolation_policy", "-- CREATE POLICY tenant_isolation_policy"),
    r'Drop Policy.*tenant_isolation_policy'))

# -----------------------------------------------------------------------------
# PROCEDURAL OBJECTS
# -----------------------------------------------------------------------------
scenarios.append(('Create Domain',
    lambda c: c + "\nCREATE DOMAIN titan_db_core.email AS TEXT CHECK (VALUE ~* '^.+@.+$');",
    r'Create Domain.*email'))

scenarios.append(('Create Type (Enum)',
    lambda c: c + "\nCREATE TYPE titan_db_core.status AS ENUM ('active', 'inactive', 'banned');",
    r'Create Type.*status'))

scenarios.append(('Create Trigger',
    lambda c: c + "\nCREATE TRIGGER new_trig AFTER UPDATE ON titan_db_core.inventory FOR EACH ROW EXECUTE FUNCTION titan_db_core.audit_trigger_func();",
    r'Create Trigger.*new_trig'))


if __name__ == '__main__':
    if not os.path.exists(GOD_SOURCE):
        print(f"ERROR: {GOD_SOURCE} not found")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"POSTGRES ADVANCED FEATURE TEST SUITE")
    print(f"{'='*80}\n")
    
    passed = 0
    failed_scenarios = []
    
    for i, (name, mod, expect) in enumerate(scenarios, 1):
        if run_scenario(i, name, mod, expect):
            passed += 1
        else:
            failed_scenarios.append((i, name))
    
    print(f"\n{'='*80}")
    print(f"RESULTS: {passed}/{len(scenarios)} passed ({100*passed//len(scenarios)}%)")
    print(f"{'='*80}\n")
    
    sys.exit(0 if passed == len(scenarios) else 1)
