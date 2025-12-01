#!/usr/bin/env python3
"""
Python port of the PowerShell 100-scenario test suite.
This allows us to run the same tests locally without PowerShell.
"""
import subprocess
import sys
import os
import re
from pathlib import Path

# Paths
SF_CMD = ["python3", "-m", "schemaforge.main"]
GOD_SOURCE = "tests/fixtures/god_level_schema.sql"
GOD_TARGET = "tests/fixtures/god_level_schema_Copy.sql"
HEALTH_SOURCE = "tests/fixtures/health_insurance.sql"
HEALTH_TARGET = "tests/fixtures/health_insurance_Copy.sql"

def reset_files():
    """Reset target files to match source files."""
    if os.path.exists(GOD_SOURCE):
        with open(GOD_SOURCE, 'r') as f:
            content = f.read()
        with open(GOD_TARGET, 'w') as f:
            f.write(content)
    
    if os.path.exists(HEALTH_SOURCE):
        with open(HEALTH_SOURCE, 'r') as f:
            content = f.read()
        with open(HEALTH_TARGET, 'w') as f:
            f.write(content)

def run_sf(source, target):
    """Run schemaforge compare command."""
    try:
        cmd = SF_CMD + ["compare", "--source", source, "--target", target, "--dialect", "snowflake", "--plan"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return result.stdout + result.stderr
    except Exception as e:
        return str(e)

def test_scenario(name, file_to_mod, mod_func, expected_pattern):
    """Test a single scenario."""
    print(f"Running Scenario: {name}... ", end='', flush=True)
    reset_files()
    
    try:
        # Read current content
        with open(file_to_mod, 'r') as f:
            content = f.read()
        
        # Apply modification
        new_content = mod_func(content)
        
        # Write modified content
        with open(file_to_mod, 'w') as f:
            f.write(new_content)
        
        # Determine source file
        source = GOD_SOURCE if file_to_mod == GOD_TARGET else HEALTH_SOURCE
        
        # Run comparison
        output = run_sf(source, file_to_mod)
        
        # Check for expected pattern (case-insensitive)
        if re.search(expected_pattern, output, re.IGNORECASE):
            print("✅ PASS")
            return True
        else:
            print("❌ FAIL")
            print(f"  Expected pattern '{expected_pattern}' not found.")
            print(f"  Output: {output[:200]}")
            return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

# Define all scenarios
scenarios = []

# Sample scenarios for testing (we'll add the full 100 later)
scenarios.append({
    'name': 'Dynamic Table',
    'file': GOD_TARGET,
    'mod': lambda c: c + "\nCREATE DYNAMIC TABLE DT_TEST TARGET_LAG = '1 minute' AS SELECT * FROM EVENTS_OMNIVERSE;",
    'expect': r'Create Dynamic Table: DT_TEST'
})

scenarios.append({
    'name': 'External Table',
    'file': GOD_TARGET,
    'mod': lambda c: c + "\nCREATE EXTERNAL TABLE EXT_TEST (ID INT) LOCATION=@stg_test FILE_FORMAT=ff_csv;",
    'expect': r'Create External Table: EXT_TEST'
})

scenarios.append({
    'name': 'Secure View',
    'file': GOD_TARGET,
    'mod': lambda c: c + "\nCREATE SECURE VIEW V_SECURE AS SELECT * FROM EVENTS_OMNIVERSE;",
    'expect': r'Create Secure View: V_SECURE'
})

scenarios.append({
    'name': 'Create Alert',
    'file': GOD_TARGET,
    'mod': lambda c: c + "\nCREATE ALERT ALERT_TEST WAREHOUSE = COMPUTE_WH SCHEDULE = '1 MINUTE' IF (EXISTS(SELECT * FROM EVENTS_OMNIVERSE)) THEN INSERT INTO LOGS VALUES ('Alert');",
    'expect': r'Create Alert: ALERT_TEST'
})

scenarios.append({
    'name': 'Create Database Role',
    'file': GOD_TARGET,
    'mod': lambda c: c + "\nCREATE DATABASE ROLE DB_ADMIN;",
    'expect': r'Create Database Role'
})

if __name__ == '__main__':
    # Check if fixture files exist
    if not os.path.exists(GOD_SOURCE):
        print(f"ERROR: {GOD_SOURCE} not found. Skipping tests.")
        print("This test requires the actual SQL fixture files from the user's environment.")
        sys.exit(1)
    
    print(f"Starting test with {len(scenarios)} scenarios...")
    passed = 0
    for s in scenarios:
        if test_scenario(s['name'], s['file'], s['mod'], s['expect']):
            passed += 1
    
    print(f"\nTest Complete. {passed}/{len(scenarios)} passed.")
    sys.exit(0 if passed == len(scenarios) else 1)
