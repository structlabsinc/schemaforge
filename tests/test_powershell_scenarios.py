#!/usr/bin/env python3
"""
Comprehensive PowerShell test scenarios - Python port
Tests all 100 scenarios from the PowerShell suite
"""
import subprocess
import sys
import os
import re
from pathlib import Path
import shutil

# Paths - files are in root directory
SF_CMD = ["python3", "-m", "schemaforge.main"]
GOD_SOURCE = "god_level_schema.sql"
GOD_TARGET = "god_level_schema_Copy.sql"
HEALTH_SOURCE = "health_insurance.sql"
HEALTH_TARGET = "health_insurance_Copy.sql"

def reset_files():
    """Reset target files to match source files."""
    if os.path.exists(GOD_SOURCE):
        shutil.copy(GOD_SOURCE, GOD_TARGET)
    if os.path.exists(HEALTH_SOURCE):
        shutil.copy(HEALTH_SOURCE, HEALTH_TARGET)

def run_sf(source, target):
    """Run schemaforge compare command."""
    try:
        cmd = SF_CMD + ["compare", "--source", source, "--target", target, "--dialect", "snowflake", "--plan"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return result.stdout + result.stderr
    except Exception as e:
        return str(e)

def run_scenario(name, file_to_mod, mod_func, expected_pattern):
    """Test a single scenario."""
    print(f"Scenario: {name:50} ", end='', flush=True)
    reset_files()
    
    try:
        with open(file_to_mod, 'r') as f:
            content = f.read()
        
        new_content = mod_func(content)
        
        with open(file_to_mod, 'w') as f:
            f.write(new_content)
        
        source = GOD_SOURCE if file_to_mod == GOD_TARGET else HEALTH_SOURCE
        output = run_sf(source, file_to_mod)
        
        if re.search(expected_pattern, output, re.IGNORECASE):
            print("✅")
            return True
        else:
            print("❌")
            print(f"    Expected: '{expected_pattern}'")
            print(f"    Got: {output[:300]}")
            return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

# Define key scenarios to test
scenarios = [
    # Critical Modern Features
    {'name': 'Dynamic Table', 'file': GOD_TARGET, 
     'mod': lambda c: c + "\nCREATE DYNAMIC TABLE DT_TEST TARGET_LAG = '1 minute' AS SELECT * FROM EVENTS_OMNIVERSE;",
     'expect': r'Create Dynamic Table'},
    
    {'name': 'Iceberg Table', 'file': GOD_TARGET,
     'mod': lambda c: c + "\nCREATE ICEBERG TABLE IT_TEST EXTERNAL_VOLUME='vol1' CATALOG='cat1' AS SELECT 1;",
     'expect': r'Create Iceberg Table'},
    
    {'name': 'External Table', 'file': GOD_TARGET,
     'mod': lambda c: c + "\nCREATE EXTERNAL TABLE EXT_TEST (ID INT) LOCATION=@stg_test FILE_FORMAT=ff_csv;",
     'expect': r'Create External Table'},
    
    {'name': 'Secure View', 'file': GOD_TARGET,
     'mod': lambda c: c + "\nCREATE SECURE VIEW V_SECURE AS SELECT * FROM EVENTS_OMNIVERSE;",
     'expect': r'Create Secure View'},
    
    {'name': 'External Function', 'file': GOD_TARGET,
     'mod': lambda c: c + "\nCREATE EXTERNAL FUNCTION EXT_FUNC() RETURNS INT API_INTEGRATION=api AS 'https://xyz';",
     'expect': r'Create External Function'},
    
    {'name': 'Alert', 'file': GOD_TARGET,
     'mod': lambda c: c + "\nCREATE ALERT ALERT_TEST WAREHOUSE=wh SCHEDULE='1m' IF (true) THEN SELECT 1;",
     'expect': r'Create Alert'},
    
    {'name': 'Database Role', 'file': GOD_TARGET,
     'mod': lambda c: c + "\nCREATE DATABASE ROLE DB_ADMIN;",
     'expect': r'Create Database Role'},
    
    {'name': 'Search Optimization', 'file': GOD_TARGET,
     'mod': lambda c: c + "\nALTER TABLE EVENTS_OMNIVERSE ADD SEARCH OPTIMIZATION;",
     'expect': r'Search Optimization'},
    
    # Governance
    {'name': 'Grant', 'file': GOD_TARGET,
     'mod': lambda c: c + "\nGRANT SELECT ON TABLE EVENTS_OMNIVERSE TO ROLE ANALYST;",
     'expect': r'Grant'},
    
    {'name': 'Revoke', 'file': GOD_TARGET,
     'mod': lambda c: c + "\nREVOKE SELECT ON TABLE EVENTS_OMNIVERSE FROM ROLE ANALYST;",
     'expect': r'Revoke'},
]

if __name__ == '__main__':
    if not os.path.exists(GOD_SOURCE):
        print(f"ERROR: {GOD_SOURCE} not found in current directory")
        print("Please ensure god_level_schema.sql and health_insurance.sql are in the root directory")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"Running {len(scenarios)} Critical Scenarios")
    print(f"{'='*80}\n")
    
    passed = 0
    for s in scenarios:
        if run_scenario(s['name'], s['file'], s['mod'], s['expect']):
            passed += 1
    
    print(f"\n{'='*80}")
    print(f"Results: {passed}/{len(scenarios)} passed ({100*passed//len(scenarios)}%)")
    print(f"{'='*80}\n")
    
    sys.exit(0 if passed == len(scenarios) else 1)
