import os
import sys
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

# Configuration
DIALECTS = {
    'snowflake': 'tests/god_mode/snowflake_god.sql',
    'postgres': 'tests/god_mode/postgres_god.sql',
    'mysql': 'tests/god_mode/mysql_god.sql',
    'sqlite': 'tests/god_mode/sqlite_god.sql',
    'oracle': 'tests/god_mode/oracle_god.sql',
    'db2': 'tests/god_mode/db2_god.sql'
}

GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def run_test(dialect, schema_file):
    print(f"[{dialect.upper()}] Starting God Mode Test...")
    
    if not os.path.exists(schema_file):
        print(f"{RED}[{dialect.upper()}] Schema file not found: {schema_file}{RESET}")
        return False

    # 1. Test Parsing & Idempotency (Source = Target)
    # Expect: No changes detected
    cmd = [
        sys.executable, 'schemaforge/main.py', 'compare',
        '--source', schema_file,
        '--target', schema_file,
        '--dialect', dialect,
        '--plan'
    ]
    
    try:
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
        duration = time.time() - start_time
        
        if result.returncode != 0:
            print(f"{RED}[{dialect.upper()}] CRASHED during parsing!{RESET}")
            print(result.stderr)
            return False
            
        if "No changes detected" not in result.stdout:
            print(f"{RED}[{dialect.upper()}] FAILED Idempotency! Detected phantom changes.{RESET}")
            print(result.stdout)
            return False
            
        print(f"{GREEN}[{dialect.upper()}] PASSED Parsing & Idempotency ({duration:.2f}s){RESET}")
        
    except Exception as e:
        print(f"{RED}[{dialect.upper()}] EXCEPTION: {e}{RESET}")
        return False

    # 2. Test Diff Detection (Modify Target)
    # Create a temporary modified file
    mod_file = f"{schema_file}.mod"
    with open(schema_file, 'r') as f:
        content = f.read()
    
    # Inject a change based on dialect
    if dialect == 'snowflake':
        content += "\nCREATE TABLE GOD_MODE_EXTRA (ID INT);"
    elif dialect == 'postgres':
        content += "\nCREATE TABLE god_mode_extra (id SERIAL);"
    elif dialect == 'mysql':
        content += "\nCREATE TABLE god_mode_extra (id INT);"
    elif dialect == 'sqlite':
        content += "\nCREATE TABLE god_mode_extra (id INTEGER);"
    elif dialect == 'oracle':
        content += "\nCREATE TABLE god_mode_extra (id NUMBER);"
    elif dialect == 'db2':
        content += "\nCREATE TABLE god_mode_extra (id INT);"
        
    with open(mod_file, 'w') as f:
        f.write(content)
        
    try:
        cmd = [
            sys.executable, 'schemaforge/main.py', 'compare',
            '--source', schema_file,
            '--target', mod_file,
            '--dialect', dialect,
            '--plan'
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
        
        if "Create Table: GOD_MODE_EXTRA" in result.stdout or "Create Table: god_mode_extra" in result.stdout:
             print(f"{GREEN}[{dialect.upper()}] PASSED Diff Detection{RESET}")
        else:
             print(f"{RED}[{dialect.upper()}] FAILED Diff Detection! Change not found.{RESET}")
             print(result.stdout)
             return False
             
    finally:
        if os.path.exists(mod_file):
            os.remove(mod_file)
            
    return True

def main():
    print(f"{YELLOW}=== STARTING GOD MODE TEST SUITE ==={RESET}")
    results = {}
    
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(run_test, d, f): d for d, f in DIALECTS.items()}
        for future in futures:
            dialect = futures[future]
            try:
                results[dialect] = future.result()
            except Exception as e:
                results[dialect] = False
                print(f"{RED}[{dialect.upper()}] Thread Error: {e}{RESET}")

    print(f"\n{YELLOW}=== SUMMARY ==={RESET}")
    all_passed = True
    for dialect, passed in results.items():
        status = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"{dialect.ljust(10)}: {status}")
        if not passed:
            all_passed = False
            
    sys.exit(0 if all_passed else 1)

if __name__ == '__main__':
    main()
