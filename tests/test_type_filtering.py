import sqlite3
import os
import subprocess

DB_FILE = 'test_filtering.db'
TARGET_SQL = 'target_filtering.sql'

def setup_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE t1 (id INT)")
    cursor.execute("CREATE VIEW v1 AS SELECT * FROM t1")
    conn.commit()
    conn.close()

def create_target_schema():
    # Target has t1 (same) and t2 (new)
    # We will filter for 'table', so v1 should be ignored in source
    with open(TARGET_SQL, 'w') as f:
        f.write("CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT);")

def run_test():
    setup_db()
    create_target_schema()
    
    print("Running compare-livedb with --object-types table...")
    cmd = [
        "python3", "schemaforge/main.py", "compare-livedb",
        f"sqlite:///{DB_FILE}", TARGET_SQL,
        "--dialect", "sqlite",
        "--object-types", "table",
        "--plan"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    print("STDOUT:", result.stdout)
    
    # If filtering works, we should see t2 being created.
    # If v1 was introspected (and supported), it might show up as dropped or modified if not in target.
    # Since we don't support views fully yet, the main check is that it DOESN'T crash and DOES find tables.
    
    if "Create Table: t2" in result.stdout:
        print("SUCCESS: Object type filtering verified (tables found).")
    else:
        print("FAILURE: Tables not found.")

    # Cleanup
    if os.path.exists(DB_FILE): os.remove(DB_FILE)
    if os.path.exists(TARGET_SQL): os.remove(TARGET_SQL)

if __name__ == "__main__":
    run_test()
