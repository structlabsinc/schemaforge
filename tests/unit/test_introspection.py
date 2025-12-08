import sqlite3
import os
import subprocess

DB_FILE = 'test_introspection.db'
TARGET_SQL = 'target_schema.sql'

def setup_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create a table in the "live" DB
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))")
    cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, price DECIMAL(10,2))")
    
    conn.commit()
    conn.close()

def create_target_schema():
    # Target schema has an extra column in users and a new table
    sql = """
    CREATE TABLE users (
        id INTEGER PRIMARY KEY, 
        name VARCHAR(50), 
        email VARCHAR(100),
        status VARCHAR(20) -- New column
    );
    
    CREATE TABLE products (
        id INTEGER PRIMARY KEY, 
        price DECIMAL(10,2)
    );
    
    CREATE TABLE orders ( -- New table
        id INTEGER PRIMARY KEY,
        user_id INTEGER
    );
    """
    with open(TARGET_SQL, 'w') as f:
        f.write(sql)

def run_test():
    setup_db()
    create_target_schema()
    
    print("Running compare-livedb...")
    # Use sqlite:///test_introspection.db as URL
    cmd = [
        "python3", "schemaforge/main.py", "compare-livedb",
        f"sqlite:///{DB_FILE}", TARGET_SQL,
        "--dialect", "sqlite",
        "--plan"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    
    if "Create Table: orders" in result.stdout and "Add Column: status" in result.stdout:
        print("SUCCESS: Introspection and comparison worked!")
    else:
        print("FAILURE: Output did not match expectations.")

    # Cleanup
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    if os.path.exists(TARGET_SQL):
        os.remove(TARGET_SQL)

if __name__ == "__main__":
    run_test()
