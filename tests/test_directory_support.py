import os
import shutil
import subprocess

TEST_DIR = 'test_schema_dir'
TARGET_FILE = 'target_single.sql'

def setup():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    os.makedirs(os.path.join(TEST_DIR, 'tables'))
    os.makedirs(os.path.join(TEST_DIR, 'views'))
    
    # Create split schema files
    with open(os.path.join(TEST_DIR, 'tables', 'users.sql'), 'w') as f:
        f.write("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50));")
        
    with open(os.path.join(TEST_DIR, 'tables', 'orders.sql'), 'w') as f:
        f.write("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);")
        
    # Create target file (single file)
    with open(TARGET_FILE, 'w') as f:
        f.write("""
        CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50));
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);
        CREATE TABLE products (id INT PRIMARY KEY);
        """)

def run_test():
    setup()
    
    print("Running compare with directory source...")
    cmd = [
        "python3", "schemaforge/main.py", "compare",
        TEST_DIR, TARGET_FILE,
        "--dialect", "postgres",
        "--plan"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    
    # Expectation: users and orders match, products is new
    if "Create Table: products" in result.stdout and "Create Table: users" not in result.stdout:
        print("SUCCESS: Directory support verified.")
    else:
        print("FAILURE: Output did not match expectations.")

    # Cleanup
    if os.path.exists(TEST_DIR): shutil.rmtree(TEST_DIR)
    if os.path.exists(TARGET_FILE): os.remove(TARGET_FILE)

if __name__ == "__main__":
    run_test()
