import os
import time
import subprocess
import sqlalchemy
from sqlalchemy import create_engine, text

# Config
CONTAINER_NAME = "dac-test-postgres-sql"
DB_PORT = 5433 # Use different port to avoid conflict
DB_USER = "postgres"
DB_PASS = "password"
DB_NAME = "postgres"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@localhost:{DB_PORT}/{DB_NAME}"
TARGET_SQL = "target_schema_pg_sql.sql"
MIGRATION_FILE = "migration_pg.sql"

def run_command(cmd):
    subprocess.run(cmd, shell=True, check=True)

def setup_container():
    print("Starting Postgres container...")
    subprocess.run(f"docker rm -f {CONTAINER_NAME}", shell=True, stderr=subprocess.DEVNULL)
    run_command(f"docker run --name {CONTAINER_NAME} -e POSTGRES_PASSWORD={DB_PASS} -d -p {DB_PORT}:5432 postgres:alpine")
    
    print("Waiting for Postgres to be ready...")
    retries = 30
    while retries > 0:
        try:
            engine = create_engine(DB_URL)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Postgres is ready!")
            return
        except Exception:
            time.sleep(1)
            retries -= 1
    raise Exception("Postgres failed to start")

def setup_data():
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50))"))
        conn.commit()

def create_target_schema():
    sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY, 
        name VARCHAR(50), 
        email VARCHAR(100) -- New column
    );
    """
    with open(TARGET_SQL, 'w') as f:
        f.write(sql)

def run_test():
    try:
        setup_container()
        setup_data()
        create_target_schema()
        
        if os.path.exists(MIGRATION_FILE):
            os.remove(MIGRATION_FILE)
        
        print("Running compare-livedb against Postgres with --sql-out...")
        cmd = [
            "python3", "schemaforge/main.py", "compare-livedb",
            DB_URL, TARGET_SQL,
            "--dialect", "postgres",
            "--sql-out", MIGRATION_FILE
        ]
        
        subprocess.run(cmd, check=True)
        
        if os.path.exists(MIGRATION_FILE):
            with open(MIGRATION_FILE, 'r') as f:
                content = f.read()
            print("SUCCESS: Migration file generated.")
            print("--- Content ---")
            print(content)
            print("---------------")
            
            if "ALTER TABLE users ADD COLUMN email" in content:
                print("VERIFIED: Correct ALTER statement found.")
            else:
                print("FAILURE: Correct ALTER statement NOT found.")
        else:
            print("FAILURE: Migration file not found.")
            
    finally:
        print("Cleaning up...")
        subprocess.run(f"docker rm -f {CONTAINER_NAME}", shell=True, stderr=subprocess.DEVNULL)
        if os.path.exists(TARGET_SQL): os.remove(TARGET_SQL)
        if os.path.exists(MIGRATION_FILE): os.remove(MIGRATION_FILE)

if __name__ == "__main__":
    run_test()
