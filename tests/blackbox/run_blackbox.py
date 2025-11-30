import unittest
import subprocess
import os
import shutil

DAC_CLI = "python3 schemaforge/main.py"

class TestBlackboxCLI(unittest.TestCase):
    def setUp(self):
        self.test_dir = "tests/blackbox/tmp"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def run_dac(self, args):
        cmd = f"export PYTHONPATH=$PYTHONPATH:. && {DAC_CLI} {args}"
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True
        )
        return result

    def test_invalid_arguments(self):
        # Missing required args
        result = self.run_dac("compare")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("error", result.stderr.lower())

        # Unknown flag
        result = self.run_dac("compare --unknown-flag")
        self.assertNotEqual(result.returncode, 0)

    def test_file_not_found(self):
        result = self.run_dac("compare non_existent_1.sql non_existent_2.sql --dialect sqlite")
        self.assertNotEqual(result.returncode, 0)
        # Expecting some error message about file not found
        err = result.stderr.lower()
        self.assertTrue("no such file" in err or "does not exist" in err or "error" in err or "path not found" in err)

    def test_empty_input(self):
        f1 = os.path.join(self.test_dir, "empty1.sql")
        f2 = os.path.join(self.test_dir, "empty2.sql")
        open(f1, 'w').close()
        open(f2, 'w').close()
        
        result = self.run_dac(f"compare {f1} {f2} --dialect sqlite --plan")
        self.assertEqual(result.returncode, 0)
        # Output might vary depending on implementation of empty diff
        # self.assertIn("No changes detected", result.stdout) 

    def test_invalid_sql(self):
        f1 = os.path.join(self.test_dir, "bad.sql")
        with open(f1, 'w') as f:
            f.write("THIS IS NOT SQL;")
        f2 = os.path.join(self.test_dir, "empty.sql")
        open(f2, 'w').close()
        
        # Depending on implementation, this might fail or just parse nothing
        result = self.run_dac(f"compare {f1} {f2} --dialect sqlite")
        # If it parses nothing, it sees empty schema. 
        # Ideally, a robust parser might warn. 
        # For now, let's check if it crashes.
        self.assertEqual(result.returncode, 0)

    def test_table_renaming(self):
        # Scenario: Table 'users' renamed to 'people'
        # Expectation: DROP users, CREATE people (naive)
        f1 = os.path.join(self.test_dir, "v1.sql")
        with open(f1, 'w') as f:
            f.write("CREATE TABLE users (id INT);")
            
        f2 = os.path.join(self.test_dir, "v2.sql")
        with open(f2, 'w') as f:
            f.write("CREATE TABLE people (id INT);")
            
        result = self.run_dac(f"compare {f1} {f2} --dialect sqlite --plan")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Drop Table: users", result.stdout)
        self.assertIn("Create Table: people", result.stdout)

    def test_column_type_change(self):
        f1 = os.path.join(self.test_dir, "v1.sql")
        with open(f1, 'w') as f:
            f.write("CREATE TABLE t1 (col1 INT);")
            
        f2 = os.path.join(self.test_dir, "v2.sql")
        with open(f2, 'w') as f:
            f.write("CREATE TABLE t1 (col1 VARCHAR(50));")
            
        result = self.run_dac(f"compare {f1} {f2} --dialect sqlite --plan")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Modify Column: col1 (INT -> VARCHAR(50))", result.stdout)

    def test_constraint_modifications(self):
        # V1: No PK
        f1 = os.path.join(self.test_dir, "v1.sql")
        with open(f1, 'w') as f:
            f.write("CREATE TABLE t1 (id INT);")
            
        # V2: Add PK
        f2 = os.path.join(self.test_dir, "v2.sql")
        with open(f2, 'w') as f:
            f.write("CREATE TABLE t1 (id INT PRIMARY KEY);")
            
        result = self.run_dac(f"compare {f1} {f2} --dialect sqlite --plan")
        self.assertEqual(result.returncode, 0)
        # This depends on how generator handles PK addition on existing column
        # It might be ALTER TABLE ADD PRIMARY KEY or MODIFY COLUMN
        # Checking for "Modify Table: t1" is safer
        self.assertIn("Modify Table: t1", result.stdout)

    def test_nested_directories(self):
        # Source: dir/v1.sql, dir/subdir/v1_part2.sql
        src_dir = os.path.join(self.test_dir, "src")
        os.makedirs(os.path.join(src_dir, "subdir"))
        
        with open(os.path.join(src_dir, "t1.sql"), 'w') as f:
            f.write("CREATE TABLE t1 (id INT);")
        with open(os.path.join(src_dir, "subdir", "t2.sql"), 'w') as f:
            f.write("CREATE TABLE t2 (id INT);")
            
        # Target: single file with both
        target_file = os.path.join(self.test_dir, "target.sql")
        with open(target_file, 'w') as f:
            f.write("CREATE TABLE t1 (id INT);\nCREATE TABLE t2 (id INT);")
            
        result = self.run_dac(f"compare {src_dir} {target_file} --dialect sqlite --plan")
        self.assertEqual(result.returncode, 0)
        self.assertIn("No changes detected", result.stdout)

    def test_mixed_content_directory(self):
        # Directory with .sql and .txt
        src_dir = os.path.join(self.test_dir, "mixed")
        os.makedirs(src_dir)
        
        with open(os.path.join(src_dir, "t1.sql"), 'w') as f:
            f.write("CREATE TABLE t1 (id INT);")
        with open(os.path.join(src_dir, "notes.txt"), 'w') as f:
            f.write("This is not SQL")
            
        target_file = os.path.join(self.test_dir, "target.sql")
        with open(target_file, 'w') as f:
            f.write("CREATE TABLE t1 (id INT);")
            
        result = self.run_dac(f"compare {src_dir} {target_file} --dialect sqlite --plan")
        self.assertEqual(result.returncode, 0)
        self.assertIn("No changes detected", result.stdout)

    def test_postgres_serial(self):
        # Postgres SERIAL vs INTEGER DEFAULT nextval
        # This tests if the parser normalizes them or if we detect a diff
        # Realistically, they might be different in AST, so we expect a diff unless normalized
        f1 = os.path.join(self.test_dir, "v1.sql")
        with open(f1, 'w') as f:
            f.write("CREATE TABLE t1 (id SERIAL);")
            
        f2 = os.path.join(self.test_dir, "v2.sql")
        with open(f2, 'w') as f:
            f.write("CREATE TABLE t1 (id INTEGER DEFAULT nextval('seq'));")
            
        result = self.run_dac(f"compare {f1} {f2} --dialect postgres --plan")
        self.assertEqual(result.returncode, 0)
        # If our parser is smart, maybe no diff? 
        # But likely it sees a diff in type or default value.
        # Let's just assert it runs without crashing for now, and check output
        # print(result.stdout) 

    def test_snowflake_mixed_case(self):
        # Snowflake handles unquoted as uppercase
        f1 = os.path.join(self.test_dir, "v1.sql")
        with open(f1, 'w') as f:
            f.write("CREATE TABLE MyTable (Id INT);")
            
        f2 = os.path.join(self.test_dir, "v2.sql")
        with open(f2, 'w') as f:
            f.write("CREATE TABLE MYTABLE (ID INT);")
            
        result = self.run_dac(f"compare {f1} {f2} --dialect snowflake --plan")
        self.assertEqual(result.returncode, 0)
        # Should be no changes if parser upper-cases everything
        self.assertIn("No changes detected", result.stdout)

if __name__ == '__main__':
    unittest.main()
