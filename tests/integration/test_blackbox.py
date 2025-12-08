import pytest
import subprocess
import os
import shutil
import sys

# Path to the CLI entry point
DAC_CLI = [sys.executable, "schemaforge/main.py"]

class TestBlackboxCLI:
    @pytest.fixture(autouse=True)
    def setup_teardown(self, tmp_path):
        self.test_dir = tmp_path
        yield

    def run_dac(self, args):
        cmd = DAC_CLI + args
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True,
            env={**os.environ, 'PYTHONPATH': '.'}
        )
        return result

    def test_invalid_arguments(self):
        result = self.run_dac(["compare"])
        assert result.returncode != 0
        assert "error" in result.stderr.lower()

        result = self.run_dac(["compare", "--unknown-flag"])
        assert result.returncode != 0

    def test_file_not_found(self):
        result = self.run_dac(["compare", "non_existent_1.sql", "non_existent_2.sql", "--dialect", "sqlite"])
        assert result.returncode != 0
        err = result.stderr.lower()
        assert any(x in err for x in ["no such file", "does not exist", "error", "path not found"])

    def test_empty_input(self):
        f1 = self.test_dir / "empty1.sql"
        f2 = self.test_dir / "empty2.sql"
        f1.touch()
        f2.touch()
        
        result = self.run_dac(["compare", "--source", str(f1), "--target", str(f2), "--dialect", "sqlite", "--plan"])
        assert result.returncode == 0

    def test_invalid_sql(self):
        f1 = self.test_dir / "bad.sql"
        f1.write_text("THIS IS NOT SQL;")
        f2 = self.test_dir / "empty.sql"
        f2.touch()
        
        result = self.run_dac(["compare", "--source", str(f1), "--target", str(f2), "--dialect", "sqlite"])
        assert result.returncode == 0 # Should not crash

    def test_table_renaming(self):
        f1 = self.test_dir / "v1.sql"
        f1.write_text("CREATE TABLE users (id INT);")
        f2 = self.test_dir / "v2.sql"
        f2.write_text("CREATE TABLE people (id INT);")
            
        result = self.run_dac(["compare", "--source", str(f1), "--target", str(f2), "--dialect", "sqlite", "--plan"])
        assert result.returncode == 0
        assert "Drop Table: users" in result.stdout
        assert "Create Table: people" in result.stdout

    def test_column_type_change(self):
        f1 = self.test_dir / "v1.sql"
        f1.write_text("CREATE TABLE t1 (col1 INT);")
        f2 = self.test_dir / "v2.sql"
        f2.write_text("CREATE TABLE t1 (col1 VARCHAR(50));")
            
        result = self.run_dac(["compare", "--source", str(f1), "--target", str(f2), "--dialect", "sqlite", "--plan"])
        assert result.returncode == 0
        assert "Modify Column: col1" in result.stdout
        assert "INT -> VARCHAR(50)" in result.stdout

    def test_constraint_modifications(self):
        f1 = self.test_dir / "v1.sql"
        f1.write_text("CREATE TABLE t1 (id INT);")
        f2 = self.test_dir / "v2.sql"
        f2.write_text("CREATE TABLE t1 (id INT PRIMARY KEY);")
            
        result = self.run_dac(["compare", "--source", str(f1), "--target", str(f2), "--dialect", "sqlite", "--plan"])
        assert result.returncode == 0
        assert "Modify Table: t1" in result.stdout

    def test_nested_directories(self):
        src_dir = self.test_dir / "src"
        src_dir.mkdir()
        subdir = src_dir / "subdir"
        subdir.mkdir()
        
        (src_dir / "t1.sql").write_text("CREATE TABLE t1 (id INT);")
        (subdir / "t2.sql").write_text("CREATE TABLE t2 (id INT);")
            
        target_file = self.test_dir / "target.sql"
        target_file.write_text("CREATE TABLE t1 (id INT);\nCREATE TABLE t2 (id INT);")
            
        result = self.run_dac(["compare", "--source", str(src_dir), "--target", str(target_file), "--dialect", "sqlite", "--plan"])
        assert result.returncode == 0
        assert "No changes detected" in result.stdout

    def test_snowflake_mixed_case(self):
        f1 = self.test_dir / "v1.sql"
        f1.write_text("CREATE TABLE MyTable (Id INT);")
        f2 = self.test_dir / "v2.sql"
        f2.write_text("CREATE TABLE MYTABLE (ID INT);")
            
        result = self.run_dac(["compare", "--source", str(f1), "--target", str(f2), "--dialect", "snowflake", "--plan"])
        assert result.returncode == 0
        assert "No changes detected" in result.stdout
