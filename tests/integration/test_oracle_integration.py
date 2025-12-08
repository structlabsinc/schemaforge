import pytest
import os
import sys
import subprocess
from pathlib import Path

@pytest.fixture
def oracle_complex_schema(tmp_path):
    schema_content = """
    CREATE TABLE employees (
        emp_id NUMBER PRIMARY KEY,
        name VARCHAR2(100),
        dept_id NUMBER
    ) TABLESPACE users;

    CREATE TABLE sales_data (
        sale_id NUMBER,
        sale_date DATE
    ) PARTITION BY RANGE (sale_date) (
        PARTITION p_2020 VALUES LESS THAN (TO_DATE('01-JAN-2021', 'DD-MON-YYYY')),
        PARTITION p_2021 VALUES LESS THAN (TO_DATE('01-JAN-2022', 'DD-MON-YYYY'))
    );

    CREATE OR REPLACE FUNCTION get_salary(p_emp_id NUMBER) RETURN NUMBER AS
    BEGIN
        RETURN 1000;
    END;
    /

    CREATE SEQUENCE emp_seq START WITH 1 INCREMENT BY 1;

    CREATE SYNONYM emps FOR employees;
    
    CREATE OR REPLACE PACKAGE emp_pkg AS
        PROCEDURE hire_emp(p_name VARCHAR2);
    END emp_pkg;
    /
    """
    f = tmp_path / "oracle_complex.sql"
    f.write_text(schema_content)
    return str(f)

def test_oracle_parsing_coverage(oracle_complex_schema):
    cmd = [
        sys.executable, 'schemaforge/main.py', 'compare',
        '--source', oracle_complex_schema,
        '--target', oracle_complex_schema,
        '--dialect', 'oracle',
        '--plan'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
    assert result.returncode == 0
    assert "No changes detected" in result.stdout

@pytest.mark.skipif(not os.path.exists('schemaforge/main.py'), reason="Main script not found")
@pytest.mark.xfail(reason="Oracle sequence increments and partition append diffs not currently detected by comparator")
def test_oracle_diff_detection(oracle_complex_schema, tmp_path):
    target_content = Path(oracle_complex_schema).read_text()
    
    # 1. Modify Partition
    # Append a new partition (simplified for raw SQL diff test)
    target_content += "\nCREATE TABLE new_part (id NUMBER);"
    
    # 2. Modify Sequence
    target_content = target_content.replace(
        "INCREMENT BY 1",
        "INCREMENT BY 5"
    )
    
    target_file = tmp_path / "oracle_target.sql"
    target_file.write_text(target_content)
    
    cmd = [
        sys.executable, 'schemaforge/main.py', 'compare',
        '--source', oracle_complex_schema,
        '--target', str(target_file),
        '--dialect', 'oracle',
        '--plan'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
    assert result.returncode == 0
    assert "INCREMENT BY 5" in result.stdout or "Sequence" in result.stdout or "Modify Sequence" in result.stdout
    assert "new_part" in result.stdout.lower() or "new_part" in result.stdout
