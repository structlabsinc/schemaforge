import pytest
import os
import sys
import subprocess
from pathlib import Path

@pytest.fixture
def db2_complex_schema(tmp_path):
    schema_content = """
    CREATE TABLE t1 (
        id INT NOT NULL,
        data VARCHAR(100),
        PERIOD FOR SYSTEM_TIME (sys_start, sys_end),
        sys_start TIMESTAMP(12) GENERATED ALWAYS AS ROW BEGIN,
        sys_end TIMESTAMP(12) GENERATED ALWAYS AS ROW END,
        trans_id TIMESTAMP(12) GENERATED ALWAYS AS TRANSACTION START_ID
    ) IN DATABASE db1.ts1
      AUDIT CHANGES
      CCSID UNICODE;

    CREATE TABLE t_physical (
        id INT
    ) USING STOGROUP sg1 PRIQTY 100 SECQTY 20;

    CREATE AUX TABLE clob_aux 
    IN DATABASE db1.aux_ts 
    STORES t1 COLUMN data;
    
    CREATE ALIAS t1_alias FOR t1;
    
    CREATE VIEW v1 AS SELECT id FROM t1;

    CREATE UNIQUE INDEX idx_t1 ON t1 (id) CLUSTER PARTITIONED;
    CREATE INDEX idx_phys ON t_physical (id) USING STOGROUP sg_idx PRIQTY 50;
    """
    f = tmp_path / "db2_complex.sql"
    f.write_text(schema_content)
    return str(f)

def test_db2_parsing_coverage(db2_complex_schema):
    cmd = [
        sys.executable, 'schemaforge/main.py', 'compare',
        '--source', db2_complex_schema,
        '--target', db2_complex_schema,
        '--dialect', 'db2',
        '--plan'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
    assert result.returncode == 0
    assert "No changes detected" in result.stdout
