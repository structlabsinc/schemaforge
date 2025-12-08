import pytest
import os
import sys
import subprocess
from pathlib import Path

@pytest.fixture
def sqlite_complex_schema(tmp_path):
    schema_content = """
    CREATE TABLE t1 (
        id INTEGER PRIMARY KEY,
        data TEXT
    ) STRICT;

    CREATE TABLE t2 (
        id INT,
        val TEXT,
        PRIMARY KEY(id)
    ) WITHOUT ROWID;

    CREATE VIEW v1 AS SELECT * FROM t1;

    CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN
        INSERT INTO t2 VALUES (new.id, new.data);
    END;
    
    CREATE VIRTUAL TABLE ft USING fts5(data);
    """
    f = tmp_path / "sqlite_complex.sql"
    f.write_text(schema_content)
    return str(f)

def test_sqlite_parsing_coverage(sqlite_complex_schema):
    cmd = [
        sys.executable, 'schemaforge/main.py', 'compare',
        '--source', sqlite_complex_schema,
        '--target', sqlite_complex_schema,
        '--dialect', 'sqlite',
        '--plan'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
    assert result.returncode == 0
    assert "No changes detected" in result.stdout
