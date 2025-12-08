import pytest
import os
import sys
import subprocess
from pathlib import Path

@pytest.fixture
def mysql_complex_schema(tmp_path):
    schema_content = """
    CREATE TABLE articles (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(200),
        body TEXT,
        FULLTEXT (title, body)
    ) ENGINE=InnoDB;

    CREATE TABLE logs (
        log_id INT,
        created_at DATETIME
    ) PARTITION BY RANGE (YEAR(created_at)) (
        PARTITION p0 VALUES LESS THAN (2020),
        PARTITION p1 VALUES LESS THAN (2021)
    );

    CREATE VIEW active_articles AS SELECT * FROM articles WHERE id > 0;

    DELIMITER //
    CREATE PROCEDURE cleanup_logs()
    BEGIN
        DELETE FROM logs WHERE created_at < NOW();
    END //
    DELIMITER ;
    
    CREATE TRIGGER before_ins_log BEFORE INSERT ON logs FOR EACH ROW SET NEW.created_at = NOW();
    """
    f = tmp_path / "mysql_complex.sql"
    f.write_text(schema_content)
    return str(f)

def test_mysql_parsing_coverage(mysql_complex_schema):
    cmd = [
        sys.executable, 'schemaforge/main.py', 'compare',
        '--source', mysql_complex_schema,
        '--target', mysql_complex_schema,
        '--dialect', 'mysql',
        '--plan'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
    assert result.returncode == 0
    assert "No changes detected" in result.stdout

def test_mysql_diff_detection(mysql_complex_schema, tmp_path):
    target_content = Path(mysql_complex_schema).read_text()
    
    # Add new partition info via raw SQL append
    # (SchemaForge might not parse partition content deeply yet, but should handle raw SQL object changes)
    target_content += "\nCREATE TABLE new_table (id INT);"
    
    target_file = tmp_path / "mysql_target.sql"
    target_file.write_text(target_content)
    
    cmd = [
        sys.executable, 'schemaforge/main.py', 'compare',
        '--source', mysql_complex_schema,
        '--target', str(target_file),
        '--dialect', 'mysql',
        '--plan'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
    assert "Create Table" in result.stdout
    assert "new_table" in result.stdout.lower() or "new_table" in result.stdout
