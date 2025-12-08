import pytest
import os
import sys
import subprocess
from pathlib import Path

@pytest.fixture
def snowflake_complex_schema(tmp_path):
    schema_content = """
    -- Modern Table Types
    CREATE OR REPLACE TRANSIENT TABLE raw_events (
        id INT,
        raw_data VARIANT,
        ingested_at TIMESTAMP
    ) DATA_RETENTION_TIME_IN_DAYS = 0;

    CREATE OR REPLACE DYNAMIC TABLE user_summary
    TARGET_LAG = '1 minute'
    WAREHOUSE = compute_wh
    AS SELECT id, count(*) FROM raw_events GROUP BY id;

    CREATE TABLE clustered_events (
        event_date DATE,
        type STRING
    ) CLUSTER BY (event_date, type);

    -- Objects with properties
    CREATE OR REPLACE STAGE my_stage
    URL='s3://my-bucket/data'
    CREDENTIALS=(AWS_KEY_ID='x' AWS_SECRET_KEY='y');

    CREATE OR REPLACE PIPE my_pipe
    AUTO_INGEST = TRUE
    AS COPY INTO raw_events FROM @my_stage;

    CREATE OR REPLACE TASK daily_rollup
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 0 * * * UTC'
    AS CALL rollup_procedure();

    -- Security Policies
    CREATE OR REPLACE ROW ACCESS POLICY region_policy 
    AS (region VARCHAR) RETURNS BOOLEAN -> region = 'US';

    CREATE OR REPLACE MASKING POLICY email_mask 
    AS (val string) RETURNS string -> '***';

    ALTER TABLE raw_events MODIFY COLUMN raw_data SET MASKING POLICY email_mask;
    ALTER TABLE raw_events ADD ROW ACCESS POLICY region_policy ON (ingested_at);
    
    -- File Format
    CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ',';

    -- Deep Dive Objects
    CREATE OR REPLACE EXTERNAL TABLE ext_table (
        col1 VARCHAR AS (value:c1::varchar)
    ) LOCATION=@my_stage
    FILE_FORMAT = my_csv_format;

    CREATE OR REPLACE MATERIALIZED VIEW mv_user_summary 
    CLUSTER BY (id)
    AS SELECT id, count(*) as cnt FROM raw_events GROUP BY id;

    CREATE OR REPLACE STREAM event_stream ON TABLE raw_events;

    ALTER TABLE raw_events ALTER COLUMN id SET TAG (pii_tag = 'id_num');
    """
    f = tmp_path / "sf_complex.sql"
    f.write_text(schema_content)
    return str(f)

def test_snowflake_parsing_coverage(snowflake_complex_schema):
    """Verify Snowflawke advanced features parsing."""
    cmd = [
        sys.executable, 'schemaforge/main.py', 'compare',
        '--source', snowflake_complex_schema,
        '--target', snowflake_complex_schema,
        '--dialect', 'snowflake',
        '--plan'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
    
    assert result.returncode == 0, f"Command failed: {result.stderr}"
    assert "No changes detected" in result.stdout

def test_snowflake_diff_generation(snowflake_complex_schema, tmp_path):
    """Verify change detection for Snowflake objects."""
    target_content = Path(snowflake_complex_schema).read_text()
    
    # 1. Change CLUSTER BY
    target_content = target_content.replace(
        "CLUSTER BY (event_date, type)",
        "CLUSTER BY (type, event_date)" # Change order
    )
    
    # 2. Alter Pipeline (Replaced by Custom Object logic usually, or just new SQL)
    target_content = target_content.replace("AUTO_INGEST = TRUE", "AUTO_INGEST = FALSE")
    
    # 3. Drop Masking Policy
    target_content = target_content.replace(
        "ALTER TABLE raw_events MODIFY COLUMN raw_data SET MASKING POLICY email_mask;",
        "" 
    )
    target_content += "\nALTER TABLE raw_events MODIFY COLUMN raw_data UNSET MASKING POLICY;"

    target_file = tmp_path / "sf_target.sql"
    target_file.write_text(target_content)
    
    cmd = [
        sys.executable, 'schemaforge/main.py', 'compare',
        '--source', snowflake_complex_schema,
        '--target', str(target_file),
        '--dialect', 'snowflake',
        '--plan'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '.'})
    
    assert result.returncode == 0
    # Search Optimization or Cluster change might show up as Alter Table
    assert "Modify Table" in result.stdout or "Alter Table" in result.stdout or "ALTER" in result.stdout
    # Pipe change might show up as Custom Object diff
    assert "PIPE" in result.stdout or "pipe" in result.stdout.lower()
