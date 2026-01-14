-- Snowflake Exhaustive Coverage Test
CREATE OR REPLACE TRANSIENT TABLE raw_data (
    id INT,
    payload VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) DATA_RETENTION_TIME_IN_DAYS = 1;

CREATE DYNAMIC TABLE filtered_data
  TARGET_LAG = '1 minute'
  WAREHOUSE = COMPUTE_WH
  AS SELECT * FROM raw_data WHERE id IS NOT NULL;

CREATE MASKING POLICY email_mask AS (val string) 
  RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('ANALYST') THEN val
    ELSE '*********'
  END;

ALTER TABLE raw_data MODIFY COLUMN payload SET MASKING POLICY email_mask;

CREATE TAG security_level COMMENT = 'Privacy sensitivity level';

ALTER TABLE raw_data SET TAG security_level = 'highly_sensitive';

CREATE STAGE my_s3_stage URL = 's3://mybucket/data/' CREDENTIALS = (AWS_KEY_ID = 'xxx' AWS_SECRET_KEY = 'yyy');

CREATE PIPE my_pipe AUTO_INGEST = TRUE AS COPY INTO raw_data FROM @my_s3_stage;

CREATE TASK my_task WAREHOUSE = COMPUTE_WH SCHEDULE = '5 MINUTE' AS SELECT 1;

CREATE STREAM raw_data_stream ON TABLE raw_data;
