-- Snowflake God Mode: Complex Schema for Adversarial Testing

-- 1. Database & Schema (Context)
CREATE DATABASE IF NOT EXISTS god_mode_db;
USE SCHEMA god_mode_db.public;

-- 2. File Formats
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

CREATE OR REPLACE FILE FORMAT my_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- 3. Sequences
CREATE SEQUENCE seq_user_id START = 1 INCREMENT = 1;

-- 4. Tables with Advanced Properties
CREATE OR REPLACE TRANSIENT TABLE raw_events (
    event_id STRING DEFAULT UUID_STRING(),
    event_time TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    payload VARIANT,
    metadata OBJECT,
    tags ARRAY,
    user_info GEOGRAPHY
)
DATA_RETENTION_TIME_IN_DAYS = 0
COMMENT = 'Transient table for raw ingestion';

CREATE TABLE clustered_orders (
    order_id NUMBER(38,0) DEFAULT seq_user_id.NEXTVAL,
    customer_id NUMBER(38,0),
    order_date DATE,
    amount NUMBER(10,2),
    status VARCHAR(20)
)
CLUSTER BY (order_date, customer_id)
COMMENT = 'Clustered table for performance';

-- 5. Views (Secure & Materialized)
CREATE OR REPLACE SECURE VIEW v_high_value_orders AS
SELECT * FROM clustered_orders WHERE amount > 1000;

-- 6. Stages
CREATE OR REPLACE STAGE my_s3_stage
  URL='s3://my-bucket/data/'
  CREDENTIALS=(AWS_KEY_ID='xxxx' AWS_SECRET_KEY='yyyy')
  FILE_FORMAT = my_csv_format;

-- 7. Pipes
CREATE OR REPLACE PIPE raw_event_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO raw_events
  FROM @my_s3_stage
  FILE_FORMAT = (TYPE = 'JSON');

-- 8. Streams
CREATE OR REPLACE STREAM orders_stream ON TABLE clustered_orders;

-- 9. Tasks
CREATE OR REPLACE TASK daily_cleanup_task
  WAREHOUSE = compute_wh
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  AS
  DELETE FROM raw_events WHERE event_time < DATEADD(day, -7, CURRENT_TIMESTAMP());

-- 10. Procedures (JavaScript)
CREATE OR REPLACE PROCEDURE cleanup_old_data(days_retention FLOAT)
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  AS
  $$
  var sql_command = "DELETE FROM raw_events WHERE event_time < DATEADD(day, -" + DAYS_RETENTION + ", CURRENT_TIMESTAMP())";
  snowflake.execute({sqlText: sql_command});
  return "Cleanup complete";
  $$;
