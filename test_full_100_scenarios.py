#!/usr/bin/env python3
"""
Comprehensive 100-Scenario Test Suite - Direct port from PowerShell
Tests ALL scenarios to provide accurate pass rate for release validation
"""
import subprocess
import sys
import os
import re
import shutil

SF_CMD = ["python3", "-m", "schemaforge.main"]
GOD_SOURCE = "god_level_schema.sql"
GOD_TARGET = "god_level_schema_Copy.sql"
HEALTH_SOURCE = "health_insurance.sql"
HEALTH_TARGET = "health_insurance_Copy.sql"

def reset_files():
    if os.path.exists(GOD_SOURCE):
        shutil.copy(GOD_SOURCE, GOD_TARGET)
    if os.path.exists(HEALTH_SOURCE):
        shutil.copy(HEALTH_SOURCE, HEALTH_TARGET)

def run_sf(source, target):
    try:
        cmd = SF_CMD + ["compare", "--source", source, "--target", target, "--dialect", "snowflake", "--plan"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return result.stdout + result.stderr
    except Exception as e:
        return str(e)

def test_scenario(num, name, file_to_mod, mod_func, expected_pattern):
    print(f"{num:3d}. {name:55} ", end='', flush=True)
    reset_files()
    
    try:
        with open(file_to_mod, 'r') as f:
            content = f.read()
        
        new_content = mod_func(content)
        
        with open(file_to_mod, 'w') as f:
            f.write(new_content)
        
        source = GOD_SOURCE if file_to_mod == GOD_TARGET else HEALTH_SOURCE
        output = run_sf(source, file_to_mod)
        
        if re.search(expected_pattern, output, re.IGNORECASE):
            print("✅")
            return True
        else:
            print("❌")
            if os.getenv('VERBOSE'):
                print(f"    Expected: '{expected_pattern}'")
                print(f"    Got: {output[:200]}")
            return False
    except Exception as e:
        print(f"💥 {str(e)[:50]}")
        return False

# All 100 scenarios from PowerShell script
scenarios = []

# Health Insurance Scenarios (1-13)
scenarios.append(('Add Column', HEALTH_TARGET, 
    lambda c: c.replace("PAYER_ID VARCHAR(10),", "PAYER_ID VARCHAR(10),\n    NEW_COL VARCHAR(50),"), 
    r'Column: NEW_COL'))

scenarios.append(('Drop Column', HEALTH_TARGET,
    lambda c: c.replace("MEMBER_SK VARCHAR(32),", ""),
    r'Drop Column: MEMBER_SK'))

scenarios.append(('Rename Column (Add/Drop)', HEALTH_TARGET,
    lambda c: c.replace("CITY VARCHAR(100),", "MEMBER_CITY VARCHAR(100),"),
    r'Column: MEMBER_CITY'))

scenarios.append(('Modify Type (Safe)', HEALTH_TARGET,
    lambda c: c.replace("AMT_BILLED_CHARGE NUMBER(18,2),", "AMT_BILLED_CHARGE NUMBER(20,4),"),
    r'Modify Column: AMT_BILLED_CHARGE'))

scenarios.append(('Modify Type (Unsafe)', HEALTH_TARGET,
    lambda c: c.replace("CLAIM_ID VARCHAR(50) NOT NULL,", "CLAIM_ID INT NOT NULL,"),
    r'Modify Column: CLAIM_ID'))

scenarios.append(('Add Not Null', HEALTH_TARGET,
    lambda c: c.replace("ADMITTING_DIAGNOSIS VARCHAR(10),", "ADMITTING_DIAGNOSIS VARCHAR(10) NOT NULL,"),
    r'Modify Column: ADMITTING_DIAGNOSIS'))

scenarios.append(('Drop Not Null', HEALTH_TARGET,
    lambda c: c.replace("CLAIM_ID VARCHAR(50) NOT NULL,", "CLAIM_ID VARCHAR(50),"),
    r'Modify Column: CLAIM_ID'))

scenarios.append(('Add Default', HEALTH_TARGET,
    lambda c: c.replace("SOURCE_SYSTEM_ID VARCHAR(20),", "SOURCE_SYSTEM_ID VARCHAR(20) DEFAULT 'UNKNOWN',"),
    r'Modify Column: SOURCE_SYSTEM_ID'))

scenarios.append(('Add Table', HEALTH_TARGET,
    lambda c: c + "\nCREATE TABLE DIM_PROVIDER (PROVIDER_ID INT, NAME VARCHAR(100));",
    r'Create Table: DIM_PROVIDER'))

scenarios.append(('Drop Table', HEALTH_TARGET,
    lambda c: c.replace("CREATE OR REPLACE TABLE DIM_MEMBER_HISTORY", "-- CREATE OR REPLACE TABLE DIM_MEMBER_HISTORY"),
    r'Drop Table: DIM_MEMBER_HISTORY'))

scenarios.append(('Rename Table', HEALTH_TARGET,
    lambda c: c.replace("CREATE OR REPLACE TABLE FCT_MEDICAL_CLAIMS", "CREATE OR REPLACE TABLE FCT_CLAIMS"),
    r'Create Table: FCT_CLAIMS'))

scenarios.append(('Add Primary Key', HEALTH_TARGET,
    lambda c: c.replace("PAYER_ID VARCHAR(10),", "PAYER_ID VARCHAR(10),\n    PRIMARY KEY (CLAIM_ID),"),
    r'Primary Key \(PK\): False -> True'))

scenarios.append(('Add Transient Table', HEALTH_TARGET,
    lambda c: c + "\nCREATE TRANSIENT TABLE STAGE_CLAIMS (RAW_DATA VARIANT);",
    r'Create Table: STAGE_CLAIMS'))

# God Level Scenarios (14-28)
scenarios.append(('Add Geography', GOD_TARGET,
    lambda c: c.replace("ATTRIBUTES OBJECT,", "ATTRIBUTES OBJECT,\n    LOCATION GEOGRAPHY,"),
    r'Column: location'))

scenarios.append(('Modify Variant to Object', GOD_TARGET,
    lambda c: c.replace("PAYLOAD VARIANT,", "PAYLOAD OBJECT,"),
    r'Modify Column: PAYLOAD'))

scenarios.append(('Add Array Column', GOD_TARGET,
    lambda c: c.replace("ASSET_CLASS VARCHAR(10),", "ASSET_CLASS VARCHAR(10),\n    TAGS ARRAY,"),
    r'Column: TAGS'))

scenarios.append(('Cluster By Change', GOD_TARGET,
    lambda c: c.replace("CLUSTER BY (TO_DATE(TIMESTAMP), ACTOR_ID);", "CLUSTER BY (ACTOR_ID, TO_DATE(TIMESTAMP));"),
    r'Cluster Key'))

scenarios.append(('Database Property', GOD_TARGET,
    lambda c: c.replace("CREATE SCHEMA TITAN_DB.CORE;", "CREATE SCHEMA TITAN_DB.CORE;\nALTER DATABASE TITAN_DB SET DATA_RETENTION_TIME_IN_DAYS = 90;"),
    r'DATA_RETENTION'))

scenarios.append(('Modify View Logic', GOD_TARGET,
    lambda c: c.replace("WHERE p.DEPTH < 20", "WHERE p.DEPTH < 50"),
    r'Modify View: V_COMPLEX_01_HIERARCHY_FLATTENER'))

scenarios.append(('Add View', GOD_TARGET,
    lambda c: c + "\nCREATE VIEW V_TEST_NEW AS SELECT * FROM EVENTS_OMNIVERSE;",
    r'Create View: V_TEST_NEW'))

scenarios.append(('Drop View', GOD_TARGET,
    lambda c: c.replace("CREATE OR REPLACE VIEW V_COMPLEX_05_AUDIENCE_OVERLAP", "-- CREATE OR REPLACE VIEW V_COMPLEX_05_AUDIENCE_OVERLAP"),
    r'Drop View: V_COMPLEX_05_AUDIENCE_OVERLAP'))

scenarios.append(('Rename View', GOD_TARGET,
    lambda c: c.replace("CREATE OR REPLACE VIEW V_COMPLEX_02_SESSION_PATTERN", "CREATE OR REPLACE VIEW V_SESSION_PATTERN"),
    r'Create View: V_SESSION_PATTERN'))

scenarios.append(('Add Materialized View', GOD_TARGET,
    lambda c: c + "\nCREATE MATERIALIZED VIEW MV_TEST AS SELECT ACTOR_ID, COUNT(*) as CNT FROM EVENTS_OMNIVERSE GROUP BY 1;",
    r'Create Materialized View: MV_TEST'))

scenarios.append(('Case Sensitivity (No Change)', GOD_TARGET,
    lambda c: c.replace("EVENT_UUID VARCHAR(64)", "event_uuid VARCHAR(64)"),
    r'No changes detected'))

scenarios.append(('Whitespace (No Change)', GOD_TARGET,
    lambda c: c.replace("SELECT * FROM EVENTS_OMNIVERSE", "SELECT * \n    FROM EVENTS_OMNIVERSE"),
    r'No changes detected'))

scenarios.append(('Multiple Changes', HEALTH_TARGET,
    lambda c: c.replace("PAYER_ID VARCHAR(10),", "PAYER_ID VARCHAR(10),\n    NEW_COL_1 VARCHAR(10),").replace("MEMBER_SK VARCHAR(32),", ""),
    r'Column: NEW_COL_1'))

scenarios.append(('Add Schema', GOD_TARGET,
    lambda c: c + "\nCREATE SCHEMA NEW_SCHEMA;",
    r'Create Schema: NEW_SCHEMA'))

# Expanded Scenarios (29-50)
scenarios.append(('Add Check Constraint', HEALTH_TARGET,
    lambda c: c.replace("AMT_BILLED_CHARGE NUMBER(18,2),", "AMT_BILLED_CHARGE NUMBER(18,2),\n    CONSTRAINT ck_billed_pos CHECK (AMT_BILLED_CHARGE > 0),"),
    r'Constraint'))

scenarios.append(('Drop Check Constraint', HEALTH_TARGET,
    lambda c: c.replace("NOT NULL", ""),
    r'Modify Column'))

scenarios.append(('Add Unique Constraint', HEALTH_TARGET,
    lambda c: c.replace("MEMBER_ID VARCHAR(20),", "MEMBER_ID VARCHAR(20) UNIQUE,"),
    r'Unique'))

scenarios.append(('Add Foreign Key', HEALTH_TARGET,
    lambda c: c.replace("PAYER_ID VARCHAR(10),", "PAYER_ID VARCHAR(10) REFERENCES DIM_PAYER(ID),"),
    r'Foreign Key'))

scenarios.append(('Create Sequence', GOD_TARGET,
    lambda c: c + "\nCREATE SEQUENCE SEQ_TEST START = 1 INCREMENT = 1;",
    r'Create Sequence: SEQ_TEST'))

scenarios.append(('Create File Format', GOD_TARGET,
    lambda c: c + "\nCREATE FILE FORMAT FF_CSV TYPE = CSV;",
    r'Create File Format: FF_CSV'))

scenarios.append(('Create Stage', GOD_TARGET,
    lambda c: c + "\nCREATE STAGE STG_TEST FILE_FORMAT = FF_CSV;",
    r'Create Stage: STG_TEST'))

scenarios.append(('Create Pipe', GOD_TARGET,
    lambda c: c + "\nCREATE PIPE PIPE_TEST AS COPY INTO EVENTS_OMNIVERSE FROM @STG_TEST;",
    r'Create Pipe: PIPE_TEST'))

scenarios.append(('Create Stream', GOD_TARGET,
    lambda c: c + "\nCREATE STREAM STREAM_TEST ON TABLE EVENTS_OMNIVERSE;",
    r'Create Stream: STREAM_TEST'))

scenarios.append(('Create Task', GOD_TARGET,
    lambda c: c + "\nCREATE TASK TASK_TEST SCHEDULE = '1 MINUTE' AS SELECT 1;",
    r'Create Task: TASK_TEST'))

scenarios.append(('Create Procedure', GOD_TARGET,
    lambda c: c + "\nCREATE PROCEDURE PROC_TEST() RETURNS STRING LANGUAGE JAVASCRIPT AS $$ return 'done'; $$;",
    r'Create Procedure: PROC_TEST'))

scenarios.append(('Create Function', GOD_TARGET,
    lambda c: c + "\nCREATE FUNCTION FUNC_TEST() RETURNS INT AS '1';",
    r'Create Function: FUNC_TEST'))

scenarios.append(('Add Masking Policy', GOD_TARGET,
    lambda c: c + "\nCREATE MASKING POLICY MP_TEST AS (val string) RETURNS string -> CASE WHEN CURRENT_ROLE() = 'ADMIN' THEN val ELSE '***' END;",
    r'Create Masking Policy: MP_TEST'))

scenarios.append(('Apply Masking Policy', GOD_TARGET,
    lambda c: c.replace("ACTOR_ID VARCHAR(32),", "ACTOR_ID VARCHAR(32) MASKING POLICY MP_TEST,"),
    r'Policy'))

scenarios.append(('Add Row Access Policy', GOD_TARGET,
    lambda c: c + "\nCREATE ROW ACCESS POLICY RAP_TEST AS (val string) RETURNS boolean -> true;",
    r'Create Row Access Policy: RAP_TEST'))

scenarios.append(('Apply Row Access Policy', GOD_TARGET,
    lambda c: c + "\nALTER TABLE EVENTS_OMNIVERSE ADD ROW ACCESS POLICY RAP_TEST ON (ACTOR_ID);",
    r'Policy'))

scenarios.append(('Set Tag', GOD_TARGET,
    lambda c: c + "\nALTER TABLE EVENTS_OMNIVERSE SET TAG cost_center = 'marketing';",
    r'Tag'))

scenarios.append(('Add Table Comment', HEALTH_TARGET,
    lambda c: c.replace("CLUSTER BY (DT_SERVICE_START, MEMBER_SK);", "CLUSTER BY (DT_SERVICE_START, MEMBER_SK) COMMENT = 'Main claims table';"),
    r'Comment'))

scenarios.append(('Add Column Comment', HEALTH_TARGET,
    lambda c: c.replace("CLAIM_ID VARCHAR(50) NOT NULL,", "CLAIM_ID VARCHAR(50) NOT NULL COMMENT 'Primary Key',"),
    r'Comment'))

scenarios.append(('Alter Column Comment', HEALTH_TARGET,
    lambda c: c.replace("CLAIM_ID VARCHAR(50) NOT NULL,", "CLAIM_ID VARCHAR(50) NOT NULL COMMENT 'Updated Comment',"),
    r'Comment'))

scenarios.append(('Collation', HEALTH_TARGET,
    lambda c: c.replace("FIRST_NAME VARCHAR(100),", "FIRST_NAME VARCHAR(100) COLLATE 'en-ci',"),
    r'Collation'))

scenarios.append(('Vector Type', GOD_TARGET,
    lambda c: c.replace("QUANTUM_STATE BINARY(8000),", "QUANTUM_STATE VECTOR(INT, 3),"),
    r'VECTOR'))

scenarios.append(('Geometry Type', GOD_TARGET,
    lambda c: c.replace("GEO_TRACE GEOGRAPHY,", "GEO_TRACE GEOMETRY,"),
    r'GEOMETRY'))

# Mega-Suite Expansion (51-100)
scenarios.append(('Dynamic Table', GOD_TARGET,
    lambda c: c + "\nCREATE DYNAMIC TABLE DT_TEST TARGET_LAG = '1 minute' AS SELECT * FROM EVENTS_OMNIVERSE;",
    r'Create Dynamic Table'))

scenarios.append(('Iceberg Table', GOD_TARGET,
    lambda c: c + "\nCREATE ICEBERG TABLE IT_TEST EXTERNAL_VOLUME='vol1' CATALOG='cat1' AS SELECT 1;",
    r'Create Iceberg Table'))

scenarios.append(('Hybrid Table', GOD_TARGET,
    lambda c: c + "\nCREATE HYBRID TABLE HT_TEST (ID INT PRIMARY KEY);",
    r'Create Hybrid Table'))

scenarios.append(('Event Table', GOD_TARGET,
    lambda c: c + "\nCREATE EVENT TABLE ET_TEST;",
    r'Create Event Table'))

scenarios.append(('Add Search Optimization', GOD_TARGET,
    lambda c: c + "\nALTER TABLE EVENTS_OMNIVERSE ADD SEARCH OPTIMIZATION;",
    r'Search Optimization'))

scenarios.append(('Drop Search Optimization', GOD_TARGET,
    lambda c: c + "\nALTER TABLE EVENTS_OMNIVERSE DROP SEARCH OPTIMIZATION;",
    r'Search Optimization'))

scenarios.append(('Create External Table', GOD_TARGET,
    lambda c: c + "\nCREATE EXTERNAL TABLE EXT_TEST (ID INT) LOCATION=@stg_test FILE_FORMAT=ff_csv;",
    r'Create External Table'))

scenarios.append(('Create External Function', GOD_TARGET,
    lambda c: c + "\nCREATE EXTERNAL FUNCTION EXT_FUNC_TEST(n INT) RETURNS INT API_INTEGRATION = my_api AS 'https://xyz';",
    r'Create External Function'))

scenarios.append(('Secure View', GOD_TARGET,
    lambda c: c + "\nCREATE SECURE VIEW V_SECURE AS SELECT * FROM EVENTS_OMNIVERSE;",
    r'Create Secure View'))

scenarios.append(('Recursive View', GOD_TARGET,
    lambda c: c + "\nCREATE VIEW V_RECURSIVE AS WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte;",
    r'Create View: V_RECURSIVE'))

scenarios.append(('View with Qualify', GOD_TARGET,
    lambda c: c + "\nCREATE VIEW V_QUALIFY AS SELECT * FROM EVENTS_OMNIVERSE QUALIFY ROW_NUMBER() OVER (ORDER BY TIMESTAMP) = 1;",
    r'Create View: V_QUALIFY'))

scenarios.append(('Virtual Column', HEALTH_TARGET,
    lambda c: c.replace("AMT_BILLED_CHARGE NUMBER(18,2),", "AMT_BILLED_CHARGE NUMBER(18,2),\n    AMT_TOTAL AS (AMT_BILLED_CHARGE * 1.1),"),
    r'Column: AMT_TOTAL'))

scenarios.append(('Identity Column', HEALTH_TARGET,
    lambda c: c.replace("PERSON_NUMBER INT,", "PERSON_NUMBER INT IDENTITY(1,1),"),
    r'Identity'))

scenarios.append(('Alter Identity', HEALTH_TARGET,
    lambda c: c.replace("PERSON_NUMBER INT,", "PERSON_NUMBER INT IDENTITY(1,1),") + "\nALTER TABLE DIM_MEMBER_HISTORY ALTER COLUMN PERSON_NUMBER SET START 100;",
    r'Identity'))

scenarios.append(('Composite Primary Key', HEALTH_TARGET,
    lambda c: c.replace("CLAIM_ID VARCHAR(50) NOT NULL,", "CLAIM_ID VARCHAR(50) NOT NULL,\n    PRIMARY KEY (CLAIM_ID, MEMBER_SK),"),
    r'PK'))

scenarios.append(('Composite Unique Key', HEALTH_TARGET,
    lambda c: c.replace("MEMBER_ID VARCHAR(20),", "MEMBER_ID VARCHAR(20),\n    UNIQUE (MEMBER_ID, PAYER_ID),"),
    r'Unique'))

scenarios.append(('Self-Referencing FK', HEALTH_TARGET,
    lambda c: c.replace("PERSON_NUMBER INT,", "PERSON_NUMBER INT,\n    PARENT_ID INT REFERENCES DIM_MEMBER_HISTORY(PERSON_NUMBER),"),
    r'Foreign Key'))

scenarios.append(('Named Constraint', HEALTH_TARGET,
    lambda c: c.replace("CLAIM_ID VARCHAR(50) NOT NULL,", "CLAIM_ID VARCHAR(50) NOT NULL,\n    CONSTRAINT pk_claims PRIMARY KEY (CLAIM_ID),"),
    r'PK'))

scenarios.append(('Unset Tag', GOD_TARGET,
    lambda c: c + "\nALTER TABLE EVENTS_OMNIVERSE UNSET TAG cost_center;",
    r'Unset Tag'))

scenarios.append(('Unset Masking Policy', GOD_TARGET,
    lambda c: c + "\nALTER TABLE EVENTS_OMNIVERSE MODIFY COLUMN ACTOR_ID UNSET MASKING POLICY;",
    r'Unset Policy'))

scenarios.append(('Drop Row Access Policy', GOD_TARGET,
    lambda c: c + "\nALTER TABLE EVENTS_OMNIVERSE DROP ROW ACCESS POLICY RAP_TEST;",
    r'Drop Policy'))

scenarios.append(('Tag on View', GOD_TARGET,
    lambda c: c + "\nALTER VIEW V_COMPLEX_01_HIERARCHY_FLATTENER SET TAG cost_center = 'analytics';",
    r'Tag'))

scenarios.append(('Stream on View', GOD_TARGET,
    lambda c: c + "\nCREATE STREAM S_VIEW ON VIEW V_COMPLEX_01_HIERARCHY_FLATTENER;",
    r'Create Stream: S_VIEW'))

scenarios.append(('Append-Only Stream', GOD_TARGET,
    lambda c: c + "\nCREATE STREAM S_APPEND ON TABLE EVENTS_OMNIVERSE APPEND_ONLY = TRUE;",
    r'Create Stream: S_APPEND'))

scenarios.append(('Alter Task Schedule', GOD_TARGET,
    lambda c: c + "\nALTER TASK TASK_TEST SET SCHEDULE = '5 MINUTE';",
    r'Alter Task'))

scenarios.append(('Task Graph', GOD_TARGET,
    lambda c: c + "\nCREATE TASK TASK_CHILD AFTER TASK_TEST AS SELECT 1;",
    r'Create Task: TASK_CHILD'))

scenarios.append(('Create Alert', GOD_TARGET,
    lambda c: c + "\nCREATE ALERT ALERT_TEST WAREHOUSE = COMPUTE_WH SCHEDULE = '1 MINUTE' IF (EXISTS(SELECT * FROM EVENTS_OMNIVERSE)) THEN INSERT INTO LOGS VALUES ('Alert');",
    r'Create Alert'))

scenarios.append(('Alter Alert', GOD_TARGET,
    lambda c: c + "\nALTER ALERT ALERT_TEST RESUME;",
    r'Alter Alert'))

scenarios.append(('Quoted Identifiers', GOD_TARGET,
    lambda c: c + '\nCREATE TABLE "My Table" (ID INT);',
    r'Create Table: My Table'))

scenarios.append(('Long Comments', GOD_TARGET,
    lambda c: c + f"\nCOMMENT ON TABLE EVENTS_OMNIVERSE IS '{'a'*1000}';",
    r'Comment'))

scenarios.append(('SQL Injection Pattern', HEALTH_TARGET,
    lambda c: c.replace("SOURCE_SYSTEM_ID VARCHAR(20),", "SOURCE_SYSTEM_ID VARCHAR(20) DEFAULT '''; DROP TABLE --',"),
    r'Modify Column'))

scenarios.append(('Case Sensitivity (Quoted)', GOD_TARGET,
    lambda c: c.replace("ACTOR_ID VARCHAR(32),", 'ACTOR_ID VARCHAR(32),\n    "Actor_Id" VARCHAR(32),'),
    r'Column: Actor_Id'))

scenarios.append(('Keyword as Identifier', GOD_TARGET,
    lambda c: c + '\nCREATE TABLE "TABLE" (ID INT);',
    r'Create Table: TABLE'))

scenarios.append(('Empty Table', GOD_TARGET,
    lambda c: c + "\nCREATE TABLE EMPTY_TABLE ();",
    r'Create Table: EMPTY_TABLE'))

scenarios.append(('Duplicate Columns', HEALTH_TARGET,
    lambda c: c.replace("PAYER_ID VARCHAR(10),", "PAYER_ID VARCHAR(10),\n    PAYER_ID VARCHAR(10),"),
    r'Error'))

scenarios.append(('Cross-Schema FK', HEALTH_TARGET,
    lambda c: c.replace("PAYER_ID VARCHAR(10),", "PAYER_ID VARCHAR(10) REFERENCES OTHER_SCHEMA.DIM_PAYER(ID),"),
    r'Foreign Key'))

scenarios.append(('Cloning', GOD_TARGET,
    lambda c: c + "\nCREATE TABLE CLONE_TEST CLONE EVENTS_OMNIVERSE;",
    r'Create Table: CLONE_TEST'))

scenarios.append(('Undrop', GOD_TARGET,
    lambda c: c + "\nUNDROP TABLE EVENTS_OMNIVERSE;",
    r'Undrop'))

scenarios.append(('Swap With', GOD_TARGET,
    lambda c: c + "\nALTER TABLE EVENTS_OMNIVERSE SWAP WITH EVENTS_OMNIVERSE_V2;",
    r'Swap'))

scenarios.append(('Alter Pipe', GOD_TARGET,
    lambda c: c + "\nALTER PIPE PIPE_TEST REFRESH;",
    r'Alter Pipe'))

scenarios.append(('Alter File Format', GOD_TARGET,
    lambda c: c + "\nALTER FILE FORMAT FF_CSV SET COMPRESSION = GZIP;",
    r'Alter File Format'))

scenarios.append(('Create XML Format', GOD_TARGET,
    lambda c: c + "\nCREATE FILE FORMAT FF_XML TYPE = XML;",
    r'Create File Format: FF_XML'))

scenarios.append(('Create JSON Format', GOD_TARGET,
    lambda c: c + "\nCREATE FILE FORMAT FF_JSON TYPE = JSON;",
    r'Create File Format: FF_JSON'))

scenarios.append(('Grant Select', GOD_TARGET,
    lambda c: c + "\nGRANT SELECT ON TABLE EVENTS_OMNIVERSE TO ROLE ANALYST;",
    r'Grant'))

scenarios.append(('Revoke Select', GOD_TARGET,
    lambda c: c + "\nREVOKE SELECT ON TABLE EVENTS_OMNIVERSE FROM ROLE ANALYST;",
    r'Revoke'))

scenarios.append(('Create Managed Schema', GOD_TARGET,
    lambda c: c + "\nCREATE MANAGED SCHEMA MANAGED_SCHEMA;",
    r'Create Schema: MANAGED_SCHEMA'))

scenarios.append(('Alter Schema Retention', GOD_TARGET,
    lambda c: c + "\nALTER SCHEMA TITAN_DB.CORE SET DATA_RETENTION_TIME_IN_DAYS = 1;",
    r'DATA_RETENTION'))

scenarios.append(('Create Database Role', GOD_TARGET,
    lambda c: c + "\nCREATE DATABASE ROLE DB_ADMIN;",
    r'Create Database Role'))

scenarios.append(('Comment on Database', GOD_TARGET,
    lambda c: c + "\nCOMMENT ON DATABASE TITAN_DB IS 'Main DB';",
    r'Comment'))

if __name__ == '__main__':
    if not os.path.exists(GOD_SOURCE):
        print(f"ERROR: {GOD_SOURCE} not found")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"COMPREHENSIVE 100-SCENARIO TEST SUITE")
    print(f"{'='*80}\n")
    
    passed = 0
    failed_scenarios = []
    
    for i, (name, file, mod, expect) in enumerate(scenarios, 1):
        if test_scenario(i, name, file, mod, expect):
            passed += 1
        else:
            failed_scenarios.append((i, name))
    
    print(f"\n{'='*80}")
    print(f"RESULTS: {passed}/{len(scenarios)} passed ({100*passed//len(scenarios)}%)")
    print(f"{'='*80}\n")
    
    if failed_scenarios:
        print(f"FAILED SCENARIOS ({len(failed_scenarios)}):")
        for num, name in failed_scenarios:
            print(f"  {num}. {name}")
        print()
    
    sys.exit(0 if passed == len(scenarios) else 1)
