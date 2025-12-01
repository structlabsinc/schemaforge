/********************************************************************************
 * FILENAME: insurance_wide_schema.sql
 * CONTEXT: Enterprise Health Payer (Data Warehouse Layer)
 * CHARACTERISTICS: 
 * - "Wide" Tables (40-50 columns) simulating legacy denormalization
 * - Complex Data Types (Variant/JSON, Arrays, Geography)
 * - Heavy Views (Regex, Spatial, Windowing over wide datasets)
 ********************************************************************************/

CREATE OR REPLACE DATABASE ENTERPRISE_PAYER_PROD;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_PAYER_PROD.CLAIMS_MART;
USE SCHEMA ENTERPRISE_PAYER_PROD.CLAIMS_MART;

-- ==============================================================================
-- 1. WIDE BASE TABLES (30-50 Columns)
-- ==============================================================================

-- TABLE 1: MEDICAL CLAIMS FACT
-- A massive, denormalized transaction table typical of mainframe migrations.
CREATE OR REPLACE TABLE FCT_MEDICAL_CLAIMS (
    -- [Identity Section]
    CLAIM_ID VARCHAR(50) NOT NULL,
    CLAIM_LINE_NUM INT,
    PREV_CLAIM_ID VARCHAR(50), -- Link for adjustments
    MEMBER_SK VARCHAR(32),     -- Surrogate Key
    PROVIDER_SK VARCHAR(32),
    PAYER_ID VARCHAR(10),
    
    -- [Dates Section]
    DT_SERVICE_START DATE,
    DT_SERVICE_END DATE,
    DT_ADMISSION DATE,
    DT_DISCHARGE DATE,
    DT_RECEIVED TIMESTAMP_NTZ,
    DT_ADJUDICATED TIMESTAMP_NTZ,
    DT_PAID DATE,
    
    -- [Provider & Facility Section]
    RENDERING_NPI VARCHAR(10),
    BILLING_NPI VARCHAR(10),
    FACILITY_NPI VARCHAR(10),
    PLACE_OF_SERVICE_CODE VARCHAR(5),
    BILL_TYPE_CODE VARCHAR(5),
    
    -- [Clinical Coding Section]
    DRG_CODE VARCHAR(5),            -- Diagnosis Related Group
    DRG_SEVERITY INT,
    ADMITTING_DIAGNOSIS VARCHAR(10),
    PRINCIPAL_DIAGNOSIS VARCHAR(10),
    DIAGNOSIS_CODES_ARRAY ARRAY,    -- Array of all ICD-10 codes
    PROCEDURE_CODE_CPT VARCHAR(10),
    PROCEDURE_MODIFIER_1 VARCHAR(5),
    PROCEDURE_MODIFIER_2 VARCHAR(5),
    
    -- [Financials - The "Wide" Part]
    AMT_BILLED_CHARGE NUMBER(18,2),
    AMT_ALLOWED NUMBER(18,2),
    AMT_MEMBER_LIABILITY NUMBER(18,2),
    AMT_DEDUCTIBLE NUMBER(18,2),
    AMT_COPAY NUMBER(18,2),
    AMT_COINSURANCE NUMBER(18,2),
    AMT_COB_SAVINGS NUMBER(18,2),   -- Coordination of Benefits
    AMT_WITHHOLD NUMBER(18,2),
    AMT_NET_PAID NUMBER(18,2),
    
    -- [Adjudication & Status]
    CLAIM_STATUS_CODE VARCHAR(5),   -- 'P', 'D', 'S'
    DENIAL_REASON_CODE_1 VARCHAR(10),
    DENIAL_REASON_CODE_2 VARCHAR(10),
    REMARK_CODES ARRAY,
    IS_CAPITATED BOOLEAN,
    IS_ADJUSTMENT BOOLEAN,
    
    -- [Audit & Metadata]
    SOURCE_SYSTEM_ID VARCHAR(20),   -- e.g., 'FACETS', 'QNXT'
    BATCH_ID VARCHAR(50),
    ETL_LOAD_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORD_HASH VARCHAR(64),
    RAW_HL7_PAYLOAD VARIANT         -- Full original message
) CLUSTER BY (DT_SERVICE_START, MEMBER_SK);


-- TABLE 2: MEMBER DIMENSION (SCD Type 2)
-- Tracks member demographics and policy changes over time.
CREATE OR REPLACE TABLE DIM_MEMBER_HISTORY (
    MEMBER_SK VARCHAR(32) NOT NULL,
    MEMBER_ID VARCHAR(20),
    SUBSCRIBER_ID VARCHAR(20),
    PERSON_NUMBER INT,
    
    -- [Demographics]
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    DOB DATE,
    GENDER_CODE CHAR(1),
    MARITAL_STATUS CHAR(1),
    LANGUAGE_PREF VARCHAR(20),
    RACE_ETHNICITY_CODE VARCHAR(10),
    
    -- [Address & Geo]
    ADDR_LINE_1 VARCHAR(150),
    ADDR_LINE_2 VARCHAR(150),
    CITY VARCHAR(100),
    STATE_CODE CHAR(2),
    ZIP_CODE VARCHAR(10),
    GEO_LOCATION GEOGRAPHY,      -- Exact Lat/Lon
    COUNTY_FIPS VARCHAR(5),
    
    -- [Policy Details]
    PLAN_ID VARCHAR(20),
    GROUP_ID VARCHAR(20),
    LOB_CODE VARCHAR(10),        -- Line of Business (Medicare, Comm)
    COVERAGE_TIER VARCHAR(10),   -- Employee Only, Family, etc.
    HRA_HSA_FLAG BOOLEAN,
    
    -- [Risk & Clinical Attributes]
    RISK_SCORE_RAF FLOAT,        -- Risk Adjustment Factor
    HAS_DIABETES BOOLEAN,
    HAS_HYPERTENSION BOOLEAN,
    HAS_CKD BOOLEAN,
    CARE_MANAGER_ID VARCHAR(20),
    
    -- [SCD Metadata]
    EFFECTIVE_START_DATE DATE,
    EFFECTIVE_END_DATE DATE,
    IS_CURRENT BOOLEAN,
    CHANGE_REASON VARCHAR(50),
    META_JSON VARIANT            -- Extra custom fields
);

-- ==============================================================================
-- 2. COMPLEX VIEWS (Handling Wide Data)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- VIEW 1: The Claims Payment Waterfall (Windowing over 50 columns)
-- Complexity: Calculates "Save Rates" and "Denial Trends" using Window functions
-- on the massive financial columns.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_FINANCIAL_WATERFALL_ANALYSIS AS
SELECT 
    f.PAYER_ID,
    f.LOB_CODE, -- Needs Join to Dimension
    f.DT_PAID,
    
    -- Aggregated Financials
    SUM(f.AMT_BILLED_CHARGE) as total_billed,
    SUM(f.AMT_ALLOWED) as total_allowed,
    SUM(f.AMT_NET_PAID) as total_paid,
    
    -- Complex Ratios
    DIV0(SUM(f.AMT_NET_PAID), SUM(f.AMT_ALLOWED)) as payment_to_allowed_ratio,
    DIV0(SUM(f.AMT_COB_SAVINGS), SUM(f.AMT_BILLED_CHARGE)) as cob_savings_rate,
    
    -- Moving Average of Denials (Window Function)
    AVG(CASE WHEN f.CLAIM_STATUS_CODE = 'D' THEN 1 ELSE 0 END) 
        OVER (PARTITION BY f.PAYER_ID ORDER BY f.DT_PAID ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
        as rolling_30d_denial_rate,
        
    -- Rank Providers by Cost
    RANK() OVER (PARTITION BY f.DRG_CODE ORDER BY SUM(f.AMT_NET_PAID) DESC) as drg_cost_rank

FROM FCT_MEDICAL_CLAIMS f
WHERE f.DT_PAID >= DATEADD('year', -2, CURRENT_DATE())
GROUP BY 1, 2, 3, f.CLAIM_STATUS_CODE, f.DRG_CODE;


-- ------------------------------------------------------------------------------
-- VIEW 2: The "Member 360" Risk Engine (Geo + Array + JSON)
-- Complexity: Joins Wide Fact + Wide Dim, parses Arrays, computes Distance
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_MEMBER_RISK_360 AS
SELECT 
    m.MEMBER_ID,
    m.RISK_SCORE_RAF,
    
    -- 1. Extract recent high-cost diagnoses from Fact Array
    ARRAY_INTERSECTION(
        f.DIAGNOSIS_CODES_ARRAY, 
        ARRAY_CONSTRUCT('E11.9', 'I10', 'N18.9') -- Diabetes, HTN, CKD
    ) as chronic_conditions_found,
    
    -- 2. Geospatial Risk (Distance to nearest Trauma Center)
    -- *Assuming a reference point for Trauma Center roughly in center of zipcode*
    ST_DISTANCE(m.GEO_LOCATION, ST_POINT(-74.0060, 40.7128)) as dist_to_hub,
    
    -- 3. JSON Extraction from wide payload
    f.RAW_HL7_PAYLOAD:ADT_A01:PV1:assignedPatientLocation:facility::STRING as last_facility,
    
    -- 4. Financial Toxicity (High OOP)
    SUM(f.AMT_DEDUCTIBLE + f.AMT_COPAY + f.AMT_COINSURANCE) as total_out_of_pocket

FROM DIM_MEMBER_HISTORY m
JOIN FCT_MEDICAL_CLAIMS f 
    ON m.MEMBER_SK = f.MEMBER_SK
WHERE m.IS_CURRENT = TRUE
  AND f.DT_SERVICE_START > DATEADD('month', -12, CURRENT_DATE())
GROUP BY 1, 2, m.GEO_LOCATION, f.RAW_HL7_PAYLOAD, f.DIAGNOSIS_CODES_ARRAY;


-- ------------------------------------------------------------------------------
-- VIEW 3: Fraud/Waste/Abuse (FWA) Detector
-- Complexity: Regular Expressions on Text Columns + Pattern Matching
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_FWA_PATTERN_DETECTOR AS
SELECT 
    CLAIM_ID,
    RENDERING_NPI,
    PROCEDURE_CODE_CPT,
    
    -- Rule 1: Detect Upcoding (CPT ending in 4 or 5 often higher severity)
    CASE 
        WHEN PROCEDURE_CODE_CPT RLIKE '992[0-1][4-5]' THEN 'HIGH_SEVERITY_E_M'
        ELSE 'STANDARD'
    END as coding_level,
    
    -- Rule 2: Unbundling Detection (Modifiers 25/59 usage)
    CASE 
        WHEN PROCEDURE_MODIFIER_1 IN ('25', '59') OR PROCEDURE_MODIFIER_2 IN ('25', '59') 
        THEN TRUE 
        ELSE FALSE 
    END as has_override_modifier,
    
    -- Rule 3: Text Mining Denial Reasons
    CASE 
        WHEN DENIAL_REASON_CODE_1 LIKE 'DUP%' OR DENIAL_REASON_CODE_2 LIKE 'DUP%' 
        THEN 'DUPLICATE'
        WHEN RAW_HL7_PAYLOAD:preAuth::STRING IS NULL AND AMT_BILLED_CHARGE > 1000 
        THEN 'MISSING_AUTH_HIGH_$$'
        ELSE 'CLEAN'
    END as flag_category

FROM FCT_MEDICAL_CLAIMS
WHERE AMT_BILLED_CHARGE > 500;