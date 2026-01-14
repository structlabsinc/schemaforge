-- Finance V2: Audit Compliance Upgrade
-- Dialect: DB2 z/OS
--
-- Changes:
-- 1. Convert to System-Period Temporal Table for full audit history.
-- 2. Expand 'description' column (VARCHAR 200 -> 500).
-- 3. Adjust storage parameters for growth (PRIQTY 1000 -> 2000).

-- 1. History Table for Versioning
CREATE TABLE "general_ledger_hist" (
    "gl_id" BIGINT NOT NULL,
    "account_number" CHAR(20) NOT NULL,
    "transaction_date" DATE NOT NULL,
    "amount" DECIMAL(19, 4) NOT NULL,
    "currency" CHAR(3) NOT NULL,
    "description" VARCHAR(500),
    "sys_start" TIMESTAMP(12) NOT NULL,
    "sys_end" TIMESTAMP(12) NOT NULL,
    "trans_id" TIMESTAMP(12) NOT NULL
) IN DATABASE "fin_db"."ts_gl_hist"
  USING STOGROUP "sg_hist"
  PRIQTY 5000
  SECQTY 500;

-- 2. Main Table Upgrade
CREATE TABLE "general_ledger" (
    "gl_id" BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    "account_number" CHAR(20) NOT NULL,
    "transaction_date" DATE NOT NULL,
    "amount" DECIMAL(19, 4) NOT NULL,
    "currency" CHAR(3) NOT NULL,
    
    -- Change: Expanded column
    "description" VARCHAR(500),
    
    -- Change: Temporal Columns
    "sys_start" TIMESTAMP(12) NOT NULL GENERATED ALWAYS AS ROW BEGIN,
    "sys_end" TIMESTAMP(12) NOT NULL GENERATED ALWAYS AS ROW END,
    "trans_id" TIMESTAMP(12) GENERATED ALWAYS AS TRANSACTION START ID,
    
    CONSTRAINT "pk_gl" PRIMARY KEY ("gl_id", "sys_start"),
    PERIOD FOR SYSTEM_TIME ("sys_start", "sys_end")
) IN DATABASE "fin_db"."ts_gl"
  USING STOGROUP "sg_fin"
  -- Change: Increased Primary Quantity
  PRIQTY 2000
  SECQTY 100;

-- Link for automatic history tracking
ALTER TABLE "general_ledger" ADD VERSIONING USE HISTORY TABLE "general_ledger_hist";
