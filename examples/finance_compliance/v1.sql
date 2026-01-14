-- Finance V1: Basic General Ledger
-- Dialect: DB2 z/OS

CREATE TABLE "general_ledger" (
    "gl_id" BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    "account_number" CHAR(20) NOT NULL,
    "transaction_date" DATE NOT NULL,
    "amount" DECIMAL(19, 4) NOT NULL,
    "currency" CHAR(3) NOT NULL,
    "description" VARCHAR(200),
    CONSTRAINT "pk_gl" PRIMARY KEY ("gl_id")
) IN DATABASE "fin_db"."ts_gl"
  USING STOGROUP "sg_fin"
  PRIQTY 1000
  SECQTY 100;
