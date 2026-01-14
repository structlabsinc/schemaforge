-- Migration Script for snowflake
ALTER TABLE "logs" MODIFY COLUMN "msg" VARCHAR;