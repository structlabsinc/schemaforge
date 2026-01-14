-- Migration Script for postgres
ALTER TABLE EVENTS ENABLE ROW LEVEL SECURITY;
ALTER TABLE "events" ALTER COLUMN "status" TYPE status_enum USING "status"::status_enum;