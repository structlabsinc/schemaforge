-- Migration Script for postgres
ALTER TABLE "departments" ADD COLUMN "manager_id" INT;
ALTER TABLE "departments" ADD CONSTRAINT "fk_dept_mgr" FOREIGN KEY ("manager_id") REFERENCES "employees";
ALTER TABLE "employees" ADD COLUMN "mentor_id" INT;
ALTER TABLE "projects" ADD COLUMN "name" VARCHAR(100);