-- Migration Script for postgres
ALTER TABLE "departments" DROP COLUMN "manager_id";
ALTER TABLE "departments" DROP CONSTRAINT "fk_dept_mgr";
ALTER TABLE "employees" DROP COLUMN "mentor_id";
ALTER TABLE "projects" DROP COLUMN "name";