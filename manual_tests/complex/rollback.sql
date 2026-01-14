-- Rollback Migration Script for postgres
-- This script reverses the forward migration
ALTER TABLE "departments" DROP COLUMN "manager_id";

ALTER TABLE "departments" DROP FOREIGN KEY "fk_dept_mgr";

ALTER TABLE "employees" DROP COLUMN "mentor_id";

ALTER TABLE "projects" DROP COLUMN "name";