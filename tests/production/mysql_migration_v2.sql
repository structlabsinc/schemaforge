-- Migration Script for mysql
DROP TABLE claims;

ALTER TABLE providers ADD COLUMN is_network BOOLEAN;

ALTER TABLE policies ADD COLUMN deductible DECIMAL(10, 2);

ALTER TABLE policies MODIFY COLUMN coverage_limit DECIMAL(12, 2);