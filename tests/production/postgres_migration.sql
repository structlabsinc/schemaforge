-- Migration Script for postgres
CREATE TABLE claims_archive (
    claim_id INT PRIMARY KEY,
    policy_id INT,
    amount DECIMAL(10, 2),
    archived_at TIMESTAMP
);

DROP TABLE claims;

DROP TABLE idx_claims_status;

ALTER TABLE members ADD COLUMN status VARCHAR(20);

ALTER TABLE providers ADD COLUMN is_network BOOLEAN;

ALTER TABLE policies ADD COLUMN deductible DECIMAL(10, 2);

ALTER TABLE policies MODIFY COLUMN coverage_limit DECIMAL(12, 2);