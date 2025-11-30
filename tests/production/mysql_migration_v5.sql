-- Migration Script for mysql
CREATE TABLE claims_archive (
    claim_id INT PRIMARY KEY,
    policy_id INT,
    amount DECIMAL(10, 2),
    archived_at TIMESTAMP
);

DROP TABLE claims;

ALTER TABLE members ADD COLUMN status VARCHAR(20);

CREATE INDEX idx_members_status ON members(status);

ALTER TABLE providers ADD COLUMN is_network BOOLEAN;

ALTER TABLE policies ADD COLUMN deductible DECIMAL(10, 2);

ALTER TABLE policies MODIFY COLUMN coverage_limit DECIMAL(12, 2);