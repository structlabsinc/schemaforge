-- Migration Script for postgres
CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    event_data JSONB
);

ALTER TABLE users ADD COLUMN last_login TIMESTAMP;

ALTER TABLE orders ADD COLUMN status VARCHAR(50);