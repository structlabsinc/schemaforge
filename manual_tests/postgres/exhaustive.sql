-- PostgreSQL Exhaustive Coverage Test
CREATE SEQUENCE user_id_seq START 100 INCREMENT 5;

CREATE TYPE status_enum AS ENUM ('active', 'inactive', 'pending');

CREATE DOMAIN email_address AS TEXT CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

CREATE TABLE users (
    id INT PRIMARY KEY DEFAULT nextval('user_id_seq'),
    email email_address UNIQUE NOT NULL,
    status status_enum DEFAULT 'pending',
    profile JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    search_vector tsvector
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_users_profile_gin ON users USING GIN (profile);
CREATE INDEX idx_users_active ON users (email) WHERE status = 'active';

CREATE TABLE orders (
    order_id UUID PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE CASCADE,
    total DECIMAL(12,2) CHECK (total > 0),
    tags TEXT[]
) PARTITION BY RANGE (total);

CREATE TABLE orders_small PARTITION OF orders FOR VALUES FROM (0) TO (1000);
CREATE TABLE orders_large PARTITION OF orders FOR VALUES FROM (1000) TO (1000000);

CREATE UNLOGGED TABLE transient_logs (
    log_id SERIAL PRIMARY KEY,
    log_data TEXT
);

CREATE VIEW user_emails AS SELECT id, email FROM users;

CREATE MATERIALIZED VIEW user_stats AS
SELECT status, count(*) FROM users GROUP BY status;

ALTER TABLE users ENABLE ROW LEVEL SECURITY;
