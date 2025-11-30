-- Health Insurance Schema v2

CREATE TABLE members (
    member_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    date_of_birth DATE,
    phone VARCHAR(20),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active' -- Added column
);

CREATE INDEX idx_members_email ON members(email);
CREATE INDEX idx_members_status ON members(status); -- Added Index

CREATE TABLE providers (
    provider_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    specialty VARCHAR(50),
    contract_start_date DATE,
    is_network BOOLEAN DEFAULT TRUE -- Added column
);

CREATE TABLE policies (
    policy_id INT PRIMARY KEY,
    member_id INT,
    policy_number VARCHAR(50) UNIQUE,
    start_date DATE,
    end_date DATE,
    coverage_limit DECIMAL(12, 2), -- Modified precision
    deductible DECIMAL(10, 2), -- Added column
    provider_id INT, -- Added column for FK test
    CONSTRAINT fk_policies_member FOREIGN KEY (member_id) REFERENCES members(member_id),
    CONSTRAINT fk_policies_provider FOREIGN KEY (provider_id) REFERENCES providers(provider_id) -- Added FK
);

-- Dropped table claims (Simulating a major refactor or archiving)

CREATE TABLE claims_archive ( -- New table
    claim_id INT PRIMARY KEY,
    policy_id INT,
    amount DECIMAL(10, 2),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
