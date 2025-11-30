-- Health Insurance Schema v1

CREATE TABLE members (
    member_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    date_of_birth DATE,
    phone VARCHAR(20),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_members_email ON members(email);

CREATE TABLE providers (
    provider_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    specialty VARCHAR(50),
    contract_start_date DATE
);

CREATE TABLE policies (
    policy_id INT PRIMARY KEY,
    member_id INT,
    policy_number VARCHAR(50) UNIQUE,
    start_date DATE,
    end_date DATE,
    coverage_limit DECIMAL(10, 2),
    CONSTRAINT fk_policies_member FOREIGN KEY (member_id) REFERENCES members(member_id)
);

CREATE TABLE claims (
    claim_id INT PRIMARY KEY,
    policy_id INT,
    provider_id INT,
    claim_date DATE,
    amount DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'submitted',
    diagnosis_code VARCHAR(10),
    CONSTRAINT fk_claims_policy FOREIGN KEY (policy_id) REFERENCES policies(policy_id),
    CONSTRAINT fk_claims_provider FOREIGN KEY (provider_id) REFERENCES providers(provider_id)
);

CREATE INDEX idx_claims_status ON claims(status);
