-- Healthcare V1: Patient Records
-- Dialect: PostgreSQL

CREATE TABLE patients (
    patient_id UUID PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    dob DATE NOT NULL,
    ssn TEXT, -- PII (unprotected in V1)
    insurance_provider TEXT
);

CREATE TABLE medical_records (
    record_id SERIAL PRIMARY KEY,
    patient_id UUID REFERENCES patients(patient_id),
    diagnosis_code TEXT NOT NULL,
    notes TEXT,
    visit_date TIMESTAMP DEFAULT NOW()
);
