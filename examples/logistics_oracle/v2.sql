-- Target State: High Performance IOT & Partitioning

-- 1. Performance: Index Organized Table (IOT)
CREATE TABLE shipment_tracking (
    tracking_id VARCHAR2(50) NOT NULL,
    status_code VARCHAR2(10),
    location_id NUMBER(10),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_shipment PRIMARY KEY (tracking_id)
) ORGANIZATION INDEX
-- 2. Scalability: Hash Partitioning
PARTITION BY HASH (tracking_id) PARTITIONS 16;

-- 3. Security: VPD Policy (Stub for demonstration)
-- Note: Policies are typically PL/SQL, but represented here for schema tracking
