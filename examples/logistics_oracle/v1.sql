-- Initial State: Standard Heap Table
CREATE TABLE shipment_tracking (
    tracking_id VARCHAR2(50) NOT NULL,
    status_code VARCHAR2(10),
    location_id NUMBER(10),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_shipment PRIMARY KEY (tracking_id)
);
