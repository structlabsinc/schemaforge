-- SaaS V2: High-Scale Partitioning
-- Dialect: MySQL
--
-- Changes:
-- 1. Partition 'orders' table by YEAR (range partitioning) for performance.
-- 2. Add 'sharding_key' (region_id) to users for future sharding.
-- 3. Add Fulltext index for search.

CREATE TABLE users (
    user_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    region_id INT NOT NULL DEFAULT 1, -- New Sharding Key
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE INDEX idx_region ON users(region_id);

-- Partitioned Orders Table
-- Note: Partitioning usually requires the partition key to be part of the PK.
CREATE TABLE orders (
    order_id BIGINT UNSIGNED AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status ENUM('pending', 'paid', 'shipped') DEFAULT 'pending',
    order_date DATETIME NOT NULL, -- Key for partitioning
    
    PRIMARY KEY (order_id, order_date),
    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(user_id)
) ENGINE=InnoDB
PARTITION BY RANGE (YEAR(order_date)) (
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Fulltext Search
CREATE FULLTEXT INDEX ft_user_email ON users(email);
