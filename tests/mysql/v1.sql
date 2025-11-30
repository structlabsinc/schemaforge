CREATE TABLE users (
    id INT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    bio TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    settings JSON,
    status ENUM('active', 'inactive') DEFAULT 'active'
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    total DECIMAL(10, 2),
    order_date DATE
);

CREATE TABLE legacy_data (
    id INT,
    data BLOB
);
