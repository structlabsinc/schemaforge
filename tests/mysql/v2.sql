CREATE TABLE users (
    id INT PRIMARY KEY,
    username VARCHAR(100) NOT NULL, -- Modified type
    bio TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    settings JSON,
    status ENUM('active', 'inactive', 'banned') DEFAULT 'active', -- Modified type (enum)
    last_login TIMESTAMP -- Added column
);

-- Dropped table legacy_data

CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    total DECIMAL(12, 2), -- Modified precision
    order_date DATE,
    status VARCHAR(20) DEFAULT 'pending' -- Added column
);

CREATE TABLE products ( -- New table
    id INT PRIMARY KEY,
    name VARCHAR(255),
    price DECIMAL(10, 2)
);
