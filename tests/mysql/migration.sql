-- Migration Script for mysql
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    price DECIMAL(10, 2)
);

DROP TABLE legacy_data;

ALTER TABLE users ADD COLUMN last_login TIMESTAMP;

ALTER TABLE users MODIFY COLUMN username VARCHAR(100);

ALTER TABLE users MODIFY COLUMN status ENUM('active', 'inactive', 'banned');

ALTER TABLE orders ADD COLUMN status VARCHAR(20);

ALTER TABLE orders MODIFY COLUMN total DECIMAL(12, 2);