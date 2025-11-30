CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku VARCHAR(20),
    stock INTEGER,
    last_updated TIMESTAMP -- Added column
);
