CREATE TABLE devices (
    id INTEGER PRIMARY KEY,
    name TEXT,
    config BLOB,
    last_seen REAL,
    location TEXT -- Added column
);

-- Dropped column 'config' (Checking if generator emits DROP COLUMN)
