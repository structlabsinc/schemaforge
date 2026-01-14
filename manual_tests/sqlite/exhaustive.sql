-- SQLite Exhaustive Coverage Test
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    account_uuid TEXT UNIQUE,
    balance REAL DEFAULT 0.0,
    full_name TEXT,
    full_name_upper TEXT GENERATED ALWAYS AS (UPPER(full_name)) VIRTUAL
) STRICT;

CREATE TABLE fast_logs (
    id INTEGER PRIMARY KEY,
    msg TEXT
) WITHOUT ROWID;

CREATE VIEW active_accounts AS SELECT * FROM accounts WHERE balance > 0;
