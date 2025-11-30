CREATE TABLE events (
    id NUMBER PRIMARY KEY,
    payload VARIANT,
    source STRING -- Added column
);
