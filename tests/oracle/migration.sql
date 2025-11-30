-- Migration Script for oracle
ALTER TABLE employees ADD COLUMN department_id NUMBER(5) -- Added column
;

ALTER TABLE employees MODIFY COLUMN last_name VARCHAR2(100);

ALTER TABLE employees MODIFY COLUMN salary NUMBER(10, 2);