CREATE TABLE employees (
    emp_id NUMBER(10) PRIMARY KEY,
    first_name VARCHAR2(50),
    last_name VARCHAR2(100), -- Modified length
    hire_date DATE,
    salary NUMBER(10, 2), -- Modified precision
    department_id NUMBER(5) -- Added column
);
