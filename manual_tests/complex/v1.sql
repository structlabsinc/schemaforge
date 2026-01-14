-- Complex Schema Baseline
CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    name VARCHAR(100),
    dept_id INT REFERENCES departments(dept_id)
);

-- Cyclic dependency (optional addition later)
CREATE TABLE projects (
    project_id INT PRIMARY KEY,
    lead_id INT REFERENCES employees(emp_id)
);
