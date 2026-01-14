-- Complex Schema Target
CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    name VARCHAR(100),
    manager_id INT -- To be linked to employees
);

CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    name VARCHAR(100),
    dept_id INT REFERENCES departments(dept_id),
    mentor_id INT REFERENCES employees(emp_id) -- Self-reference
);

ALTER TABLE departments ADD CONSTRAINT fk_dept_mgr 
FOREIGN KEY (manager_id) REFERENCES employees(emp_id);

CREATE TABLE projects (
    project_id INT PRIMARY KEY,
    name VARCHAR(100),
    lead_id INT REFERENCES employees(emp_id)
);
