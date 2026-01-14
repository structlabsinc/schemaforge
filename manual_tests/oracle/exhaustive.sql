-- Oracle Exhaustive Coverage Test
CREATE TABLE customers (
    cust_id NUMBER PRIMARY KEY,
    name VARCHAR2(100),
    credit_limit NUMBER(10,2)
) 
PCTFREE 20 
PCTUSED 40 
TABLESPACE users 
STORAGE (INITIAL 64K NEXT 64K);

CREATE TABLE countries (
    country_id CHAR(2) PRIMARY KEY,
    country_name VARCHAR2(40)
) ORGANIZATION INDEX
  TABLESPACE users
  PCTFREE 5;

CREATE GLOBAL TEMPORARY TABLE session_logs (
    log_id NUMBER,
    msg VARCHAR2(4000)
) ON COMMIT PRESERVE ROWS;

CREATE SYNONYM cust_syn FOR customers;

CREATE PACKAGE emp_mgmt AS
  PROCEDURE hire_employee(name VARCHAR2);
END emp_mgmt;
CREATE PACKAGE emp_pkg AS END emp_pkg;
CREATE PACKAGE emp_pkg AS END emp_pkg;
