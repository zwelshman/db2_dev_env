-- Connect to the database (it should already exist from DBNAME env var)
CONNECT TO ${DBNAME};

-- Create a schema
CREATE SCHEMA MYSCHEMA;

-- Set current schema
SET SCHEMA MYSCHEMA;

-- Create your tables
CREATE TABLE EMPLOYEES (
    EMP_ID INTEGER NOT NULL PRIMARY KEY,
    FIRST_NAME VARCHAR(50) NOT NULL,
    LAST_NAME VARCHAR(50) NOT NULL,
    EMAIL VARCHAR(100),
    HIRE_DATE DATE,
    SALARY DECIMAL(10,2),
    DEPARTMENT VARCHAR(50)
);

CREATE TABLE DEPARTMENTS (
    DEPT_ID INTEGER NOT NULL PRIMARY KEY,
    DEPT_NAME VARCHAR(100) NOT NULL,
    LOCATION VARCHAR(100)
);

-- Insert sample data (optional)
INSERT INTO EMPLOYEES VALUES 
(1, 'John', 'Doe', 'john.doe@example.com', '2024-01-15', 75000.00, 'Engineering'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '2024-02-20', 82000.00, 'Marketing');

INSERT INTO DEPARTMENTS VALUES
(1, 'Engineering', 'Building A'),
(2, 'Marketing', 'Building B');

-- Grant privileges
GRANT ALL ON TABLE MYSCHEMA.EMPLOYEES TO PUBLIC;
GRANT ALL ON TABLE MYSCHEMA.DEPARTMENTS TO PUBLIC;

CONNECT RESET;
