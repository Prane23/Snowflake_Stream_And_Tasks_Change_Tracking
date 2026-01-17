--1. CREATE DATABASE WAREHOUSE

CREATE DATABASE IF NOT EXISTS HR_DATALAKE;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WAREHOUSE_SIZE = 'XSMALL';

--2 CREATE STAGING SCHEMAS
USE DATABASE HR_DATALAKE;
CREATE SCHEMA IF NOT EXISTS STAGING;


--3 CREATE PRODUCTION SCHEMAS
USE DATABASE HR_DATALAKE;
CREATE SCHEMA IF NOT EXISTS PRODUCTION;


-- 2 .Create Base Tables (Simple Tables, NOT Dynamic)  employee_detail
CREATE OR REPLACE TABLE HR_DATALAKE.STAGING.EMPLOYEE_DETAIL (

    EMPLOYEE_ID NUMBER,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    HIRE_DATE DATE,
    DEPARTMENT_NAME STRING,
    JOB_TITLE STRING,
    COST_CENTER STRING,
    LOCATION STRING,
    MANAGER_NAME STRING,
    EMPLOYMENTSTATUS STRING,
    ISACTIVE BOOLEAN
);



INSERT INTO HR_DATALAKE.STAGING.EMPLOYEE_DETAIL (
    EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DATE, DEPARTMENT_NAME, JOB_TITLE, COST_CENTER, LOCATION, MANAGER_NAME, EMPLOYMENTSTATUS, ISACTIVE
) VALUES
(1001, 'Alice', 'Johnson', 'alice.johnson@corp.com', '2020-03-15', 'HR', 'HR Specialist', 'CC101', 'San Francisco', 'Robert Smith', 'Full-Time', TRUE),
(1002, 'Brian', 'Lee', 'brian.lee@corp.com', '2019-07-22', 'Finance', 'Financial Analyst', 'CC102', 'New York', 'Susan Clark', 'Full-Time', TRUE),
(1003, 'Carla', 'Gomez', 'carla.gomez@corp.com', '2021-01-10', 'IT', 'Systems Engineer', 'CC103', 'Austin', 'David Brown', 'Full-Time', TRUE),
(1004, 'Daniel', 'Patel', 'daniel.patel@corp.com', '2018-11-05', 'Marketing', 'Marketing Manager', 'CC104', 'Chicago', 'Emily Davis', 'Full-Time', TRUE),
(1005, 'Emma', 'Wright', 'emma.wright@corp.com', '2022-06-18', 'HR', 'Recruiter', 'CC101', 'Los Angeles', 'Robert Smith', 'Full-Time', TRUE),
(1006, 'Frank', 'Chen', 'frank.chen@corp.com', '2020-09-30', 'Finance', 'Accountant', 'CC102', 'Seattle', 'Susan Clark', 'Full-Time', TRUE),
(1007, 'Grace', 'Miller', 'grace.miller@corp.com', '2021-12-12', 'IT', 'Data Analyst', 'CC103', 'Denver', 'David Brown', 'Full-Time', TRUE),
(1008, 'Henry', 'Lopez', 'henry.lopez@corp.com', '2017-04-25', 'Marketing', 'Content Strategist', 'CC104', 'Miami', 'Emily Davis', 'Full-Time', TRUE),
(1009, 'Irene', 'Carter', 'irene.carter@corp.com', '2023-02-14', 'HR', 'HR Coordinator', 'CC101', 'San Diego', 'Robert Smith', 'Full-Time', TRUE),
(1010, 'Jack', 'Wilson', 'jack.wilson@corp.com', '2019-08-19', 'Finance', 'Payroll Specialist', 'CC102', 'Boston', 'Susan Clark', 'Full-Time', TRUE),
(1011, 'Kelly', 'Adams', 'kelly.adams@corp.com', '2020-05-07', 'IT', 'Cloud Architect', 'CC103', 'Dallas', 'David Brown', 'Full-Time', TRUE),
(1012, 'Liam', 'Brooks', 'liam.brooks@corp.com', '2021-03-29', 'Marketing', 'SEO Analyst', 'CC104', 'Atlanta', 'Emily Davis', 'Full-Time', TRUE),
(1013, 'Mia', 'Rivera', 'mia.rivera@corp.com', '2018-10-11', 'HR', 'Benefits Specialist', 'CC101', 'Phoenix', 'Robert Smith', 'Full-Time', TRUE),
(1014, 'Noah', 'Evans', 'noah.evans@corp.com', '2022-01-20', 'Finance', 'Budget Analyst', 'CC102', 'Portland', 'Susan Clark', 'Full-Time', TRUE),
(1015, 'Olivia', 'Foster', 'olivia.foster@corp.com', '2019-06-03', 'IT', 'Security Engineer', 'CC103', 'Houston', 'David Brown', 'Full-Time', TRUE),
(1016, 'Paul', 'Gray', 'paul.gray@corp.com', '2020-08-15', 'Marketing', 'Social Media Manager', 'CC104', 'Orlando', 'Emily Davis', 'Part-Time', FALSE),
(1017, 'Quinn', 'Hughes', 'quinn.hughes@corp.com', '2021-11-09', 'HR', 'HR Generalist', 'CC101', 'Sacramento', 'Robert Smith', 'Full-Time', TRUE),
(1018, 'Ruby', 'James', 'ruby.james@corp.com', '2017-02-28', 'Finance', 'Tax Specialist', 'CC102', 'Minneapolis', 'Susan Clark', 'Full-Time', TRUE),
(1019, 'Sam', 'King', 'sam.king@corp.com', '2023-04-04', 'IT', 'DevOps Engineer', 'CC103', 'Salt Lake City', 'David Brown', 'Full-Time', TRUE),
(1020, 'Tina', 'Lewis', 'tina.lewis@corp.com', '2018-09-17', 'Marketing', 'Brand Manager', 'CC104', 'Charlotte', 'Emily Davis', 'Full-Time', TRUE);



Select * from HR_DATALAKE.STAGING.EMPLOYEE_DETAIL


-- Lets create steram object to track the  chnages in EMPLOYEE_DETAIL table  
CREATE OR REPLACE STREAM EMPLOYEE_DETAIL_STREAM ON TABLE EMPLOYEE_DETAIL 

SELECT * FROM EMPLOYEE_DETAIL_STREAM



INSERT INTO HR_DATALAKE.STAGING.EMPLOYEE_DETAIL (
    EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DATE, DEPARTMENT_NAME, JOB_TITLE, COST_CENTER, LOCATION, MANAGER_NAME, EMPLOYMENTSTATUS, ISACTIVE
) VALUES
(1021, 'Victor', 'Nelson', 'victor.nelson@corp.com', '2024-07-10', 'IT', 'Machine Learning Engineer', 'CC103', 'San Jose', 'David Brown', 'Full-Time', TRUE);

INSERT INTO HR_DATALAKE.STAGING.EMPLOYEE_DETAIL (
    EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DATE, DEPARTMENT_NAME, JOB_TITLE, COST_CENTER, LOCATION, MANAGER_NAME, EMPLOYMENTSTATUS, ISACTIVE
) VALUES
(1022, 'Sophia', 'Martinez', 'sophia.martinez@corp.com', '2023-11-03', 'Finance', 'Risk Analyst', 'CC102', 'Philadelphia', 'Susan Clark', 'Full-Time', TRUE);
