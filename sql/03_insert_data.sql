--3. Insert data
INSERT INTO HR_DATALAKE.STAGING.EMPLOYEE_DETAIL (
    EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DATE, DEPARTMENT_NAME, JOB_TITLE, COST_CENTER, LOCATION, MANAGER_NAME, EMPLOYMENTSTATUS, ISACTIVE
) VALUES
(1001, 'Alice', 'Johnson', 'alice.johnson@corp.com', '2020-03-15', 'HR', 'HR Specialist', 'CC101', 'San Francisco', 'Robert Smith', 'Full-Time', TRUE),
(1002, 'Brian', 'Lee', 'brian.lee@corp.com', '2019-07-22', 'Finance', 'Financial Analyst', 'CC102', 'New York', 'Susan Clark', 'Full-Time', TRUE),
(1003, 'Carla', 'Gomez', 'carla.gomez@corp.com', '2021-01-10', 'IT', 'Systems Engineer', 'CC103', 'Austin', 'David Brown', 'Full-Time', TRUE),
(1004, 'Daniel', 'Patel', 'daniel.patel@corp.com', '2018-11-05', 'Marketing', 'Marketing Manager', 'CC104', 'Chicago', 'Emily Davis', 'Full-Time', TRUE),
(1005, 'Emma', 'Wright', 'emma.wright@corp.com', '2022-06-18', 'HR', 'Recruiter', 'CC101', 'Los Angeles', 'Robert Smith', 'Full-Time', TRUE);

SELECT * FROM HR_DATALAKE.STAGING.EMPLOYEE_DETAIL