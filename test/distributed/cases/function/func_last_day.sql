SELECT LAST_DAY('2024-07-01');
SELECT LAST_DAY('2024-02-15');
-- @ignore:0
SELECT LAST_DAY(CURDATE());
-- @ignore:0
SELECT LAST_DAY(NOW());
-- @ignore:0
SELECT LAST_DAY(DATE_SUB('2024-07-01', INTERVAL 1 MONTH)) AS last_day_of_previous_month;

create database abc;
use abc;
CREATE TABLE employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    hire_date DATE
);

-- 插入员工数据
INSERT INTO employees (first_name, last_name, hire_date) VALUES
('John', 'Doe', LAST_DAY(DATE_SUB('2024-07-01', INTERVAL 1 MONTH))),
('Jane', 'Smith', LAST_DAY(DATE_SUB('2024-06-01', INTERVAL 1 MONTH))),
('Alice', 'Johnson', LAST_DAY(DATE_SUB('2024-05-01', INTERVAL 1 MONTH))),
('Bob', 'Brown', LAST_DAY(DATE_SUB('2024-04-01', INTERVAL 1 MONTH)));

SELECT LAST_DAY(hire_date) from employees;
SELECT LAST_DAY(DATE_SUB(hire_date, INTERVAL 1 MONTH)) from employees AS last_day_of_previous_month;

drop database abc;

SELECT LAST_DAY("2017-02-10 09:34:00");
SELECT LAST_DAY('2003-02-05');
SELECT LAST_DAY('2004-02-05');
SELECT LAST_DAY('2004-01-01 01:01:01');

SELECT LAST_DAY('2024-02-01 23:01:61');
SELECT LAST_DAY('2024-02-01 24:01:01');
SELECT LAST_DAY('2024-02-01 25:01:01');
SELECT LAST_DAY('2024-02-01 23:61:01');
