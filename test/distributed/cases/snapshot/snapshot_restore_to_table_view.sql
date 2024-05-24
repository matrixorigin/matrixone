-- drop database
drop database if exists test01;
create database test01;
use test01;
drop table if exists EmployeeSalaries;
create table EmployeeSalaries (
          EmployeeID INT,
          EmployeeName VARCHAR(100),
          Salary DECIMAL(10, 2)
);
insert into EmployeeSalaries (EmployeeID, EmployeeName, Salary) VALUES
            (1, 'Alice', 70000),
            (2, 'Bob', 80000),
            (3, 'Charlie', 90000),
            (4, 'David', 65000),
            (5, 'Eva', 75000);
drop view if exists EmployeeSalaryRanking;
create view EmployeeSalaryRanking AS
select
    EmployeeID,
    EmployeeName,
    Salary,
    rank() over (order by Salary desc) as SalaryRank
from
    EmployeeSalaries;
select * from EmployeeSalaryRanking;

drop snapshot if exists sp06;
create snapshot sp06 for account sys;

drop database test01;

restore account sys from snapshot sp06;

select * from test01.EmployeeSalaries;
select * from test01.EmployeeSalaryRanking;

drop database if exists test01;
drop snapshot if exists sp06;


-- drop tables
drop database if exists test01;
create database test01;
use test01;
drop table if exists EmployeeSalaries;
create table EmployeeSalaries (
          EmployeeID INT,
          EmployeeName VARCHAR(100),
          Salary DECIMAL(10, 2)
);
insert into EmployeeSalaries (EmployeeID, EmployeeName, Salary) VALUES
            (1, 'Alice', 70000),
            (2, 'Bob', 80000),
            (3, 'Charlie', 90000),
            (4, 'David', 65000),
            (5, 'Eva', 75000);
drop view if exists EmployeeSalaryRanking;
create view EmployeeSalaryRanking AS
select
    EmployeeID,
    EmployeeName,
    Salary,
    rank() over (order by Salary desc) as SalaryRank
from
    EmployeeSalaries;
select * from EmployeeSalaryRanking;

drop snapshot if exists sp06;
create snapshot sp06 for account sys;

drop table EmployeeSalaries;
drop view EmployeeSalaryRanking;

restore account sys from snapshot sp06;

select * from test01.EmployeeSalaries;
select * from test01.EmployeeSalaryRanking;

drop database if exists test01;
drop snapshot if exists sp06;

-- multi view
drop database if exists test01;
create database test01;
use test01;
drop table if exists employees;
CREATE TABLE employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department_id INT,
    status VARCHAR(10)
);

drop table if exists departments;
CREATE TABLE departments (
    department_id INT AUTO_INCREMENT PRIMARY KEY,
    department_name VARCHAR(100)
);

drop table if exists orders;
CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT,
    order_date DATE,
    order_amount DECIMAL(10, 2)
);

drop table if exists sales_summary;
CREATE TABLE sales_summary (
    department_name VARCHAR(100),
    total_sales DECIMAL(10, 2)
);

INSERT INTO departments (department_name) VALUES ('Sales'), ('Engineering'), ('Human Resources');

INSERT INTO employees (first_name, last_name, department_id, status) VALUES
('John', 'Doe', 1, 'active'),
('Jane', 'Smith', 2, 'active'),
('Jim', 'Beam', 3, 'inactive');

INSERT INTO orders (employee_id, order_date, order_amount) VALUES
(1, '2024-01-15', 1500.00),
(1, '2024-02-10', 1200.00),
(2, '2024-03-01', 2000.00),
(3, '2024-04-01', 1000.00);

INSERT INTO sales_summary (department_name, total_sales) VALUES
('Sales', 2700.00),
('Engineering', 2000.00),
('Human Resources', 0.00);

select * from employees;
select * from departments;
select * from orders;
select * from sales_summary;

CREATE VIEW department_sales AS
SELECT 
    d.department_name,
    e.first_name,
    e.last_name,
    SUM(o.order_amount) AS total_sales
FROM 
    employees e
JOIN 
    departments d ON e.department_id = d.department_id
JOIN 
    orders o ON e.employee_id = o.employee_id
GROUP BY 
    d.department_name, e.first_name, e.last_name;

SELECT * FROM department_sales;

CREATE VIEW total_department_sales AS
SELECT 
    department_name,
    SUM(total_sales) AS department_total_sales
FROM 
    department_sales
GROUP BY 
    department_name;

SELECT * FROM total_department_sales;

CREATE VIEW combined_sales_view AS
SELECT 
    COALESCE(ds.department_name, ss.department_name) AS department_name,
    ds.total_sales AS individual_sales,
    ss.total_sales AS department_summary_sales
FROM 
    sales_summary ss
LEFT JOIN 
    department_sales ds ON ss.department_name = ds.department_name;

SELECT * FROM combined_sales_view;

drop snapshot if exists sp06;
-- @ignore:1
show snapshots;
create snapshot sp06 for account sys;

drop database test01;

restore account sys from snapshot sp06;

select * from test01.employees;
select * from test01.departments;
select * from test01.orders;
select * from test01.sales_summary;

select * from test01.department_sales;
select * from test01.total_department_sales;
select * from test01.combined_sales_view;

-- drop tables
drop table if exists employees;
drop table if exists departments;
drop table if exists orders;
drop table if exists sales_summary;

restore account sys from snapshot sp06;

select * from test01.employees;
select * from test01.departments;
select * from test01.orders;
select * from test01.sales_summary;

select * from test01.department_sales;
select * from test01.total_department_sales;
select * from test01.combined_sales_view;

drop database if exists test01;
drop snapshot if exists sp06;

