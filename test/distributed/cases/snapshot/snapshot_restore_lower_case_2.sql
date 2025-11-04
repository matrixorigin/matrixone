drop account if exists a1;
create account a1 ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';

-- @session:id=1&user=a1:admin1&password=test123
-- default value is 1
select @@lower_case_table_names;
set global lower_case_table_names = 0;
-- @session

-- @session:id=2&user=a1:admin1&password=test123
-- it's 0 now
select @@lower_case_table_names;
drop database if exists test02;
create database test02;
use test02;
drop table if exists Departments;
drop table if exists Employees;
create table Departments (
 DepartmentID INT PRIMARY KEY,
 DepartmentName VARCHAR(255) NOT NULL
);

create table Employees (
EmployeeID INT PRIMARY KEY,
FirstName VARCHAR(255) NOT NULL,
LastName VARCHAR(255) NOT NULL,
DepartmentID INT,
foreign key (DepartmentID) REFERENCES Departments(DepartmentID)
);

insert into Departments (DepartmentID, DepartmentName) values
(1, 'Human Resources'),
(2, 'Engineering'),
(3, 'Marketing'),
(4, 'Sales'),
(5, 'Finance');

insert into Employees (EmployeeID, FirstName, LastName, DepartmentID) values
(101, 'John', 'Doe', 1),
(102, 'Jane', 'Smith', 2),
(103, 'Alice', 'Johnson', 3),
(104, 'Mark', 'Patterson', 4),
(105, 'David', 'Finley', 5);

drop view if exists EmployeeDepartmentView;
create view EmployeeDepartmentView as
select
    e.FirstName,
    e.LastName,
    d.DepartmentName
from
    Employees e
        inner join
    Departments d ON e.DepartmentID = d.DepartmentID;
select * from EmployeeDepartmentView;

drop snapshot if exists sp02_restore_lower;
create snapshot sp02_restore_lower for account a1;

select @@lower_case_table_names;
drop database test02;

restore account a1{snapshot="sp02_restore_lower"};

use test02;
show tables;

drop database if exists test02;
restore database test02{snapshot="sp02_restore_lower"};

use test02;
show tables;
-- @session

drop account if exists a1;

drop account if exists a1;
create account a1 ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';

-- @session:id=3&user=a1:admin1&password=test123
-- default value is 1
select @@lower_case_table_names;
set global lower_case_table_names = 0;
-- @session

-- @session:id=4&user=a1:admin1&password=test123
-- it's 0 now
select @@lower_case_table_names;
select @@lower_case_table_names;
drop database if exists test02;
create database test02;
use test02;
drop table if exists Departments;
drop table if exists Employees;
create table Departments (
 DepartmentID INT PRIMARY KEY,
 DepartmentName VARCHAR(255) NOT NULL
);

create table Employees (
EmployeeID INT PRIMARY KEY,
FirstName VARCHAR(255) NOT NULL,
LastName VARCHAR(255) NOT NULL,
DepartmentID INT,
foreign key (DepartmentID) REFERENCES Departments(DepartmentID)
);

insert into Departments (DepartmentID, DepartmentName) values
(1, 'Human Resources'),
(2, 'Engineering'),
(3, 'Marketing'),
(4, 'Sales'),
(5, 'Finance');

insert into Employees (EmployeeID, FirstName, LastName, DepartmentID) values
(101, 'John', 'Doe', 1),
(102, 'Jane', 'Smith', 2),
(103, 'Alice', 'Johnson', 3),
(104, 'Mark', 'Patterson', 4),
(105, 'David', 'Finley', 5);

drop view if exists EmployeeDepartmentView;
create view EmployeeDepartmentView as
select
    e.FirstName,
    e.LastName,
    d.DepartmentName
from
    Employees e
        inner join
    Departments d ON e.DepartmentID = d.DepartmentID;
select * from EmployeeDepartmentView;

show tables;
-- @session

drop snapshot if exists sp02_restore_lower;
create snapshot sp02_restore_lower for account a1;

-- @session:id=4&user=a1:admin1&password=test123
select @@lower_case_table_names;
drop database test02;
-- @session

restore account a1{snapshot="sp02_restore_lower"};

-- @session:id=4&user=a1:admin1&password=test123
use test02;
show tables;
-- @session

drop account if exists a1;
drop snapshot if exists sp02_restore_lower;
