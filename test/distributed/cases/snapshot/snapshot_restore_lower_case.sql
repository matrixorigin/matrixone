drop account if exists acc01;
create account acc01 ADMIN_NAME 'test_account' IDENTIFIED BY '111';
drop account if exists acc02;
create account acc02 admin_name 'test_account' identified by '111';
drop account if exists acc04;
create account acc04 admin_name 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
select @@lower_case_table_names;

set global lower_case_table_names = 0;
-- @session

-- @session:id=2&user=acc01:test_account&password=111
select @@lower_case_table_names;

create database test;
use test;
create table TT (c1 int);
insert into TT values(1);
create table tt(a1 int);
insert into tt values(2);
create table Tt(b1 int);
insert into Tt values(3);
create table tT(d1 int);
insert into tT values(4);
show create table TT;
show create table tt;
show create table Tt;
show create table tT;
show tables;
select * from TT;
select * from tt;
select * from Tt;
select * from tT;
-- @session

drop snapshot if exists sp01_restore_lower;
create snapshot sp01_restore_lower for account acc01;

-- @session:id=2&user=acc01:test_account&password=111
drop database test;
-- @session

restore account acc01 from snapshot sp01_restore_lower;

-- @session:id=2&user=acc01:test_account&password=111
use test;
show tables;
show create table TT;
show create table tt;
show create table Tt;
show create table tT;
select * from TT;
select * from tt;
select * from Tt;
select * from tT;
-- @session

restore account acc01 from snapshot sp01_restore_lower to account acc02;

-- @session:id=3&user=acc02:test_account&password=111
use test;
show tables;
show create table TT;
show create table tt;
show create table Tt;
show create table tT;
select * from TT;
select * from tt;
select * from Tt;
select * from tT;
drop database test;
-- @session



-- view
-- @session:id=2&user=acc01:test_account&password=111
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
-- @session

drop snapshot if exists sp02_restore_lower;
create snapshot sp02_restore_lower for account acc01;

-- @session:id=2&user=acc01:test_account&password=111
select @@lower_case_table_names;
drop database test02;
-- @session

restore account acc01 from snapshot sp02_restore_lower;

-- @session:id=2&user=acc01:test_account&password=111
use test02;
show tables;
show create table EmployeeDepartmentView;
-- @session

restore account acc01 from snapshot sp02_restore_lower to account acc04;

-- @session:id=4&user=acc04:test_account&password=111
use test02;
show tables;
show create table EmployeeDepartmentView;
set global lower_case_table_names = 0;
-- @session

-- @session:id=5&user=acc04:test_account&password=111
select @@lower_case_table_names;
use test02;
show tables;
show create table EmployeeDepartmentView;
-- @session

drop account if exists acc01;
drop account if exists acc02;
drop account if exists acc04;

drop snapshot if exists sp01_restore_lower;
drop snapshot if exists sp02_restore_lower;