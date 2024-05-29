drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
-- create simple view
drop database if exists test;
create database test;
use test;
drop table if exists table01;
create table table01 (col1 int, col2 decimal(6), col3 varchar(30));
insert into table01 values (1, null, 'database');
insert into table01 values (2, 38291.32132, 'database');
insert into table01 values (3, null, 'database management system');
insert into table01 values (4, 10, null);
insert into table01 values (1, -321.321, null);
insert into table01 values (2, -1, null);
select count(*) from table01;

drop view if exists v01;
create view v01 as select * from table01;
show create view v01;
select * from v01;
drop view if exists v02;
create view v02 as select col1, col2 from table01;
show create view v02;
select * from v02;
-- @session

drop snapshot if exists sp100;
create snapshot sp100 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
use test;
delete from table01 where col1 = 1;
select * from v01;
select * from v02;
drop view v01;
insert into v02 values (100, null, 'database');

drop view if exists v03;
select * from v01;
select * from v02;
select * from v03;
-- @session

restore account acc01 from snapshot sp100 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test;
show create view v01;
select * from v01;
show create view v02;
select * from v02;
drop view v01;
drop view v02;
drop table table01;
drop database test;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
use test;
drop view v02;
drop table table01;
-- @session
drop snapshot sp100;




-- sys account restore to account: create v01, create v02, create snapshot sp02, drop v01, restore
-- @session:id=1&user=acc01:test_account&password=111
use test;
drop table if exists pri01;
create table pri01(
                      deptno int unsigned comment '部门编号',
                      dname varchar(15) comment '部门名称',
                      loc varchar(50)  comment '部门所在位置',
                      primary key(deptno)
) comment='部门表';

insert into pri01 values (10,'ACCOUNTING','NEW YORK');
insert into pri01 values (20,'RESEARCH','DALLAS');
insert into pri01 values (30,'SALES','CHICAGO');
insert into pri01 values (40,'OPERATIONS','BOSTON');

drop table if exists aff01;
create table aff01(
                      empno int unsigned auto_increment COMMENT '雇员编号',
                      ename varchar(15) comment '雇员姓名',
                      job varchar(10) comment '雇员职位',
                      mgr int unsigned comment '雇员对应的领导的编号',
                      hiredate date comment '雇员的雇佣日期',
                      sal decimal(7,2) comment '雇员的基本工资',
                      comm decimal(7,2) comment '奖金',
                      deptno int unsigned comment '所在部门',
                      primary key(empno),
                      constraint `c1` foreign key (deptno) references pri01 (deptno)
);

insert into aff01 values (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
insert into aff01 values (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
insert into aff01 values (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
insert into aff01 values (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
insert into aff01 values (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
insert into aff01 values (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
insert into aff01 values (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);

drop view if exists v04;
create view v04 as select avg(sal) from aff01 group by mgr;
show create view v04;
select * from v04;
drop view if exists v05;
create view v05 as select * from v04;
select * from v05;
-- @session

drop snapshot if exists sp02;
create snapshot sp02 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
use test;
drop view v04;
select * from v04;
select * from v05;
-- @session

restore account acc01 from snapshot sp02  to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test;
select * from v04;
select * from v05;
drop view v04;
drop view v05;
drop table aff01;
drop table pri01;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop view v05;
drop table aff01;
drop table pri01;
-- @session
drop snapshot sp02;




-- table and table join
-- @session:id=1&user=acc01:test_account&password=111
use test;
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

drop snapshot if exists sp05;
create snapshot sp05 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
use test;
drop table Employees;
select * from EmployeeDepartmentView;
-- @session

restore account acc01 from snapshot sp05 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test;
select * from EmployeeDepartmentView;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop view EmployeeDepartmentView;
select * from EmployeeDepartmentView;
-- @session

restore account acc01 from snapshot sp05 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test;
select * from EmployeeDepartmentView;
drop view EmployeeDepartmentView;
drop table Employees;
drop table Departments;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
use test;
drop table Departments;
-- @session
drop snapshot sp05;




-- view and view join
-- @session:id=1&user=acc01:test_account&password=111
use test;
drop table if exists departments;
create table departments (
                             department_id INT PRIMARY KEY,
                             department_name VARCHAR(100)
);

insert into departments (department_id, department_name)
values (1, 'HR'),
       (2, 'Engineering');

drop table if exists employees;
create table employees (
                           employee_id INT PRIMARY KEY,
                           first_name VARCHAR(50),
                           last_name VARCHAR(50),
                           department_id INT,
                           FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

insert into employees values
                          (1, 'John', 'Doe', 1),
                          (2, 'Jane', 'Smith', 2),
                          (3, 'Bob', 'Johnson', 1);

drop view if exists employee_view;
create view employee_view as select employee_id, first_name, last_name, department_id from employees;

drop view if exists department_view;
create view department_view as select department_id, department_name from departments;

drop view if exists employee_with_department_view;
create view employee_with_department_view as
select e.employee_id, e.first_name, e.last_name, d.department_name
from employee_view e JOIN department_view d ON e.department_id = d.department_id;

select * from employee_view;
select * from department_view;
select * from employee_with_department_view;
-- @session

drop snapshot if exists sp04;
create snapshot sp04 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
use test;
drop view employee_view;
drop view department_view;
select * from employee_with_department_view;
-- @session

restore account acc01 from snapshot sp04 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test;
select * from employee_view;
select * from department_view;
select * from employee_with_department_view;
drop table employees;
truncate departments;
select * from employee_view;
-- @session

restore account acc01 from snapshot sp04 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test;
select * from employee_view;
select * from department_view;
select * from employee_with_department_view;
drop view employee_view;
drop view department_view;
drop view employee_with_department_view;
drop table employees;
drop table departments;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop table employees;
drop table departments;
drop view employee_with_department_view;
-- @session
drop snapshot sp04;



-- view in partition table
-- @session:id=1&user=acc01:test_account&password=111
use test;
drop table if exists partition01;
create table partition01 (
                             emp_no      int             not null,
                             birth_date  date            not null,
                             first_name  varchar(14)     not null,
                             last_name   varchar(16)     not null,
                             gender      varchar(5)      not null,
                             hire_date   date            not null,
                             primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

insert into partition01 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                               (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20');

drop view if exists view01;
create view view01 as select * from partition01;
select * from view01;
-- @session

drop snapshot if exists sp05;
create snapshot sp05 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
use test;
insert into partition01 values (9003,'1999-02-20', 'BOB', 'DOCTOR', 'F', '2009-02-20');
select * from view01;
-- @session

drop snapshot if exists sp06;
create snapshot sp06 for account acc01;
restore account acc01 from snapshot sp05 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test;
select * from view01;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
use test;
drop table partition01;
select * from view01;
-- @session

restore account acc01 from snapshot sp06 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test;
select * from view01;
drop view view01;
drop table partition01;
drop database test;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop database test;
-- @session
drop snapshot sp06;
drop snapshot sp05;




-- drop database, restore view
-- @session:id=1&user=acc01:test_account&password=111
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
-- @session

drop snapshot if exists sp06;
create snapshot sp06 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
drop database test01;
select * from test01.EmployeeSalaryRanking;
-- @session

restore account acc01 from snapshot sp06 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
select * from test01.EmployeeSalaryRanking;
drop database test01;
-- @session
drop snapshot sp06;




-- @bvt:issue#16346
-- create table1 and view, create table2 and view, drop table1 then restore
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test02;
create database test02;
use test02;
drop table if exists students;
create table Students (
                          StudentID INT PRIMARY KEY auto_increment,
                          Name VARCHAR(100),
                          Grade DECIMAL(3, 2)
);

drop table if exists Courses;
create table Courses (
                         CourseID INT PRIMARY KEY,
                         Title VARCHAR(100),
                         Teacher VARCHAR(100)
);
insert into Students (StudentID, Name, Grade) VALUES
                          (1, 'Alice', 3.5),
                          (2, 'Bob', 3.0),
                          (3, 'Charlie', 3.7);

insert into Courses (CourseID, Title, Teacher) VALUES
                           (101, 'Mathematics', 'Mr. Smith'),
                           (102, 'Physics', 'Dr. Johnson'),
                           (103, 'Chemistry', 'Ms. Lee');

drop table if exists Enrollments;
create table Enrollments (
                             StudentID INT,
                             CourseID INT,
                             EnrollmentDate DATE,
                             PRIMARY KEY (StudentID, CourseID),
                             FOREIGN KEY (StudentID) REFERENCES Students(StudentID),
                             FOREIGN KEY (CourseID) REFERENCES Courses(CourseID)
);
insert into Enrollments (StudentID, CourseID, EnrollmentDate) VALUES
                                  (1, 101, '2024-01-10'),
                                  (2, 102, '2024-01-15'),
                                  (1, 103, '2024-01-20'),
                                  (3, 101, '2024-02-01');

drop view if exists StudentCoursesView;
create view StudentCoursesView as
select
    s.Name as StudentName,
    c.Title as CourseTitle,
    c.Teacher,
    e.EnrollmentDate
from
    Students s
        join
    Enrollments e on s.StudentID = e.StudentID
        join
    Courses c on e.CourseID = c.CourseID
order by
    s.Name, c.Title;
select * from StudentCoursesView;
-- @session

drop snapshot if exists sp07;
create snapshot sp07 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
use test02;
drop table Enrollments;
drop table students;
select * from StudentCoursesView;
-- @session

restore account acc01 database test02 table students from snapshot sp07 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test02;
select * from Students;
select * from Enrollments;
select * from StudentCoursesView;
-- @session

restore account acc01 database test02 table Enrollments from snapshot sp07 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
use test02;
select * from Enrollments;
select * from StudentCoursesView;
show create view StudentCoursesView;
drop view StudentCoursesView;
drop table Enrollments;
drop table students;
drop table Courses;
drop snapshot sp07;
drop database test02;
-- @session
-- @session:id=1&user=acc01:test_account&password=111
drop database test02;
-- @session
-- @bvt:issue




-- single table, multi table
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test02;
create database test02;
use test02;
drop table if exists employees;
create table employees (
                           id INT PRIMARY KEY,
                           name VARCHAR(255),
                           department VARCHAR(255),
                           salary DECIMAL(10, 2),
                           hire_date DATE
);
drop view if exists employees_view;
drop view if exists it_employees_view;
drop view if exists employees_by_department_view;
drop view if exists employees_by_salary_view;
drop view if exists avg_salary_per_department_view;

create view employees_view AS SELECT * FROM employees;
create view it_employees_view AS SELECT * FROM employees WHERE department = 'IT';
create view employees_by_department_view AS
select name, department
from employees
order by department;

create view employees_by_salary_view AS
select name, salary
from employees
order by salary desc;

create view avg_salary_per_department_view AS
select department, avg(salary) as avg_salary
from employees
group by department;
-- @session

drop snapshot if exists sp10;
create snapshot sp10 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
use test02;
drop database test02;
-- @session

restore account acc01 from snapshot sp10 to account acc02;
-- @session:id=2&user=acc02:test_account&password=111
use test02;
show create view employees_view;
show create view it_employees_view;
show create view employees_by_department_view;
show create view employees_by_salary_view;
show create view avg_salary_per_department_view;
drop view if exists employees_view;
drop view if exists it_employees_view;
drop view if exists employees_by_department_view;
drop view if exists employees_by_salary_view;
drop view if exists avg_salary_per_department_view;
drop table employees;
drop database test02;
-- @session
drop snapshot sp10;




-- multi db, multi table
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test03;
drop database if exists test04;
drop database if exists test05;

create database test03;
create database test04;
create database test05;

use test03;
drop table if exists departments;
create table departments (
                             department_id INT PRIMARY KEY,
                             department_name VARCHAR(100)
);

insert into departments (department_id, department_name)
values (1, 'HR'),
       (2, 'Engineering');

use test04;
drop table if exists employees;
create table employees (
                           employee_id INT PRIMARY KEY,
                           first_name VARCHAR(50),
                           last_name VARCHAR(50),
                           department_id INT,
                           FOREIGN KEY (department_id) REFERENCES test03.departments(department_id)
);

insert into employees values
                          (1, 'John', 'Doe', 1),
                          (2, 'Jane', 'Smith', 2),
                          (3, 'Bob', 'Johnson', 1);

use test04;
drop view if exists employee_view;
create view employee_view as select employee_id, first_name, last_name, department_id from test04.employees;
select * from employee_view;

use test03;
drop view if exists department_view;
create view department_view as select department_id, department_name from test03.departments;
select * from department_view;

use test05;
drop view if exists employee_with_department_view;
create view employee_with_department_view as
select e.employee_id, e.first_name, e.last_name, d.department_name
from test04.employee_view e join test03.department_view d on e.department_id = d.department_id;
select * from employee_with_department_view;
-- @session

drop snapshot if exists sp100;
create snapshot sp100 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
drop database test04;
select * from test04.employee_view;
select * from test03.department_view;
select * from test05.employee_with_department_view;
-- @session

drop snapshot if exists sp101;
create snapshot sp101 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
drop database test03;
drop database test05;
select * from test04.employee_view;
select * from test03.department_view;
select * from test05.employee_with_department_view;
-- @session

restore account acc01 from snapshot sp100 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
select * from test04.employee_view;
select * from test03.department_view;
select * from test05.employee_with_department_view;

drop view test03.department_view;
drop view test05.employee_with_department_view;
drop database test04;
drop database test03;
drop database test05;
-- @session
drop snapshot sp100;
drop snapshot sp101;
drop account acc01;
drop account acc02;