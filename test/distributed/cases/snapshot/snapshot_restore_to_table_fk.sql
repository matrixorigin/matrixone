drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists acc_test02;
create database acc_test02;
use acc_test02;
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
select count(*) from pri01;

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
insert into aff01 values (7654,'MARTIN','SALPESMAN',7698,'1981-09-28',1250,1400,30);
insert into aff01 values (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
insert into aff01 values (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
insert into aff01 values (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
insert into aff01 values (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
insert into aff01 values (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
insert into aff01 values (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
insert into aff01 values (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
insert into aff01 values (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
insert into aff01 values (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);
select count(*) from aff01;
-- @session

create snapshot snapshot_01 for account acc01;

-- @session:id=2&user=acc01:test_account&password=111
insert into acc_test02.pri01 values (50,'ACCOUNTING','NEW YORK');
insert into acc_test02.aff01 values (9000,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,50);
select count(*) from acc_test02.aff01;
-- @session

restore account acc01 from snapshot snapshot_01;
-- @session:id=2&user=acc01:test_account&password=111
show tables from acc_test02;
select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'acc_test02';
-- @session

restore account acc01 from snapshot snapshot_01;
-- @session:id=2&user=acc01:test_account&password=111
show tables from acc_test02;
select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'acc_test02';
-- @session

restore account acc01 from snapshot snapshot_01;
-- @session:id=2&user=acc01:test_account&password=111
show tables from acc_test02;
select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'acc_test02';
-- @session

restore account acc01 from snapshot snapshot_01;
-- @session:id=2&user=acc01:test_account&password=111
show tables from acc_test02;
select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'acc_test02';
-- @session

restore account acc01 from snapshot snapshot_01;
-- @session:id=2&user=acc01:test_account&password=111
show tables from acc_test02;
select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'acc_test02';
-- @session


drop account acc01;
drop snapshot snapshot_01;

CREATE DATABASE Company;
USE Company;

drop table if exists Departments;
CREATE TABLE Departments (
    DepartmentID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    ManagerID INT NULL
);

drop table if exists Employees;
CREATE TABLE Employees (
    EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
    FirstName VARCHAR(100) NOT NULL,
    LastName VARCHAR(100) NOT NULL,
    BirthDate DATE NULL,
    DepartmentID INT NULL,
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

drop table if exists Positions;
CREATE TABLE Positions (
    PositionID INT AUTO_INCREMENT PRIMARY KEY,
    Title VARCHAR(100) NOT NULL,
    DepartmentID INT NULL,
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

drop table if exists EmployeePositions;
CREATE TABLE EmployeePositions (
    EmployeeID INT NOT NULL,
    PositionID INT NOT NULL,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
    FOREIGN KEY (PositionID) REFERENCES Positions(PositionID),
    PRIMARY KEY (EmployeeID, PositionID)
);

drop table if exists Salaries;
CREATE TABLE Salaries (
    SalaryID INT AUTO_INCREMENT PRIMARY KEY,
    EmployeeID INT NOT NULL,
    PositionID INT NOT NULL,
    Salary DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
    FOREIGN KEY (PositionID) REFERENCES Positions(PositionID)
);

drop table if exists Benefits;
CREATE TABLE Benefits (
    BenefitID INT AUTO_INCREMENT PRIMARY KEY,
    EmployeeID INT NOT NULL,
    Benefit VARCHAR(100) NOT NULL,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);

drop table if exists TimeOff;
CREATE TABLE TimeOff (
    TimeOffID INT AUTO_INCREMENT PRIMARY KEY,
    EmployeeID INT NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);

drop table if exists PerformanceReviews;
CREATE TABLE PerformanceReviews (
    PerformanceReviewID INT AUTO_INCREMENT PRIMARY KEY,
    EmployeeID INT NOT NULL,
    ReviewDate DATE NOT NULL,
    ReviewerID INT NOT NULL,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
    FOREIGN KEY (ReviewerID) REFERENCES Employees(EmployeeID)
);

INSERT INTO Departments (Name, ManagerID) VALUES ('Research and Development', NULL);
INSERT INTO Departments (Name, ManagerID) VALUES ('Human Resources', NULL);
INSERT INTO Departments (Name, ManagerID) VALUES ('Sales', NULL);
INSERT INTO Departments (Name, ManagerID) VALUES ('Marketing', NULL);
INSERT INTO Departments (Name, ManagerID) VALUES ('Finance', NULL);
INSERT INTO Departments (Name, ManagerID) VALUES ('Legal', NULL);


INSERT INTO Employees (FirstName, LastName, BirthDate, DepartmentID) VALUES ('John', 'Doe', '1980-01-01', 1);
INSERT INTO Employees (FirstName, LastName, BirthDate, DepartmentID) VALUES ('Jane', 'Smith', '1981-02-02', 2);
INSERT INTO Employees (FirstName, LastName, BirthDate, DepartmentID) VALUES ('Alice', 'Jones', '1982-03-03', 3);
INSERT INTO Employees (FirstName, LastName, BirthDate, DepartmentID) VALUES ('Bob', 'Brown', '1983-04-04', 4);
INSERT INTO Employees (FirstName, LastName, BirthDate, DepartmentID) VALUES ('Charlie', 'White', '1984-05-05', 5);
INSERT INTO Employees (FirstName, LastName, BirthDate, DepartmentID) VALUES ('David', 'Black', '1985-06-06', 6);

INSERT INTO Positions (Title, DepartmentID) VALUES ('Software Engineer', 1);
INSERT INTO Positions (Title, DepartmentID) VALUES ('HR Specialist', 2);
INSERT INTO Positions (Title, DepartmentID) VALUES ('Sales Associate', 3);
INSERT INTO Positions (Title, DepartmentID) VALUES ('Marketing Specialist', 4);
INSERT INTO Positions (Title, DepartmentID) VALUES ('Accountant', 5);
INSERT INTO Positions (Title, DepartmentID) VALUES ('Attorney', 6);

INSERT INTO EmployeePositions (EmployeeID, PositionID) VALUES (1, 1);
INSERT INTO EmployeePositions (EmployeeID, PositionID) VALUES (2, 2);
INSERT INTO EmployeePositions (EmployeeID, PositionID) VALUES (3, 3);
INSERT INTO EmployeePositions (EmployeeID, PositionID) VALUES (4, 4);
INSERT INTO EmployeePositions (EmployeeID, PositionID) VALUES (5, 5);
INSERT INTO EmployeePositions (EmployeeID, PositionID) VALUES (6, 6);

INSERT INTO Salaries (EmployeeID, PositionID, Salary) VALUES (1, 1, 100000.00);
INSERT INTO Salaries (EmployeeID, PositionID, Salary) VALUES (2, 2, 50000.00);
INSERT INTO Salaries (EmployeeID, PositionID, Salary) VALUES (3, 3, 60000.00);
INSERT INTO Salaries (EmployeeID, PositionID, Salary) VALUES (4, 4, 70000.00);
INSERT INTO Salaries (EmployeeID, PositionID, Salary) VALUES (5, 5, 80000.00);
INSERT INTO Salaries (EmployeeID, PositionID, Salary) VALUES (6, 6, 90000.00);

INSERT INTO Benefits (EmployeeID, Benefit) VALUES (1, 'Health Insurance');
INSERT INTO Benefits (EmployeeID, Benefit) VALUES (2, 'Dental Insurance');
INSERT INTO Benefits (EmployeeID, Benefit) VALUES (3, 'Vision Insurance');
INSERT INTO Benefits (EmployeeID, Benefit) VALUES (4, '401k');
INSERT INTO Benefits (EmployeeID, Benefit) VALUES (5, 'Stock Options');
INSERT INTO Benefits (EmployeeID, Benefit) VALUES (6, 'Paid Time Off');

INSERT INTO TimeOff (EmployeeID, StartDate, EndDate) VALUES (1, '2020-01-01', '2020-01-02');
INSERT INTO TimeOff (EmployeeID, StartDate, EndDate) VALUES (2, '2020-02-02', '2020-02-03');
INSERT INTO TimeOff (EmployeeID, StartDate, EndDate) VALUES (3, '2020-03-03', '2020-03-04');
INSERT INTO TimeOff (EmployeeID, StartDate, EndDate) VALUES (4, '2020-04-04', '2020-04-05');
INSERT INTO TimeOff (EmployeeID, StartDate, EndDate) VALUES (5, '2020-05-05', '2020-05-06');
INSERT INTO TimeOff (EmployeeID, StartDate, EndDate) VALUES (6, '2020-06-06', '2020-06-07');

INSERT INTO PerformanceReviews (EmployeeID, ReviewDate, ReviewerID) VALUES (1, '2020-01-01', 2);
INSERT INTO PerformanceReviews (EmployeeID, ReviewDate, ReviewerID) VALUES (2, '2020-02-02', 3);
INSERT INTO PerformanceReviews (EmployeeID, ReviewDate, ReviewerID) VALUES (3, '2020-03-03', 4);
INSERT INTO PerformanceReviews (EmployeeID, ReviewDate, ReviewerID) VALUES (4, '2020-04-04', 5);
INSERT INTO PerformanceReviews (EmployeeID, ReviewDate, ReviewerID) VALUES (5, '2020-05-05', 6);
INSERT INTO PerformanceReviews (EmployeeID, ReviewDate, ReviewerID) VALUES (6, '2020-06-06', 1);

select * from Departments;
select * from Employees;
select * from Positions;
select * from EmployeePositions;
select * from Salaries;
select * from Benefits;
select * from TimeOff;
select * from PerformanceReviews;

CREATE DATABASE Projects;
USE Projects;

drop table if exists Projects;
CREATE TABLE Projects (
    ProjectID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Description TEXT,
    DepartmentID INT,
    FOREIGN KEY (DepartmentID) REFERENCES Company.Departments(DepartmentID)
);

drop table if exists ProjectMembers;
CREATE TABLE ProjectMembers (
    ProjectID INT,
    EmployeeID INT,
    FOREIGN KEY (ProjectID) REFERENCES Projects.Projects(ProjectID),
    FOREIGN KEY (EmployeeID) REFERENCES Company.Employees(EmployeeID),
    PRIMARY KEY (ProjectID, EmployeeID)
);

drop table if exists ProjectManagers;
CREATE TABLE ProjectManagers (
    ProjectID INT,
    EmployeeID INT,
    FOREIGN KEY (ProjectID) REFERENCES Projects.Projects(ProjectID),
    FOREIGN KEY (EmployeeID) REFERENCES Company.Employees(EmployeeID),
    PRIMARY KEY (ProjectID, EmployeeID)
);

INSERT INTO Projects (Name, Description, DepartmentID) VALUES ('Project 1', 'Description 1', 1);
INSERT INTO Projects (Name, Description, DepartmentID) VALUES ('Project 2', 'Description 2', 2);
INSERT INTO Projects (Name, Description, DepartmentID) VALUES ('Project 3', 'Description 3', 3);
INSERT INTO Projects (Name, Description, DepartmentID) VALUES ('Project 4', 'Description 4', 4);
INSERT INTO Projects (Name, Description, DepartmentID) VALUES ('Project 5', 'Description 5', 5);
INSERT INTO Projects (Name, Description, DepartmentID) VALUES ('Project 6', 'Description 6', 6);

INSERT INTO ProjectMembers (ProjectID, EmployeeID) VALUES (1, 1);
INSERT INTO ProjectMembers (ProjectID, EmployeeID) VALUES (2, 2);
INSERT INTO ProjectMembers (ProjectID, EmployeeID) VALUES (3, 3);
INSERT INTO ProjectMembers (ProjectID, EmployeeID) VALUES (4, 4);
INSERT INTO ProjectMembers (ProjectID, EmployeeID) VALUES (5, 5);
INSERT INTO ProjectMembers (ProjectID, EmployeeID) VALUES (6, 6);

INSERT INTO ProjectManagers (ProjectID, EmployeeID) VALUES (1, 1);
INSERT INTO ProjectManagers (ProjectID, EmployeeID) VALUES (2, 2);
INSERT INTO ProjectManagers (ProjectID, EmployeeID) VALUES (3, 3);
INSERT INTO ProjectManagers (ProjectID, EmployeeID) VALUES (4, 4);
INSERT INTO ProjectManagers (ProjectID, EmployeeID) VALUES (5, 5);
INSERT INTO ProjectManagers (ProjectID, EmployeeID) VALUES (6, 6);

select * from Projects;
select * from ProjectMembers;
select * from ProjectManagers;

CREATE DATABASE Payroll;
USE Payroll;

drop table if exists Salaries;
CREATE TABLE Salaries (
    EmployeeID INT,
    MonthlySalary DECIMAL(10, 2),
    FOREIGN KEY (EmployeeID) REFERENCES Company.Employees(EmployeeID),
    PRIMARY KEY (EmployeeID)
);

drop table if exists ProjectBonuses;
CREATE TABLE ProjectBonuses (
    ProjectID INT,
    EmployeeID INT,
    Bonus DECIMAL(10, 2),
    FOREIGN KEY (ProjectID) REFERENCES Projects.Projects(ProjectID),
    FOREIGN KEY (EmployeeID) REFERENCES Company.Employees(EmployeeID),
    PRIMARY KEY (ProjectID, EmployeeID)
);

drop table if exists DepartmentBudgets;
CREATE TABLE DepartmentBudgets (
    DepartmentID INT,
    Budget DECIMAL(15, 2),
    FOREIGN KEY (DepartmentID) REFERENCES Company.Departments(DepartmentID),
    PRIMARY KEY (DepartmentID)
);

INSERT INTO Salaries (EmployeeID, MonthlySalary) VALUES (1, 10000.00);
INSERT INTO Salaries (EmployeeID, MonthlySalary) VALUES (2, 20000.00);
INSERT INTO Salaries (EmployeeID, MonthlySalary) VALUES (3, 30000.00);
INSERT INTO Salaries (EmployeeID, MonthlySalary) VALUES (4, 40000.00);
INSERT INTO Salaries (EmployeeID, MonthlySalary) VALUES (5, 50000.00);
INSERT INTO Salaries (EmployeeID, MonthlySalary) VALUES (6, 60000.00);

INSERT INTO ProjectBonuses (ProjectID, EmployeeID, Bonus) VALUES (1, 1, 1000.00);
INSERT INTO ProjectBonuses (ProjectID, EmployeeID, Bonus) VALUES (2, 2, 2000.00);
INSERT INTO ProjectBonuses (ProjectID, EmployeeID, Bonus) VALUES (3, 3, 3000.00);
INSERT INTO ProjectBonuses (ProjectID, EmployeeID, Bonus) VALUES (4, 4, 4000.00);
INSERT INTO ProjectBonuses (ProjectID, EmployeeID, Bonus) VALUES (5, 5, 5000.00);
INSERT INTO ProjectBonuses (ProjectID, EmployeeID, Bonus) VALUES (6, 6, 6000.00);

INSERT INTO DepartmentBudgets (DepartmentID, Budget) VALUES (1, 100000.00);
INSERT INTO DepartmentBudgets (DepartmentID, Budget) VALUES (2, 200000.00);
INSERT INTO DepartmentBudgets (DepartmentID, Budget) VALUES (3, 300000.00);
INSERT INTO DepartmentBudgets (DepartmentID, Budget) VALUES (4, 400000.00);
INSERT INTO DepartmentBudgets (DepartmentID, Budget) VALUES (5, 500000.00);
INSERT INTO DepartmentBudgets (DepartmentID, Budget) VALUES (6, 600000.00);

select * from Salaries;
select * from ProjectBonuses;
select * from DepartmentBudgets;


drop snapshot if exists snapshot_01;
-- @ignore:1
show snapshots;
create snapshot snapshot_01 for account sys;

Drop database Payroll;
Drop database Projects;
Drop database Company;

restore account sys from snapshot snapshot_01;

select * from Company.Departments;
select * from Company.Employees;
select * from Company.Positions;
select * from Company.EmployeePositions;
select * from Company.Salaries;
select * from Company.Benefits;
select * from Company.TimeOff;
select * from Company.PerformanceReviews;

select * from Projects.Projects;
select * from Projects.ProjectMembers;
select * from Projects.ProjectManagers;

select * from Payroll.Salaries;
select * from Payroll.ProjectBonuses;
select * from Payroll.DepartmentBudgets;

drop database if exists Payroll;
drop database if exists Projects;
drop database if exists Company;

drop snapshot snapshot_01;
