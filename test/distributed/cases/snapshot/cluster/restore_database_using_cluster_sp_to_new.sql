drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
insert into test_snapshot_read (a) values(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from snapshot_read.test_snapshot_read;

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

select count(*) from snapshot_read.users;

CREATE TABLE students (
    student_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    age INT NOT NULL
);

INSERT INTO students (name, age) VALUES ('Alice', 20), ('Bob', 21), ('Charlie', 22), ('Diana', 23), ('Emily', 24);
INSERT INTO students (name, age) VALUES ('Francis', 25), ('Grace', 26), ('Henry', 27), ('Isabella', 28), ('Jack', 29);
INSERT INTO students (name, age) VALUES ('Katherine', 30), ('Liam', 31), ('Mia', 32), ('Noah', 33), ('Olivia', 34);
INSERT INTO students (name, age) VALUES ('Penelope', 35), ('Quinn', 36), ('Ryan', 37), ('Sophia', 38), ('Thomas', 39);
INSERT INTO students (name, age) VALUES ('Ursula', 40), ('Victor', 41), ('Wendy', 42), ('Xander', 43), ('Yvonne', 44);
INSERT INTO students (name, age) VALUES ('Zachary', 45), ('Ava', 46), ('Benjamin', 47), ('Charlotte', 48), ('Daniel', 49);
INSERT INTO students (name, age) VALUES ('Ella', 50), ('Finn', 51), ('Gabriella', 52), ('Henry', 53), ('Isabella', 54);
INSERT INTO students (name, age) VALUES ('Jack', 55), ('Katherine', 56), ('Liam', 57), ('Mia', 58), ('Noah', 59);
INSERT INTO students (name, age) VALUES ('Olivia', 60), ('Penelope', 61), ('Quinn', 62), ('Ryan', 63), ('Sophia', 64);
INSERT INTO students (name, age) VALUES ('Thomas', 65), ('Ursula', 66), ('Victor', 67), ('Wendy', 68), ('Xander', 69);

select count(*) from snapshot_read.students;

create database if not exists test_snapshot_restore;
use test_snapshot_restore;

create table test_restore (a int);
insert into test_restore (a) values(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from test_snapshot_restore.test_restore;

CREATE TABLE test_restore_2 (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    age INT NOT NULL
);
insert into test_restore_2 (name, age) values('Alice', 20), ('Bob', 21), ('Charlie', 22), ('Diana', 23), ('Emily', 24);
insert into test_restore_2 (name, age) values('Francis', 25), ('Grace', 26), ('Henry', 27), ('Isabella', 28), ('Jack', 29);
insert into test_restore_2 (name, age) values('Katherine', 30), ('Liam', 31), ('Mia', 32), ('Noah', 33), ('Olivia', 34);
insert into test_restore_2 (name, age) values('Penelope', 35), ('Quinn', 36), ('Ryan', 37), ('Sophia', 38), ('Thomas', 39);
insert into test_restore_2 (name, age) values('Ursula', 40), ('Victor', 41), ('Wendy', 42), ('Xander', 43), ('Yvonne', 44);
insert into test_restore_2 (name, age) values('Zachary', 45), ('Ava', 46), ('Benjamin', 47), ('Charlotte', 48), ('Daniel', 49);
insert into test_restore_2 (name, age) values('Ella', 50), ('Finn', 51), ('Gabriella', 52), ('Henry', 53), ('Isabella', 54);
insert into test_restore_2 (name, age) values('Jack', 55), ('Katherine', 56), ('Liam', 57), ('Mia', 58), ('Noah', 59);
insert into test_restore_2 (name, age) values('Olivia', 60), ('Penelope', 61), ('Quinn', 62), ('Ryan', 63), ('Sophia', 64);
insert into test_restore_2 (name, age) values('Thomas', 65), ('Ursula', 66), ('Victor', 67), ('Wendy', 68), ('Xander', 69);

select count(*) from test_snapshot_restore.test_restore_2;


CREATE TABLE factories (
    factory_name VARCHAR(255) PRIMARY KEY,
    address TEXT NOT NULL
);

INSERT INTO factories (factory_name, address) VALUES ('Factory A', '123 Main St, Springfield, IL 62701'), ('Factory B', '456 Elm St, Springfield, IL 62702');
INSERT INTO factories (factory_name, address) VALUES ('Factory C', '789 Oak St, Springfield, IL 62703'), ('Factory D', '101 Pine St, Springfield, IL 62704');
INSERT INTO factories (factory_name, address) VALUES ('Factory E', '112 Birch St, Springfield, IL 62705'), ('Factory F', '131 Cedar St, Springfield, IL 62706');
INSERT INTO factories (factory_name, address) VALUES ('Factory G', '151 Maple St, Springfield, IL 62707'), ('Factory H', '171 Walnut St, Springfield, IL 62708');
INSERT INTO factories (factory_name, address) VALUES ('Factory I', '191 Cherry St, Springfield, IL 62709'), ('Factory J', '211 Elm St, Springfield, IL 62710');
INSERT INTO factories (factory_name, address) VALUES ('Factory K', '231 Oak St, Springfield, IL 62711'), ('Factory LLLLLLLLLL1', '251 Pine St, Springfield, IL 62712');
INSERT INTO factories (factory_name, address) VALUES ('Factory M', '271 Birch St, Springfield, IL 62713'), ('Factory N', '291 Cedar St, Springfield, IL 62714');
INSERT INTO factories (factory_name, address) VALUES ('Factory O', '311 Maple St, Springfield, IL 62715'), ('Factory P', '331 Walnut St, Springfield, IL 62716');
INSERT INTO factories (factory_name, address) VALUES ('Factory Q', '351 Cherry St, Springfield, IL 62717'), ('Factory R', '371 Elm St, Springfield, IL 62718');
INSERT INTO factories (factory_name, address) VALUES ('Factory S', '391 Oak St, Springfield, IL 62719'), ('Factory T', '411 Pine St, Springfield, IL 62720');
INSERT INTO factories (factory_name, address) VALUES ('Factory U', '431 Birch St, Springfield, IL 62721'), ('Factory V', '451 Cedar St, Springfield, IL 62722');
INSERT INTO factories (factory_name, address) VALUES ('Factory W', '471 Maple St, Springfield, IL 62723'), ('Factory X', '491 Walnut St, Springfield, IL 62724');
INSERT INTO factories (factory_name, address) VALUES ('Factory Y', '511 Cherry St, Springfield, IL 62725'), ('Factory Z', '531 Elm St, Springfield, IL 62726');
INSERT INTO factories (factory_name, address) VALUES ('Factory AA', '551 Oak St, Springfield, IL 62727'), ('Factory BB', '571 Pine St, Springfield, IL 62728');
INSERT INTO factories (factory_name, address) VALUES ('Factory CC', '591 Birch St, Springfield, IL 62729'), ('Factory DD', '611 Cedar St, Springfield, IL 62730');
INSERT INTO factories (factory_name, address) VALUES ('Factory EE', '631 Maple St, Springfield, IL 62731'), ('Factory FF', '651 Walnut St, Springfield, IL 62732');
INSERT INTO factories (factory_name, address) VALUES ('Factory GG', '671 Cherry St, Springfield, IL 62733'), ('Factory HH', '691 Elm St, Springfield, IL 62734');
INSERT INTO factories (factory_name, address) VALUES ('Factory II', '711 Oak St, Springfield, IL 62735'), ('Factory JJ', '731 Pine St, Springfield, IL 62736');
INSERT INTO factories (factory_name, address) VALUES ('Factory KK', '751 Birch St, Springfield, IL 62737'), ('Factory LL', '771 Cedar St, Springfield, IL 62738');
INSERT INTO factories (factory_name, address) VALUES ('Factory MM', '791 Maple St, Springfield, IL 62739'), ('Factory NN', '811 Walnut St, Springfield, IL 62740');
INSERT INTO factories (factory_name, address) VALUES ('Factory OO', '831 Cherry St, Springfield, IL 62741'), ('Factory PP', '851 Elm St, Springfield, IL 62742');
INSERT INTO factories (factory_name, address) VALUES ('Factory QQ', '871 Oak St, Springfield, IL 62743'), ('Factory RR', '891 Pine St, Springfield, IL 62744');
INSERT INTO factories (factory_name, address) VALUES ('Factory SS', '911 Birch St, Springfield, IL 62745'), ('Factory TT', '931 Cedar St, Springfield, IL 62746');
INSERT INTO factories (factory_name, address) VALUES ('Factory UU', '951 Maple St, Springfield, IL 62747'), ('Factory VV', '971 Walnut St, Springfield, IL 62748');
INSERT INTO factories (factory_name, address) VALUES ('Factory WW', '991 Cherry St, Springfield, IL 62749'), ('Factory XX', '1011 Elm St, Springfield, IL 62750');
INSERT INTO factories (factory_name, address) VALUES ('Factory YY', '1031 Oak St, Springfield, IL 62751'), ('Factory ZZ', '1051 Pine St, Springfield, IL 62752');
INSERT INTO factories (factory_name, address) VALUES ('Factory AAA', '1071 Birch St, Springfield, IL 62753'), ('Factory BBB', '1091 Cedar St, Springfield, IL 62754');
INSERT INTO factories (factory_name, address) VALUES ('Factory CCC', '1111 Maple St, Springfield, IL 62755'), ('Factory DDD', '1131 Walnut St, Springfield, IL 62756');
INSERT INTO factories (factory_name, address) VALUES ('Factory EEE', '1151 Cherry St, Springfield, IL 62757'), ('Factory FFF', '1171 Elm St, Springfield, IL 62758');
INSERT INTO factories (factory_name, address) VALUES ('Factory GGG', '1191 Oak St, Springfield, IL 62759'), ('Factory HHH', '1211 Pine St, Springfield, IL 62760');
INSERT INTO factories (factory_name, address) VALUES ('Factory III', '1231 Birch St, Springfield, IL 62761'), ('Factory JJJ', '1251 Cedar St, Springfield, IL 62762');
INSERT INTO factories (factory_name, address) VALUES ('Factory KKKK', '1271 Maple St, Springfield, IL 62763'), ('Factory LLLLLLLLLLLLLLL', '1291 Walnut St, Springfield, IL 62764');
INSERT INTO factories (factory_name, address) VALUES ('Factory MMM', '1311 Cherry St, Springfield, IL 62765'), ('Factory NNN', '1331 Elm St, Springfield, IL 62766');
INSERT INTO factories (factory_name, address) VALUES ('Factory OOO', '1351 Oak St, Springfield, IL 62767'), ('Factory PPP', '1371 Pine St, Springfield, IL 62768');
INSERT INTO factories (factory_name, address) VALUES ('Factory QQQ', '1391 Birch St, Springfield, IL 62769'), ('Factory RRR', '1411 Cedar St, Springfield, IL 62770');
INSERT INTO factories (factory_name, address) VALUES ('Factory SSS', '1431 Maple St, Springfield, IL 62771'), ('Factory TTT', '1451 Walnut St, Springfield, IL 62772');
INSERT INTO factories (factory_name, address) VALUES ('Factory UUU', '1471 Cherry St, Springfield, IL 62773'), ('Factory VVV', '1491 Elm St, Springfield, IL 62774');
INSERT INTO factories (factory_name, address) VALUES ('Factory WWW', '1511 Oak St, Springfield, IL 62775'), ('Factory XXX', '1531 Pine St, Springfield, IL 62776');
INSERT INTO factories (factory_name, address) VALUES ('Factory YYY', '1551 Birch St, Springfield, IL 62777'), ('Factory ZZZ', '1571 Cedar St, Springfield, IL 62778');
INSERT INTO factories (factory_name, address) VALUES ('Factory AAAA', '1591 Maple St, Springfield, IL 62779'), ('Factory BBBB', '1611 Walnut St, Springfield, IL 62780');
INSERT INTO factories (factory_name, address) VALUES ('Factory CCCC', '1631 Cherry St, Springfield, IL 62781'), ('Factory DDDD', '1651 Elm St, Springfield, IL 62782');
INSERT INTO factories (factory_name, address) VALUES ('Factory EEEE', '1671 Oak St, Springfield, IL 62783'), ('Factory FFFF', '1691 Pine St, Springfield, IL 62784');
INSERT INTO factories (factory_name, address) VALUES ('Factory GGGG', '1711 Birch St, Springfield, IL 62785'), ('Factory HHHH', '1731 Cedar St, Springfield, IL 62786');
INSERT INTO factories (factory_name, address) VALUES ('Factory IIII', '1751 Maple St, Springfield, IL 62787'), ('Factory JJJJ', '1771 Walnut St, Springfield, IL 62788');
INSERT INTO factories (factory_name, address) VALUES ('Factory KKKK', '1791 Cherry St, Springfield, IL 62789'), ('Factory LLLL', '1811 Elm St, Springfield, IL 62790');
INSERT INTO factories (factory_name, address) VALUES ('Factory MMMM', '1831 Oak St, Springfield, IL 62791'), ('Factory NNNN', '1851 Pine St, Springfield, IL 62792');
INSERT INTO factories (factory_name, address) VALUES ('Factory OOOO', '1871 Birch St, Springfield, IL 62793'), ('Factory PPPP', '1891 Cedar St, Springfield, IL 62794');
INSERT INTO factories (factory_name, address) VALUES ('Factory QQQQ', '1911 Maple St, Springfield, IL 62795'), ('Factory RRRR', '1931 Walnut St, Springfield, IL 62796');
INSERT INTO factories (factory_name, address) VALUES ('Factory SSSS', '1951 Cherry St, Springfield, IL 62797'), ('Factory TTTT', '1971 Elm St, Springfield, IL 62798');
INSERT INTO factories (factory_name, address) VALUES ('Factory UUUU', '1991 Oak St, Springfield, IL 62799'), ('Factory VVVV', '2011 Pine St, Springfield, IL 62800');
INSERT INTO factories (factory_name, address) VALUES ('Factory WWWW', '2031 Birch St, Springfield, IL 62801'), ('Factory XXXX', '2051 Cedar St, Springfield, IL 62802');

select count(*) from test_snapshot_restore.factories;
-- @session


-- @cleanup
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;
create snapshot cluster_sp for cluster;
-- @ignore:1
show snapshots;

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists snapshot_read;
drop database if exists test_snapshot_restore;
-- @session

drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';

restore account acc01 database snapshot_read from snapshot cluster_sp to account acc02;
restore account acc01 database test_snapshot_restore from snapshot cluster_sp to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
select count(*) from snapshot_read.test_snapshot_read;
select count(*) from snapshot_read.users;
select count(*) from snapshot_read.students;
select count(*) from test_snapshot_restore.test_restore;
select count(*) from test_snapshot_restore.test_restore_2;
select count(*) from test_snapshot_restore.factories;
-- @session

drop account if exists acc01;
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;

drop account if exists acc01;
drop account if exists acc02;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=3&user=acc01:test_account&password=111
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
-- @session

-- @cleanup
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;
create snapshot cluster_sp for cluster;
-- @ignore:1
show snapshots;

-- @session:id=3&user=acc01:test_account&password=111
drop database if exists Payroll;
drop database if exists Projects;
drop database if exists Company;
-- @session

drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';

restore account acc01 database Company from snapshot cluster_sp to account acc02;
restore account acc01 database Projects from snapshot cluster_sp to account acc02;
restore account acc01 database Payroll from snapshot cluster_sp to account acc02;

-- @session:id=4&user=acc02:test_account&password=111
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
-- @session

drop account if exists acc01;
drop account if exists acc02;
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;


drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=5&user=acc01:test_account&password=111
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
-- @session

-- @cleanup
drop snapshot if exists account_sp;
-- @ignore:1
show snapshots;
create snapshot account_sp for account acc01;
-- @ignore:1
show snapshots;

-- @session:id=5&user=acc01:test_account&password=111
drop database if exists Payroll;
drop database if exists Projects;
drop database if exists Company;
-- @session

drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';

restore account acc01 database Company from snapshot account_sp to account acc02;
restore account acc01 database Projects from snapshot account_sp to account acc02;
restore account acc01 database Payroll from snapshot account_sp to account acc02;

-- @session:id=6&user=acc02:test_account&password=111
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
-- @session

drop account if exists acc01;
drop account if exists acc02;
drop snapshot if exists account_sp;
-- @ignore:1
show snapshots;


drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=7&user=acc01:test_account&password=111
CREATE DATABASE School;
USE School;

drop table if exists Students;
CREATE TABLE Students (
    StudentID INT PRIMARY KEY,
    Name VARCHAR(100),
    Grade DECIMAL(3, 2)
);

drop table if exists Courses;
CREATE TABLE Courses (
    CourseID INT PRIMARY KEY,
    Title VARCHAR(100),
    Credits INT
);

drop table if exists Enrollments;
CREATE TABLE Enrollments (
    EnrollmentID INT PRIMARY KEY,
    StudentID INT,
    CourseID INT,
    Grade DECIMAL(3, 2),
    FOREIGN KEY (StudentID) REFERENCES Students(StudentID),
    FOREIGN KEY (CourseID) REFERENCES Courses(CourseID)
);

INSERT INTO Students (StudentID, Name, Grade) VALUES (1, 'Alice Smith', 3.5);
INSERT INTO Students (StudentID, Name, Grade) VALUES (2, 'Bob Johnson', 3.7);

INSERT INTO Courses (CourseID, Title, Credits) VALUES (101, 'Calculus', 4);
INSERT INTO Courses (CourseID, Title, Credits) VALUES (102, 'Physics', 4);

INSERT INTO Enrollments (EnrollmentID, StudentID, CourseID, Grade) VALUES (1, 1, 101, 3.5);
INSERT INTO Enrollments (EnrollmentID, StudentID, CourseID, Grade) VALUES (2, 2, 101, 3.6);
INSERT INTO Enrollments (EnrollmentID, StudentID, CourseID, Grade) VALUES (3, 1, 102, 3.7);

select * from Students;
select * from Courses;
select * from Enrollments;

CREATE VIEW StudentCourses AS
SELECT s.Name AS StudentName, c.Title AS CourseTitle, e.Grade
FROM Students s
JOIN Enrollments e ON s.StudentID = e.StudentID
JOIN Courses c ON e.CourseID = c.CourseID;

CREATE VIEW HighGradeStudents AS
SELECT s.StudentID, s.Name, AVG(e.Grade) AS AverageGrade
FROM Students s
JOIN Enrollments e ON s.StudentID = e.StudentID
GROUP BY s.StudentID, s.Name
HAVING AVG(e.Grade) >3;

CREATE VIEW CourseAverageGrades AS
SELECT c.CourseID, c.Title, AVG(e.Grade) AS AverageGrade
FROM Courses c
JOIN Enrollments e ON c.CourseID = e.CourseID
GROUP BY c.CourseID, c.Title;

select * from StudentCourses;
select * from HighGradeStudents;
select * from CourseAverageGrades;

-- database University
CREATE DATABASE University;
USE University;

drop table if exists Departments;
CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY,
    Name VARCHAR(100),
    Head VARCHAR(100)
);

drop table if exists Professors;
CREATE TABLE Professors (
    ProfessorID INT PRIMARY KEY,
    Name VARCHAR(100),
    DepartmentID INT,
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

drop table if exists Students;
CREATE TABLE Students (
    StudentID INT PRIMARY KEY,
    Name VARCHAR(100),
    DepartmentID INT,
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

drop table if exists Courses;
CREATE TABLE Courses (
    CourseID INT PRIMARY KEY,
    Title VARCHAR(100),
    DepartmentID INT,
    Credits INT,
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

drop table if exists Enrollments;
CREATE TABLE Enrollments (
    EnrollmentID INT PRIMARY KEY,
    StudentID INT,
    CourseID INT,
    Grade DECIMAL(3, 2),
    FOREIGN KEY (StudentID) REFERENCES Students(StudentID),
    FOREIGN KEY (CourseID) REFERENCES Courses(CourseID)
);

drop table if exists ProfessorsCourses;
CREATE TABLE ProfessorCourses (
    ProfessorID INT,
    CourseID INT,
    Semester VARCHAR(50),
    FOREIGN KEY (ProfessorID) REFERENCES Professors(ProfessorID),
    FOREIGN KEY (CourseID) REFERENCES Courses(CourseID),
    PRIMARY KEY (ProfessorID, CourseID, Semester)
);

INSERT INTO Departments (DepartmentID, Name, Head) VALUES (1, 'Computer Science', 'Dr. Alice');
INSERT INTO Departments (DepartmentID, Name, Head) VALUES (2, 'Mathematics', 'Dr. Bob');

INSERT INTO Professors (ProfessorID, Name, DepartmentID) VALUES (1, 'Dr. Charlie', 1);
INSERT INTO Professors (ProfessorID, Name, DepartmentID) VALUES (2, 'Dr. Delta', 2);

INSERT INTO Students (StudentID, Name, DepartmentID) VALUES (1, 'Eve', 1);
INSERT INTO Students (StudentID, Name, DepartmentID) VALUES (2, 'Frank', 2);

INSERT INTO Courses (CourseID, Title, DepartmentID, Credits) VALUES (101, 'Introduction to Computer Science', 1, 4);
INSERT INTO Courses (CourseID, Title, DepartmentID, Credits) VALUES (102, 'Advanced Mathematics', 2, 3);

INSERT INTO Enrollments (EnrollmentID, StudentID, CourseID, Grade) VALUES (1, 1, 101, 3.5);
INSERT INTO Enrollments (EnrollmentID, StudentID, CourseID, Grade) VALUES (2, 2, 102, 3.6);

INSERT INTO ProfessorCourses (ProfessorID, CourseID, Semester) VALUES (1, 101, 'Fall 2024');
INSERT INTO ProfessorCourses (ProfessorID, CourseID, Semester) VALUES (2, 102, 'Spring 2024');

select * from Departments;
select * from Professors;
select * from Students;
select * from Courses;
select * from Enrollments;
select * from ProfessorCourses;

CREATE VIEW StudentCourses AS
SELECT s.Name AS StudentName, c.Title AS CourseTitle, e.Grade
FROM Students s
JOIN Enrollments e ON s.StudentID = e.StudentID
JOIN Courses c ON e.CourseID = c.CourseID;

CREATE VIEW ProfessorCourses AS
SELECT p.Name AS ProfessorName, c.Title AS CourseTitle, pc.Semester
FROM Professors p
JOIN ProfessorCourses pc ON p.ProfessorID = pc.ProfessorID
JOIN Courses c ON pc.CourseID = c.CourseID;

CREATE VIEW DepartmentAverageGrade AS
SELECT d.Name AS DepartmentName, AVG(e.Grade) AS AverageGrade
FROM Departments d
JOIN Students s ON d.DepartmentID = s.DepartmentID
JOIN Enrollments e ON s.StudentID = e.StudentID
GROUP BY d.DepartmentID, d.Name;

CREATE VIEW TopStudents AS
SELECT s.Name, AVG(e.Grade) AS AverageGrade
FROM Students s
JOIN Enrollments e ON s.StudentID = e.StudentID
GROUP BY s.StudentID, s.Name
HAVING AVG(e.Grade) >= 3;

select * from StudentCourses;
select * from ProfessorCourses;
select * from DepartmentAverageGrade;
select * from TopStudents;

CREATE DATABASE EducationSystem;
USE EducationSystem;

CREATE VIEW ComprehensiveStudentCourseInfo AS
SELECT s.Name AS StudentName, s.Grade AS StudentGrade, c.Title AS CourseTitle, e.Grade AS CourseGrade
FROM School.Students s
JOIN School.Enrollments e ON s.StudentID = e.StudentID
JOIN School.Courses c ON e.CourseID = c.CourseID
UNION
SELECT u.Name AS StudentName, NULL AS StudentGrade, uc.Title AS CourseTitle, ue.Grade AS CourseGrade
FROM University.Students u
JOIN University.Enrollments ue ON u.StudentID = ue.StudentID
JOIN University.Courses uc ON ue.CourseID = uc.CourseID;

CREATE VIEW ComprehensiveEducatorInfo AS
SELECT p.Name AS ProfessorName, d.Name AS DepartmentName, c.Title AS CourseTitle, pc.Semester
FROM University.Professors p
JOIN University.ProfessorCourses pc ON p.ProfessorID = pc.ProfessorID
JOIN University.Courses c ON pc.CourseID = c.CourseID
JOIN University.Departments d ON p.DepartmentID = d.DepartmentID;


CREATE VIEW StudentOverallPerformance AS
SELECT s.Name AS StudentName, s.Grade AS StudentGrade, AVG(e.Grade) AS AverageGrade
FROM School.Students s
JOIN School.Enrollments e ON s.StudentID = e.StudentID
GROUP BY s.StudentID, s.Name, s.Grade
UNION
SELECT u.Name AS StudentName, NULL AS StudentGrade, AVG(ue.Grade) AS AverageGrade
FROM University.Students u
JOIN University.Enrollments ue ON u.StudentID = ue.StudentID
GROUP BY u.StudentID, u.Name;

select * from ComprehensiveStudentCourseInfo;
select * from ComprehensiveEducatorInfo;
select * from StudentOverallPerformance;
-- @session

-- @cleanup
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;
create snapshot cluster_sp for cluster;

-- @session:id=7&user=acc01:test_account&password=111
drop database if exists School;
drop database if exists University;
drop database if exists EducationSystem;
-- @session

drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';

restore account acc01 database School from snapshot cluster_sp to account acc02;
restore account acc01 database University from snapshot cluster_sp to account acc02;
restore account acc01 database EducationSystem from snapshot cluster_sp to account acc02;

-- @session:id=8&user=acc02:test_account&password=111
select * from School.Students;
select * from School.Courses;
select * from School.Enrollments;
select * from School.StudentCourses;
select * from School.HighGradeStudents;
select * from School.CourseAverageGrades;

select * from University.Departments;
select * from University.Professors;
select * from University.Students;
select * from University.Courses;
select * from University.Enrollments;
select * from University.ProfessorCourses;
select * from University.StudentCourses;
select * from University.ProfessorCourses;
select * from University.DepartmentAverageGrade;
select * from University.TopStudents;

select * from EducationSystem.ComprehensiveStudentCourseInfo;
select * from EducationSystem.ComprehensiveEducatorInfo;
select * from EducationSystem.StudentOverallPerformance;
-- @session

drop account if exists acc01;
drop account if exists acc02;
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;
