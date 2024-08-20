drop DATABASE if exists Company;
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

drop DATABASE if exists Payroll;
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

drop snapshot if exists mater_sp;
-- @ignore:1
show snapshots;
create snapshot mater_sp for account sys;
-- @ignore:1
show snapshots;

-- restore
restore account sys database Company table Departments from snapshot mater_sp;
select * from Company.Departments;
select * from Company.Employees;
select * from Company.Positions;
select * from Company.EmployeePositions;
select * from Company.Salaries;
select * from Company.Benefits;
select * from Company.TimeOff;
select * from Company.PerformanceReviews;

restore account sys database Company table Employees from snapshot mater_sp;
restore account sys database Company table Positions from snapshot mater_sp;
restore account sys database Company table EmployeePositions from snapshot mater_sp;
restore account sys database Company table Salaries from snapshot mater_sp;
restore account sys database Company table Benefits from snapshot mater_sp;
restore account sys database Company table TimeOff from snapshot mater_sp;
restore account sys database Company table PerformanceReviews from snapshot mater_sp;
select * from Company.Departments;
select * from Company.Employees;
select * from Company.Positions;
select * from Company.EmployeePositions;
select * from Company.Salaries;
select * from Company.Benefits;
select * from Company.TimeOff;
select * from Company.PerformanceReviews;

drop snapshot if exists mater_sp;
-- @ignore:1
show snapshots;

drop database if exists Payroll;
drop database if exists Company;
