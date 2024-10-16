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

-- multi view
-- database School
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

drop snapshot if exists sp06;
-- @ignore:1
show snapshots;
create snapshot sp06 for account sys;

drop database School;
drop database University;
drop database EducationSystem;

restore account sys from snapshot sp06;

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

drop database School;
drop database University;
drop database EducationSystem;

drop snapshot if exists sp06;
-- @ignore:1
show snapshots;
