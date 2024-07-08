drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';

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

-- @session:id=1&user=acc01:test_account&password=111
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

-- @session:id=2&user=acc02:test_account&password=111
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
-- @ignore:1
show snapshots;

drop database if exists EducationSystem;
drop database if exists School;
drop database if exists University;

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists EducationSystem;
drop database if exists School;
drop database if exists University;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists EducationSystem;
drop database if exists School;
drop database if exists University;
-- @session

restore cluster from snapshot cluster_sp;


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


-- @session:id=1&user=acc01:test_account&password=111
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

-- @session:id=2&user=acc02:test_account&password=111
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

-- @cleanup
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;
drop database if exists EducationSystem;
drop database if exists School;
drop database if exists University;
drop account if exists acc01;
drop account if exists acc02;
