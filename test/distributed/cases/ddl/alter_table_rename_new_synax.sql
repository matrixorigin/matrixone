CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    department VARCHAR(50)
);

INSERT INTO employees (first_name, last_name, department)
VALUES ('John', 'Doe', 'IT'),
       ('Jane', 'Smith', 'HR'),
       ('Michael', 'Johnson', 'Sales');

RENAME TABLE employees TO staff;
show tables;
desc staff;

drop table employees;
drop table staff;


create database test;
use test;
CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    department VARCHAR(50)
);

INSERT INTO employees (first_name, last_name, department)
VALUES ('John', 'Doe', 'IT'),
       ('Jane', 'Smith', 'HR'),
       ('Michael', 'Johnson', 'Sales');

RENAME TABLE test.employees TO test.staff;
show tables;
desc staff;

drop database test;
