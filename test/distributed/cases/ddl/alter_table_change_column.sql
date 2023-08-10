drop database if exists db2;
create database db2;
use db2;

CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10));
desc t1;

insert into t1 values(1, 'ab');
insert into t1 values(2, 'ac');
insert into t1 values(3, 'ad');

select * from t1;

alter table t1 change a x VARCHAR(20);
desc t1;
select * from t1;
----------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10), c date);
desc t1;

insert into t1 values(1, 'ab', '1980-12-17');
insert into t1 values(2, 'ac', '1981-02-20');
insert into t1 values(3, 'ad', '1981-02-22');

select * from t1;

alter table t1 change a x VARCHAR(20) after b;
desc t1;
select * from t1;
----------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10), c date);
desc t1;

insert into t1 values(1, 'ab', '1980-12-17');
insert into t1 values(2, 'ac', '1981-02-20');
insert into t1 values(3, 'ad', '1981-02-22');

select * from t1;

alter table t1 change a x VARCHAR(20) after c;
desc t1;
select * from t1;
--------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER, b CHAR(10), c date, PRIMARY KEY(a));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17');
insert into t1 values(2, 'ac', '1981-02-20');
insert into t1 values(3, 'ad', '1981-02-22');

select * from t1;

alter table t1 change b x VARCHAR(20) first;
desc t1;
select * from t1;

-----------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);

select * from t1;

alter table t1 change b x VARCHAR(20), change d y int unsigned;

desc t1;
select * from t1;
----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);

select * from t1;
--ERROR 1068 (42000): Multiple primary key defined
alter table t1 change a x VARCHAR(20) PRIMARY KEY;
--ERROR 1068 (42000): Multiple primary key defined
alter table t1 change b x VARCHAR(20) PRIMARY KEY;
--ERROR 1054 (42S22): Unknown column 'b' in 't1'
alter table t1 change b z VARCHAR(20) first, change d m int unsigned after b;

ALTER TABLE t1 CHANGE a a INT NOT NULL;
desc t1;
select * from t1;
alter table t1 change b z VARCHAR(20) first, change d m int unsigned after z;

desc t1;
select * from t1;
----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER, b CHAR(10), c datetime, d decimal(7,2), PRIMARY KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17 11:34:45', 800);
insert into t1 values(2, 'ac', '1981-02-20 10:34:45', 1600);
insert into t1 values(3, 'ad', '1981-02-22 09:34:45', 500);
select * from t1;

alter table t1 change c x datetime default '2023-06-21 12:34:45' on update CURRENT_TIMESTAMP;
desc t1;
select * from t1;

alter table t1 change x y date;
desc t1;
select * from t1;
----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER, b CHAR(10), c datetime PRIMARY KEY default '2023-06-21' on update CURRENT_TIMESTAMP);
desc t1;

insert into t1 values(1, 'ab', '1980-12-17');
insert into t1 values(2, 'ac', '1981-02-20');
insert into t1 values(3, 'ad', '1981-02-22');
select * from t1;

alter table t1 change c x date first;
desc t1;
select * from t1;

alter table t1 change x y datetime default '2023-06-21';
desc t1;
select * from t1;
-----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2), UNIQUE KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 change b x VARCHAR(20);
show index from t1;
desc t1;
select * from t1;

alter table t1 change x y VARCHAR(20) UNIQUE KEY;
show index from t1;
desc t1;
select * from t1;

alter table t1 change y z VARCHAR(20) UNIQUE KEY;
show index from t1;
desc t1;
select * from t1;
------------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2), UNIQUE KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 rename column a to x;
show index from t1;
desc t1;
select * from t1;

alter table t1 rename column b to y;
show index from t1;
desc t1;
select * from t1;
------------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2), INDEX(a, b), KEY(c));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 change c x datetime;
show index from t1;
desc t1;
select * from t1;

--ERROR 1068 (42000): Multiple primary key defined
alter table t1 change x y VARCHAR(20) KEY;

alter table t1 change x y VARCHAR(20) UNIQUE KEY;
show index from t1;
desc t1;
select * from t1;
-----------------------------------------------------------------------------------------
drop table if exists t1;
create table t1(a int unsigned, b varchar(15) NOT NULL, c date, d decimal(7,2), primary key(a));
desc t1;

insert into t1 values (7369,'SMITH','1980-12-17',800);
insert into t1 values  (7499,'ALLEN','1981-02-20',1600);
insert into t1 values (7521,'WARD','1981-02-22',1250);
insert into t1 values  (7566,'JONES','1981-04-02',2975);
insert into t1 values  (7654,'MARTIN','1981-09-28',1250);
select * from t1;


alter table t1 change a x int auto_increment;
desc t1;
select * from t1;

alter table t1 change d y decimal(6,2);
desc t1;
select * from t1;
-----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER, b CHAR(10), c datetime, d decimal(7,2), PRIMARY KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17 11:34:45', 800);
insert into t1 values(2, 'ac', '1981-02-20 10:34:45', 1600);
insert into t1 values(3, 'ad', '1981-02-22 09:34:45', 500);
select * from t1;

alter table t1 change a x decimal(7,2);
desc t1;
select * from t1;

alter table t1 rename column b to y;
desc t1;
select * from t1;

alter table t1 rename column c to z;
desc t1;
select * from t1;
-----------------------------------------------------------------------------------------
drop table if exists dept;
create table dept(
                     deptno varchar(20),
                     dname varchar(15),
                     loc varchar(50),
                     primary key(deptno)
);

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');

drop table if exists emp;
create table emp(
                    empno int unsigned auto_increment,
                    ename varchar(15),
                    job varchar(10),
                    mgr int unsigned,
                    hiredate date,
                    sal decimal(7,2),
                    comm decimal(7,2),
                    deptno varchar(20),
                    primary key(empno),
                    FOREIGN KEY (deptno) REFERENCES dept(deptno)
);

INSERT INTO emp VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO emp VALUES (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
INSERT INTO emp VALUES (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
INSERT INTO emp VALUES (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
INSERT INTO emp VALUES (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
INSERT INTO emp VALUES (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
INSERT INTO emp VALUES (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
INSERT INTO emp VALUES (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
INSERT INTO emp VALUES (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
INSERT INTO emp VALUES (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
INSERT INTO emp VALUES (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
INSERT INTO emp VALUES (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
INSERT INTO emp VALUES (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
INSERT INTO emp VALUES (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

--ERROR 1832 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1'
alter table emp change deptno deptno char(20);
--ERROR 1832 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1'
alter table emp change deptno deptno int;

--ERROR 1832 (HY000): Cannot change column 'deptno': used in a foreign key constraint ''
alter table emp change deptno deptno varchar(10);
alter table emp change deptno deptno varchar(25);
desc emp;
select * from emp;

alter table emp change deptno deptId varchar(25);
desc emp;
select * from emp;
------------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2), UNIQUE KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 alter column a set default 100;
show index from t1;
desc t1;
select * from t1;

alter table t1 alter column a drop default;
show index from t1;
desc t1;
select * from t1;

alter table t1 alter column b set visible;
show index from t1;
desc t1;
select * from t1;
------------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER PRIMARY KEY, b CHAR(10), c date default '1990-12-17', d decimal(7,2), UNIQUE KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 alter column c set default '2003-12-17';
show index from t1;
desc t1;
select * from t1;

alter table t1 alter column c drop default;
show index from t1;
desc t1;
select * from t1;

alter table t1 alter column b set invisible;
show index from t1;
desc t1;
select * from t1;
----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2), UNIQUE KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 order by a DESC, b ASC;
desc t1;
select * from t1;

alter table t1 order by a DESC, c;
desc t1;
select * from t1;

drop table t1;
----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(col1 int not null, col2 varchar(10));
insert into t1 values (1, '137iu2');
insert into t1 values (1, '73ujf34f');
select * from t1;

alter table t1 change col1 col1 int primary key;
--ERROR 1062 (23000): Duplicate entry '1' for key 't1.PRIMARY'
desc t1;

alter table t1 change col2 col2 varchar(10) primary key;
desc t1;

insert into t1 values (1, 'cdsdsa');
select * from t1;
drop table t1;
drop database if exists db2;