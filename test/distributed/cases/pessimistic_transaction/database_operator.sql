-------------------------------------------------------------------------------------------------------------------------------------
DROP DATABASE IF EXISTS db1;
begin;
create database db1;
show databases like 'db1';
drop database db1;
show databases like 'db1';
create database db1;
show databases like 'db1';
commit;

use db1;
DROP TABLE IF EXISTS emp;
create table emp(
empno int unsigned auto_increment COMMENT '雇员编号',
ename varchar(15) COMMENT '雇员姓名',
job varchar(10) COMMENT '雇员职位',
mgr int unsigned COMMENT '雇员对应的领导的编号',
hiredate date COMMENT '雇员的雇佣日期',
sal decimal(7,2) COMMENT '雇员的基本工资',
comm decimal(7,2) COMMENT '奖金',
deptno int unsigned COMMENT '所在部门',
primary key(empno)
) COMMENT='雇员表';

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

show tables;
select * from db1.emp;
DROP DATABASE db1;

------------------------------------------------------------------------------------------------------------------
drop database if exists db2;

begin;
create database db2;
show databases like 'db2';
drop database db2;
show databases like 'db2';
create database db2;
show databases like 'db2';
use db2;
DROP TABLE IF EXISTS dept;
create table dept(
deptno int unsigned auto_increment COMMENT '部门编号',
dname varchar(15) COMMENT '部门名称',
loc varchar(50)  COMMENT '部门所在位置',
primary key(deptno)
) COMMENT='部门表';

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');

SELECT * FROM db2.dept;

drop database db2;
commit;
show databases like 'db2';

------------------------------------------------------------------------------------------------------------
create database if not exists db3;

begin;
drop database if exists db3;
create database if not exists db3;
use db3;
DROP TABLE IF EXISTS dept;
create table dept(
deptno int unsigned auto_increment COMMENT '部门编号',
dname varchar(15) COMMENT '部门名称',
loc varchar(50)  COMMENT '部门所在位置',
primary key(deptno)
) COMMENT='部门表';

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');

SELECT * FROM db3.dept;

commit;
show databases like 'db3';

use db3;
show tables;

DROP TABLE IF EXISTS emp;
create table emp(
empno int unsigned auto_increment COMMENT '雇员编号',
ename varchar(15) COMMENT '雇员姓名',
job varchar(10) COMMENT '雇员职位',
mgr int unsigned COMMENT '雇员对应的领导的编号',
hiredate date COMMENT '雇员的雇佣日期',
sal decimal(7,2) COMMENT '雇员的基本工资',
comm decimal(7,2) COMMENT '奖金',
deptno int unsigned COMMENT '所在部门',
primary key(empno)
) COMMENT='雇员表';

show tables;

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
select * from db3.emp;

DROP DATABASE db3;

------------------------------------------------------------------------------------------------------
create database if not exists db4;
begin;
drop database if exists db4;
create database if not exists db4;
use db4;
DROP TABLE IF EXISTS dept;
create table dept(
deptno int unsigned auto_increment COMMENT '部门编号',
dname varchar(15) COMMENT '部门名称',
loc varchar(50)  COMMENT '部门所在位置',
primary key(deptno)
) COMMENT='部门表';

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');

select * from db4.dept;
drop database db4;
commit;
show databases like 'db4';

create database db4;
show databases like 'db4';
use db4;
DROP TABLE IF EXISTS emp;
create table emp(
empno int unsigned auto_increment COMMENT '雇员编号',
ename varchar(15) COMMENT '雇员姓名',
job varchar(10) COMMENT '雇员职位',
mgr int unsigned COMMENT '雇员对应的领导的编号',
hiredate date COMMENT '雇员的雇佣日期',
sal decimal(7,2) COMMENT '雇员的基本工资',
comm decimal(7,2) COMMENT '奖金',
deptno int unsigned COMMENT '所在部门',
primary key(empno)
) COMMENT='雇员表';

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

select * from db4.emp;
drop database db4;

------------------------------------------------------------------------------------------------------
create database if not exists db5;
use db5;

DROP TABLE IF EXISTS dept;
create table dept(
deptno int unsigned auto_increment COMMENT '部门编号',
dname varchar(15) COMMENT '部门名称',
loc varchar(50)  COMMENT '部门所在位置',
primary key(deptno)
) COMMENT='部门表';
show tables;

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');
select * from db5.dept;

begin;
drop database db5;
create database db5;
use db5;

DROP TABLE IF EXISTS emp;
create table emp(
empno int unsigned auto_increment COMMENT '雇员编号',
ename varchar(15) COMMENT '雇员姓名',
job varchar(10) COMMENT '雇员职位',
mgr int unsigned COMMENT '雇员对应的领导的编号',
hiredate date COMMENT '雇员的雇佣日期',
sal decimal(7,2) COMMENT '雇员的基本工资',
comm decimal(7,2) COMMENT '奖金',
deptno int unsigned COMMENT '所在部门',
primary key(empno)
) COMMENT='雇员表';
show tables;

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
select * from db5.emp;

rollback;

use db5;
show tables;
select * from db5.dept;
drop database db5;