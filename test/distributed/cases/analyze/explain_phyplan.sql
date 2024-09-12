drop database if exists db1;
create database db1;
use db1;

DROP TABLE IF EXISTS dept;
create table dept(
 deptno int unsigned auto_increment,
 dname varchar(15),
 loc varchar(50),
 primary key(deptno)
);

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');
-- @separator:table
explain phyplan select * from dept;


DROP TABLE IF EXISTS emp;
create table emp(
empno int unsigned auto_increment,
ename varchar(15),
job varchar(10),
mgr int unsigned,
hiredate date,
sal decimal(7,2),
comm decimal(7,2),
deptno int unsigned,
primary key(empno)
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
-- @separator:table
explain phyplan select * from emp;
-- @separator:table
explain phyplan select empno, ename, job from emp where sal > 2000;
-- @separator:table
explain phyplan select a.ename,b.dname from emp a left join dept b on a.deptno = b.deptno;

drop table emp;
drop table dept;
drop database db1;