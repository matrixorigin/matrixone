drop table if exists dept;
create table dept(
deptno int unsigned COMMENT '部门编号',
dname varchar(15) COMMENT '部门名称',
loc varchar(50)  COMMENT '部门所在位置',
unique key(deptno)
) COMMENT='部门表';

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');


drop table if exists emp;
create table emp(
empno int unsigned,
ename varchar(15),
job varchar(10),
mgr int unsigned,
hiredate date,
sal decimal(7,2),
comm decimal(7,2),
deptno int unsigned,
unique key(empno),
unique key(ename)
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
INSERT INTO emp VALUES (7902,'FORD',NULL,7566,'1981-12-03',3000,NULL,20);
INSERT INTO emp VALUES (null,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);


drop table if exists employees;
create table employees(
empno int unsigned COMMENT '雇员编号',
ename varchar(15) COMMENT '雇员姓名',
job varchar(10) COMMENT '雇员职位',
mgr int unsigned COMMENT '雇员对应的领导的编号',
hiredate date COMMENT '雇员的雇佣日期',
sal decimal(7,2) COMMENT '雇员的基本工资',
comm decimal(7,2) COMMENT '奖金',
deptno int unsigned COMMENT '所在部门',
unique key(empno, ename)
) COMMENT='雇员表';


INSERT INTO employees VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO employees VALUES (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
INSERT INTO employees VALUES (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
INSERT INTO employees VALUES (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
INSERT INTO employees VALUES (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
INSERT INTO employees VALUES (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
INSERT INTO employees VALUES (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
INSERT INTO employees VALUES (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
INSERT INTO employees VALUES (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
INSERT INTO employees VALUES (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
INSERT INTO employees VALUES (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
INSERT INTO employees VALUES (7900,NULL,'CLERK',7698,'1981-12-03',950,NULL,30);
INSERT INTO employees VALUES (7902,'FORD',NULL,7566,'1981-12-03',3000,NULL,20);
INSERT INTO employees VALUES (null,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

-- Single table deletion test
delete from emp t1 where t1.sal > 2000;
select * from emp;

-- Compliance with index test
delete from employees where deptno = 10;
select * from employees;

delete from employees where empno = 7698 and ename = 'BLAKE';
select count(*) from employees where empno = 7698 and ename = 'BLAKE';

-- Multi table deletion test
delete t1, t2  from emp as t1,dept as t2 where t1.deptno = t2.deptno and t1.deptno = 10;
select * from emp;
select * from dept;

delete employees, dept from employees, dept where employees.deptno = dept.deptno and sal > 2000;
select * from employees;
select * from dept;


truncate table emp;
select * from emp;

truncate table dept;
select * from dept;

truncate table employees;
select * from employees;


drop table dept;
drop table emp;
drop table employees;