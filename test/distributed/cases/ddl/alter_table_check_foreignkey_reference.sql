drop database if exists db7;
create database db7;
use db7;
------------------------------------------------------------------------------------------------------------------------
drop table if exists dept;
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

drop table if exists emp;
create table emp(
                    empno int unsigned auto_increment COMMENT '雇员编号',
                    ename varchar(15) COMMENT '雇员姓名',
                    job varchar(10) COMMENT '雇员职位',
                    mgr int unsigned COMMENT '雇员对应的领导的编号',
                    hiredate date COMMENT '雇员的雇佣日期',
                    sal decimal(7,2) COMMENT '雇员的基本工资',
                    comm decimal(7,2) COMMENT '奖金',
                    deptno int unsigned COMMENT '所在部门',
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

--1.如果修改的列被其他表作为外键依赖,则不能使用change， modify进行修改
---------------------------------------------------------------------------------------------------------------------
alter table dept modify deptno int unsigned after dname;
--ERROR 1833 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1' of table 'db1.emp'

alter table dept modify deptno int unsigned;
--ERROR 1833 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1' of table 'db1.emp'

alter table dept change deptno deptId int unsigned after dname;
--ERROR 1846 (0A000): ALGORITHM=COPY is not supported. Reason: Columns participating in a foreign key are renamed. Try ALGORITHM=INPLACE.

alter table dept change deptno deptId int unsigned;
--ERROR 1846 (0A000): ALGORITHM=COPY is not supported. Reason: Columns participating in a foreign key are renamed. Try ALGORITHM=INPLACE.

alter table dept change deptno deptno int unsigned;
--ERROR 1833 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1' of table 'db1.emp'

alter table dept change deptno deptno int unsigned after dname;
--ERROR 1833 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1' of table 'db1.emp'
---------------------------------------------------------------------------------------------------------------------
alter table dept modify deptno int unsigned auto_increment;
--success
desc dept;
select * from dept;

alter table dept modify deptno int unsigned auto_increment after dname;
--success
desc dept;
select * from dept;

INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,100);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails (`db1`.`emp`, CONSTRAINT `emp_ibfk_1` FOREIGN KEY (`deptno`) REFERENCES `dept` (`deptno`))

INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);
select * from emp;

drop table emp;
drop table dept;
------------------------------------------------------------------------------------------------------------------------
drop table if exists dept;
create table dept(
                     deptno int unsigned COMMENT '部门编号',
                     dname varchar(15) COMMENT '部门名称',
                     loc varchar(50)  COMMENT '部门所在位置',
                     primary key(deptno)
) COMMENT='部门表';

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');

drop table if exists emp;
create table emp(
                    empno int unsigned auto_increment COMMENT '雇员编号',
                    ename varchar(15) COMMENT '雇员姓名',
                    job varchar(10) COMMENT '雇员职位',
                    mgr int unsigned COMMENT '雇员对应的领导的编号',
                    hiredate date COMMENT '雇员的雇佣日期',
                    sal decimal(7,2) COMMENT '雇员的基本工资',
                    comm decimal(7,2) COMMENT '奖金',
                    deptno int unsigned COMMENT '所在部门',
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

alter table dept modify deptno int unsigned auto_increment;
--ERROR 1833 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1' of table 'db1.emp'

alter table dept modify deptno int unsigned auto_increment after dname;
--ERROR 1833 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1' of table 'db1.emp'

alter table dept change deptno deptId int unsigned auto_increment after dname;
--ERROR 1846 (0A000): ALGORITHM=COPY is not supported. Reason: Columns participating in a foreign key are renamed. Try ALGORITHM=INPLACE.

alter table dept change deptno deptId int unsigned auto_increment;
--ERROR 1846 (0A000): ALGORITHM=COPY is not supported. Reason: Columns participating in a foreign key are renamed. Try ALGORITHM=INPLACE.

alter table dept change deptno deptId int unsigned after dname;
--success
desc dept;

INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,100);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails (`db1`.`emp`, CONSTRAINT `emp_ibfk_1` FOREIGN KEY (`deptno`) REFERENCES `dept` (`deptno`))
INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

select * from emp;

drop table emp;
drop table dept;
------------------------------------------------------------------------------------------------------------------------
drop table if exists dept;
create table dept(
                     deptno int unsigned COMMENT '部门编号',
                     dname varchar(15) COMMENT '部门名称',
                     loc varchar(50)  COMMENT '部门所在位置',
                     primary key(deptno)
) COMMENT='部门表';

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');

drop table if exists emp;
create table emp(
                    empno int unsigned auto_increment COMMENT '雇员编号',
                    ename varchar(15) COMMENT '雇员姓名',
                    job varchar(10) COMMENT '雇员职位',
                    mgr int unsigned COMMENT '雇员对应的领导的编号',
                    hiredate date COMMENT '雇员的雇佣日期',
                    sal decimal(7,2) COMMENT '雇员的基本工资',
                    comm decimal(7,2) COMMENT '奖金',
                    deptno int unsigned COMMENT '所在部门',
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

alter table dept modify deptno int unsigned default 10;
desc dept;
select * from dept;
--success

alter table dept modify deptno int unsigned after dname;
desc dept;
select * from dept;

--success
INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,100);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails (`db1`.`emp`, CONSTRAINT `emp_ibfk_1` FOREIGN KEY (`deptno`) REFERENCES `dept` (`deptno`))
INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

select * from emp;

drop table emp;
drop table dept;
------------------------------------------------------------------------------------------------------------------------
drop table if exists dept;
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

drop table if exists emp;
create table emp(
                    empno int unsigned auto_increment COMMENT '雇员编号',
                    ename varchar(15) COMMENT '雇员姓名',
                    job varchar(10) COMMENT '雇员职位',
                    mgr int unsigned COMMENT '雇员对应的领导的编号',
                    hiredate date COMMENT '雇员的雇佣日期',
                    sal decimal(7,2) COMMENT '雇员的基本工资',
                    comm decimal(7,2) COMMENT '奖金',
                    deptno int unsigned COMMENT '所在部门',
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


alter table dept ALTER COLUMN deptno SET DEFAULT 10;
--success
desc dept;

alter table dept ALTER COLUMN deptno SET INVISIBLE;
--success
desc dept;

alter table dept ALTER COLUMN deptno drop default;
--success
desc dept;

alter table dept rename column deptno to deptid;
--success
desc dept;
select * from dept;

--success
INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,100);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails (`db1`.`emp`, CONSTRAINT `emp_ibfk_1` FOREIGN KEY (`deptno`) REFERENCES `dept` (`deptno`))
INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);
select * from emp;

drop table emp;
drop table dept;

drop database if exists db7;