drop database if exists db9;
create database db9;
use db9;

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

desc emp;
show columns from emp;
show full columns from emp;
show columns from emp like 'e%';

create view v1 as select empno, ename, job from emp;
desc v1;
show columns from v1;
show full columns from v1;
show columns from v1 like 'e%';

DROP TABLE emp;
DROP database db9;