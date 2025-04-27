select mo_cpu("total") >= mo_cpu("available");
select mo_memory("total") >= mo_memory("available");
select * from information_schema.files limit 1;

use system;
show tables;
select * from statement_info limit 0;
select * from span_info limit 0;
select * from rawlog limit 0;
select * from log_info limit 0;
select * from error_info limit 0;


create database db01;
use db01;
create table table01 (id int);
insert into table01 values(1);
select * from table01;
select * from table01 limit 0;
create table table02 (id int);
insert into table02 select * from table01 limit 0;
(select * from table01 limit 0) union all (select * from table02 limit 0);

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- @session:id=1&user=acc01:test_account&password=111
use system;
show tables;
select * from statement_info limit 0;
create database db01;
use db01;
create table table01 (id int);
insert into table01 values(1);
select * from table01;
select * from table01 limit 0;
create table table02 (id int);
insert into table02 select * from table01 limit 0;
(select * from table01 limit 0) union all (select * from table02 limit 0);
-- @session
select * from system.statement_info limit 0;

select * from db01.table01 limit 0;
drop database db01;
drop account if exists acc01;

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

drop database if exists test01;
create database test01;
use test01;
drop table if exists pri01;
create table pri01(
                      deptno int unsigned comment '部门编号',
                      dname varchar(15) comment '部门名称',
                      loc varchar(50)  comment '部门所在位置',
                      primary key(deptno)
) comment='部门表';

insert into pri01 values (10,'ACCOUNTING','NEW YORK');
insert into pri01 values (20,'RESEARCH','DALLAS');
insert into pri01 values (30,'SALES','CHICAGO');
insert into pri01 values (40,'OPERATIONS','BOSTON');

drop table if exists aff01;
create table aff01(
                      empno int unsigned auto_increment COMMENT '雇员编号',
                      ename varchar(15) comment '雇员姓名',
                      job varchar(10) comment '雇员职位',
                      mgr int unsigned comment '雇员对应的领导的编号',
                      hiredate date comment '雇员的雇佣日期',
                      sal decimal(7,2) comment '雇员的基本工资',
                      comm decimal(7,2) comment '奖金',
                      deptno int unsigned comment '所在部门',
                      primary key(empno),
                      constraint `c1` foreign key (deptno) references pri01 (deptno)
);

insert into aff01 values (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
insert into aff01 values (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
insert into aff01 values (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
insert into aff01 values (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
insert into aff01 values (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
insert into aff01 values (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
insert into aff01 values (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);

drop publication if exists pub01;
create publication pub01 database test01 account acc01;
-- @ignore:5,6
show publications;

-- @session:id=2&user=acc01:test_account&password=111
drop database if exists sub01;
create database sub01 from sys publication pub01;
use sub01;
show tables;
select * from aff01 limit 0;
select * from pri01 limit 0;
-- @session

drop publication if exists pub01;
-- @ignore:5,6
show publications;
drop database if exists test01;
drop account if exists acc01;

create database db01;
use db01;
create table table01 (id int);
insert into table01 values(1);
select * from table01 limit 1;
select id from table01 limit 1;
select id from table01 limit 0;
select * from table01 limit 0;
select * from table02 limit 0;
select * from system.table03 limit 0;

create snapshot snapshot01 for account sys;
select * from table01 {snapshot = 'snapshot01'} limit 0;
select * from table01 {snapshot = 'snapshot01'} limit 1;
select * from table02 {snapshot = 'snapshot01'} limit 0;
select * from table02 {snapshot = 'snapshot01'} limit 1;
drop snapshot if exists snapshot01;
drop database db01;
