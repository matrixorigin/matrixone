set experimental_fulltext_index=1;
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop database if exists sp_test;
create database sp_test;
use sp_test;

-- restore db
drop table if exists test_sp01;
create table test_sp01 (col1 int, col2 decimal(6), col3 varchar(30));
insert into test_sp01 values (1, null, 'database');
insert into test_sp01 values (2, 38291.32132, 'database');
insert into test_sp01 values (3, null, 'database management system');
insert into test_sp01 values (4, 10, null);
insert into test_sp01 values (1, -321.321, null);
select count(*) from test_sp01;

drop view if exists v01;
create view v01 as select * from test_sp01;

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

create snapshot spsp01 for database sp_test;
-- @ignore:1
show snapshots;

drop database sp_test;

restore account sys database sp_test from snapshot spsp01;
show databases;
use sp_test;
show tables;
show create table test_sp01;
show create table aff01;
show create table pri01;
select * from test_sp01;
select * from pri01;
select * from aff01;
desc test_sp01;
desc aff01;
desc pri01;
drop database sp_test;
drop snapshot spsp01;
-- @ignore:1
show snapshots;



-- @bvt:issue#16438
-- create snapshot for database
drop database if exists sp_test01;
create database sp_test01;
use sp_test01;
drop table if exists partition01;
create table partition01 (
emp_no      int             not null,
birth_date  date            not null,
first_name  varchar(14)     not null,
last_name   varchar(16)     not null,
gender      varchar(5)      not null,
hire_date   date            not null,
primary key (emp_no)
) partition by range columns (emp_no)(
partition p01 values less than (100001),
partition p02 values less than (200001),
partition p03 values less than (300001),
partition p04 values less than (400001)
);
insert into partition01 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
(9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20');
drop snapshot if exists spsp02;
create snapshot spsp02 for database sp_test01;
-- @ignore:1
show snapshots;
delete from partition01 where birth_date = '1980-12-17';
select * from partition01;
restore account sys database sp_test01 from snapshot spsp02 ;
select * from partition01;
drop table partition01;
restore account sys database sp_test01 table partition01 from snapshot spsp02;
select * from partition01;
drop database sp_test01;
drop snapshot spsp02;
-- @bvt:issue



-- create snapshot for database, and snapshot is keywords
drop database if exists sp_test02;
create database sp_test02;
use sp_test02;
create table test01(col1 int, col2 decimal);
drop snapshot if exists `SELECT`;
create snapshot `SELECT` for database sp_test02;
-- @ignore:1
show snapshots;
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots;
drop snapshot `SELECT`;
drop database sp_test02;




-- create snapshot for database, snapshot name is non-keywords
drop database if exists sp_test03;
create database sp_test03;
use sp_test03;
create table test01(col1 int, col2 decimal);
drop snapshot if exists AUTO_INCREMENT;
create snapshot AUTO_INCREMENT for database sp_test03;
-- @ignore:1
show snapshots;
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots;
drop snapshot AUTO_INCREMENT;
drop database sp_test03;




-- create snapshot for database, snapshot name is case-sensitive
drop database if exists sp_Test03;
create database sp_Test03;
use sp_Test03;
create table Test01(col1 int, col2 char);
create table Test02(col1 int, col2 json);
drop snapshot if exists SP01;
create snapshot SP01 for database sp_Test03;
drop database sp_Test03;
restore account sys database sp_Test03 from snapshot SP01;
show databases;
use sp_Test03;
show tables;
drop database sp_Test03;
drop snapshot SP01;




-- abnormal test: create snapshot for system db
drop snapshot if exists sp_nor01;
create snapshot sp_nor01 for database system;



-- abnormal test: create duplicate snapshot for db
drop database if exists db01;
create database db01;
use db01;
drop snapshot if exists sp01;
create snapshot sp01 for database db01;
create snapshot sp01 for database db01;
drop snapshot sp01;
drop database db01;



-- where snapshot filter
drop database if exists db02;
create database db02;
use db02;
create table table01(col1 int, col2 decimal);
insert into table01 values(1,2);
drop snapshot if exists spsp02;
drop snapshot if exists spsp03;
create snapshot spsp02 for database db02;
create snapshot spsp03 for table db02 table01;
-- @ignore:1
show snapshots;
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where level = 'database';
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where level = 'table';
drop snapshot spsp02;
drop snapshot spsp03;
drop database db02;




-- create db/table level snapshot for non-self
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db03;
create database db03;
use db03;
create table table02(col1 char, col2 varchar(20));
insert into table02 values('a','21g3fuy214');
-- @session
drop snapshot if exists spsp04;
create snapshot spsp04 for account acc01 database db03;
create snapshot spsp04 for account acc01 table db03 table02;
-- @session:id=1&user=acc01:test_account&password=111
drop database db03;
-- @session



-- common user cannot create db/table level snapshot
-- @session:id=1&user=acc01:test_account&password=111
create role role1;
grant create database on account * to role1;
create user user1 identified by '111' default role role1;
-- @session

-- @session:id=2&user=acc01:user1:role1&password=111
create database db10;
create snapshot spsp05 for database db10;
drop database db10;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop role role1;
drop user user1;
-- @session




-- create snapshot for cluster table
-- @bvt:issue#21006
use mo_catalog;
drop table if exists t1;
create cluster table t1(a int);
insert into t1 values(1,6),(2,6),(3,6);
select * from t1;
drop snapshot if exists spsp07;
create snapshot spsp07 for table mo_catalog t1;
drop table t1;
-- @bvt:issue




-- restore db/table which does not exist when create snapshot
drop database if exists db10;
create database db10;
use db10;
create table table10(col1 int, col2 decimal, col3 char(1) primary key);
drop snapshot if exists spsp05;
create snapshot spsp05 for database db10;
drop database db10;
restore account sys database db10 table table11 from snapshot spsp05;
restore account sys database db11 from snapshot spsp05;
drop snapshot spsp05;




-- restore fk table
drop database if exists db06;
create database db06;
use db06;
drop table if exists pri01;
create table pri01(col1 int primary key,
                       col2 varchar(20),
                       col3 int,
                       col4 bigint);
drop table if exists aff01;
create table aff01(col1 int,
                       col2 int,
                       col3 int primary key,
                       constraint `c1` foreign key(col1) references pri01(col1));
show create table pri01;
show create table aff01;
insert into pri01 values(1,'sfhuwe',1,1);
insert into pri01 values(2,'37829901k3d',2,2);
insert into aff01 values(1,6,6);
insert into aff01 values(2,6,3);

drop snapshot if exists spsp06;
create snapshot spsp06 for database db06;

drop table aff01;
drop table pri01;

restore account sys database db06 from snapshot spsp06;
select * from pri01;
select * from aff01;
drop table aff01;
drop table pri01;

restore account sys database db06 table aff01 from snapshot spsp06;
restore account sys database db06 table pri01 from snapshot spsp06;
restore account sys database db06 table aff01 from snapshot spsp06;
show tables;
select * from aff01;
select * from pri01;
desc aff01;
desc pri01;
drop database db06;
drop snapshot spsp06;




-- restore fulltext index table
drop database if exists db07;
create database db07;
use db07;
drop table if exists fulltext01;
create table fulltext01
(
LastName char(10) primary key,
FirstName char(10),
Gender char(1),
DepartmentName char(20),
Age int
);
insert into fulltext01 VALUES('Gilbert', 'Kevin','M','Tool Design',33);
insert into fulltext01 VALUES('Tamburello', 'Andrea','F','Marketing',45);
insert into fulltext01 VALUES('Johnson', 'David','M','Engineering',66);
insert into fulltext01 VALUES('Sharma', 'Bradley','M','Production',27);
insert into fulltext01 VALUES('Rapier', 'Abigail','F',	'Human Resources',38);
select * from fulltext01;
drop snapshot if exists spsp08;
create snapshot spsp08 for database db07;
drop database db07;
restore account sys database db07 from snapshot spsp08;
show databases;
select * from db07.fulltext01;
drop database db07;
restore account sys database db07 table fulltext01 from snapshot spsp08;
select * from db07.fulltext01;
select count(*) from db07.fulltext01;
drop database db07;
drop snapshot spsp08;




-- restore table before alter
drop database if exists db08;
create database db08;
use db08;
drop table if exists pri01;
create table pri01 (col1 int, col2 text);
insert into pri01 (col1, col2) values (1,"database");
insert into pri01 values (234, "database management");
select * from pri01;
drop snapshot if exists spsp09;
create snapshot spsp09 for table db08 pri01;
show create table pri01;
select * from pri01;
alter table pri01 add constraint primary key(col1);
show create table pri01;
restore account sys database db08 table pri01 from snapshot spsp09;
show create table pri01;
insert into pri01 values(234, 'db');
select * from pri01;
drop database db08;
drop snapshot spsp09;




-- restore datalink type
drop database if exists db10;
create database db10;
use db10;
drop table if exists test01;
create table test01 (col1 int, col2 datalink);
insert into test01 values (1, 'file://$resources/load_data/time_date_1.csv');
select col1, load_file(col2) from test01;
-- @ignore:1
select * from test01;
drop snapshot if exists spsp10;
create snapshot spsp10 for database db10;
drop table test01;
restore account sys database db10 table test01 from snapshot spsp10;
-- @ignore:1
select * from db10.test01;
drop database db10;
drop snapshot spsp10;

drop account acc01;
-- @ignore:1
show snapshots;
