-- non-sys tenant clone sys db to current tenant
-- non-sys tenant clone table in sys db to current tenant
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- @session:id=1&user=acc01:test_account&password=111
drop snapshot if exists sp01;
create snapshot sp01 for account;
create database mo_catalog_new clone mo_catalog {snapshot = 'sp01'};
create database system_new;
create table system_new.statement_info clone system.statement_info {snapshot = 'sp01'};
create database mysql_new;
create table mysql_new.role_edges clone mysql.role_edges {snapshot = 'sp01'};
create database system_metrics_new;
create table system_metrics_new.sql_statement_duration_total clone system_metrics.sql_statement_duration_total {snapshot = 'sp01'};
create database information_schema_new;
create table information_schema_new.column_privileges clone information_schema.column_privileges {snapshot = 'sp01'};
drop database system_new;
drop database mysql_new;
drop database system_metrics_new;
drop database information_schema_new;
drop snapshot sp01;
-- @session
drop account acc01;




-- sys cannot clone data to sys db
drop database if exists db01;
create database db01;
use db01;
create table src (id bigint primary key, json1 json, json2 json);
insert into src values  (0, '{"a":1, "b":"redredredredredredredredredredrerr"}', '{"d": "happybirthdayhappybirthdayhappybirthday", "f":"winterautumnsummerspring"}'),
                        (1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
                        (2, '{"a":3, "b":"redbluebrownyelloworange"}', '{"d":"兒童中文"}');
drop snapshot if exists sp02;
create snapshot sp02 for database db01;
create table mo_debug.src clone db01.src {snapshot = 'sp02'};
create table information_schema.src clone db01.src {snapshot = 'sp02'};
create table mo_catalog.src clone db01.src {snapshot = 'sp02'};
create table mo_task.src clone db01.src {snapshot = 'sp02'};
create table mysql.src clone db01.src {snapshot = 'sp02'};
create table system.src clone db01.src {snapshot = 'sp02'};
create table system_metrics.src clone db01.src {snapshot = 'sp02'};
drop snapshot sp02;
drop database db01;




-- non-sys cannot clone data to sys db
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
-- @session:id=2&user=acc02:test_account&password=111
drop database if exists vector_db;
create database vector_db;
use vector_db;
create table vector_index_09(a int primary key, b vecf32(128),c int,key c_k(c));
insert into vector_index_09 values(9774 ,NULL,3),(9775,NULL,10);
insert into vector_index_09(a,c) values(9777,4),(9778,9);
drop snapshot if exists sp03;
create snapshot sp03 for database vector_db;
create table information_schema.vector_index_09 clone vector_db.vector_index_09 {snapshot = 'sp03'};
create table mo_catalog.vector_index_09 clone vector_db.vector_index_09 {snapshot = 'sp03'};
create table mysql.vector_index_09 clone vector_db.vector_index_09 {snapshot = 'sp03'};
create table system.vector_index_09 clone vector_db.vector_index_09 {snapshot = 'sp03'};
create table system_metrics.vector_index_09 clone vector_db.vector_index_09 {snapshot = 'sp03'};
drop snapshot sp03;
-- @session
drop account acc02;




-- sys tenant clone cluster table
use mo_catalog;
create cluster table t1(a int);
insert into t1 values(1,6),(2,6),(3,6);
drop snapshot if exists sp04;
create snapshot sp04 for account;
create database cluster_new;
create table cluster_new.t1 clone mo_catalog.t1 {snapshot = sp04};
use cluster_new;
show tables;
select * from t1;
drop table mo_catalog.t1;
drop database cluster_new;
drop snapshot sp04;




-- sys clone sys db to new non-sys db
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';
drop snapshot if exists sp05;
create snapshot sp05 for account;
create database mo_task_new clone mo_task {snapshot = 'sp05'} to account acc03;
create database information_schema_new clone information_schema {snapshot = 'sp05'}  to account acc03;
create database mo_catalog_new clone mo_catalog {snapshot = 'sp05'} to account acc03;
create database system_new clone system {snapshot = 'sp05'} to account acc03;
create database mo_task_new clone mo_task {snapshot = 'sp05'} to account acc03;
create database mo_debug_new clone mo_debug {snapshot = 'sp05'} to account acc03;
create database system_metrics_new clone system_metrics {snapshot = 'sp05'} to account acc03;
drop snapshot if exists sp05;



-- non-user under non-sys do not have clone permission
drop account if exists acc04;
create account acc04 admin_name = 'test_account' identified by '111';
-- @session:id=4&user=acc04:test_account&password=111
create user userx identified by '111';
create role role1;
grant create database on account * to role1;
grant alter user on account * to role1;
grant role1 to userx;
-- @session
-- @session:id=5&user=acc04:userx:role1&password=111
create database test01;
drop snapshot if exists sp01;
-- @session
drop account acc04;




-- sys clone sys db to current account
select count(*) from mo_catalog.mo_database;
drop snapshot if exists sp06;
create snapshot sp06 for account;
drop database if exists mo_catalog_new;
create database mo_catalog_new clone mo_catalog {snapshot = 'sp06'};
use mo_catalog_new;
show tables;
select count(*) from mo_catalog.mo_database;
select count(*) from mo_catalog_new.mo_database;
create database mo_catalog_new_new clone mo_catalog {snapshot = 'sp06'};
use mo_catalog_new_new;
show tables;
drop database mo_catalog_new_new;
drop database mo_catalog_new;




-- table level snapshot, clone db
drop database if exists test100;
create database test100;
use test100;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
drop table if exists table02;
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');
drop snapshot if exists sp07;
create snapshot sp07 for table test100 table01;
create database test_1000 clone test100 {snapshot = 'sp07'};
drop database test100;
drop database test_1000;
drop snapshot sp07;



-- clone table a where table a does not have snapshot
drop database if exists test101;
create database test101;
use test101;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
drop table if exists table02;
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');
drop snapshot if exists sp08;
create snapshot sp08 for table test100 table01;
drop database if exists db08;
create database db08;
create table db08.table01 clone test100.table01 {snapshot = 'sp08'};
drop database test101;
drop database db08;
drop snapshot sp08;



-- clone database a where database a does not have snapshot
drop database if exists test102;
create database test102;
use test102;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
drop database if exists test103;
create database test103;
use test103;
drop table if exists table02;
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');
drop snapshot if exists sp09;
create snapshot sp09 for database test102;
drop database if exists db09;
create database db09 clone test103 {snapshot = 'sp09'};
drop database test102;
drop database test103;
drop snapshot sp09;




-- account level snapshot for account acc01, clone db from snapshot acc02
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
-- @session:id=7&user=acc02:test_account&password=111
drop database if exists test102;
create database test102;
use test102;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
drop database if exists test103;
create database test103;
use test103;
drop table if exists table02;
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');
-- @session
drop snapshot if exists sp10;
create snapshot sp10 for account acc02;

create database db09 clone test102 {snapshot = 'sp10'};
drop account acc02;
drop snapshot sp10;
drop database db09;



drop account if exists acc06;
create account acc06 admin_name = 'test_account' identified by '111';
drop database if exists test10;
create database test10;
use test10;
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

drop snapshot if exists sp10;
create snapshot sp10 for account;

create database test11 clone test10 {snapshot = 'sp10'} to account acc06;
-- @session:id=6&user=acc06:test_account&password=111
use test11;
show create table aff01;
show create table pri01;
drop table aff01;
truncate pri01;
drop database test11;
-- @session
drop database test10;
drop snapshot sp10;
drop account acc06;
drop snapshot sp06;