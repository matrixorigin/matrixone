drop database if exists test;
create database test;
use test;

drop table if exists retention01;
create table retention01 (col1 int auto_increment, col2 decimal) with retention period 5 second;
insert into retention01 values (1, 2);
insert into retention01 values (2, 100);
insert into retention01 values (3, null);
select * from retention01;
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(5);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
show tables;
select * from retention01;
drop table retention01;




drop table if exists retention02;
create table retention02(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double) with retention period 3 second;
insert into retention02 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into retention02 values (2, 3, 'b', '32r32r', 'database', 1111111);
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(3);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
-- @ignore:2
select * from mo_catalog.mo_retention;
drop table if exists retention02;




-- create retention table, if not reach retention time, the table can be dropped
drop table if exists retention03;
create table retention03 (
        emp_no      int             not null,
        birth_date  date            not null,
        first_name  varchar(14)     not null,
        last_name   varchar(16)     not null,
        gender      varchar(5)      not null,
        hire_date   date            not null,
        primary key (emp_no)
) with retention period 10 second
    partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

insert into retention03 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                          (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20');
-- @ignore:2
select * from mo_catalog.mo_retention;
drop table retention03;
-- @ignore:2
select * from mo_catalog.mo_retention;
show tables;




-- create retention table, if not reach retention time, after drop database, the retention tabel does not exists
drop table if exists retention04;
create table retention04 (
  id int primary key ,
  order_number varchar(20),
  status enum('Pending', 'Processing', 'Completed', 'Cancelled')
) with retention period 5 second;
insert into retention04 values(1,'111',1),(2,'222',2),(3,'333',3),(4,'444','Cancelled');
-- @ignore:2
select * from mo_catalog.mo_retention;
drop database test;
-- @ignore:2
select * from mo_catalog.mo_retention;




-- create cluster table with retention
use mo_catalog;
drop table if exists retention05;
create cluster table retention05 (a int) with retention period 3 second;
insert into retention05 values(0, 0),(1, 0),(2, 0),(3, 0);
insert into retention05 values(0, 1),(1, 1),(2, 1),(3, 1);
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(3);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
-- @ignore:2
select * from mo_catalog.mo_retention;




-- create view depends on retention table
drop database if exists test01;
create database test01;
use test01;
drop table if exists retention06;
create table retention06 (col1 int, col2 decimal(6), col3 varchar(30)) with retention period 1 second;
insert into retention06 values (1, null, 'database');
insert into retention06 values (2, 38291.32132, 'database');
insert into retention06 values (3, null, 'database management system');
select count(*) from retention06;
show create table retention06;
drop view if exists v01;
create view v01 as select * from retention06;
select * from v01;
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(2);
-- @ignore:2
select * from mo_catalog.mo_retention;
show tables;
select * from v01;
drop view v01;




-- insert into select
drop table if exists retention08;
create table retention08(t1 time,t2 time,t3 time) with retention period 1 second;
insert into retention08 values("-838:59:59.0000","838:59:59.00","22:00:00");
insert into retention08 values("0:00:00.0000","0","0:00");
insert into retention08 values(null,NULL,null);
insert into retention08 values("23","1122","-1122");

drop table if exists retention09;
create table retention09(t1 time,t2 time,t3 time);
insert into retention09 select * from retention08;
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(2);
-- @ignore:2
select * from mo_catalog.mo_retention;
show tables;
select * from retention08;
select * from retention09;
drop table retention09;




-- fk table, pri table is retention table
-- @bvt:issue#18647
drop table if exists aff01;
drop table if exists pri01;
create table pri01(a int primary key, b int unique key) with retention period 2 second;
create table aff01 (a int, b int, foreign key f_a(a) references pri01(a));
insert into pri01 values (1,1), (2,2), (3,3);
insert into aff01 values (1,1), (2,2), (3,3);
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
-- @ignore:2
select * from mo_catalog.mo_retention;
show tables;
-- @bvt:issue




-- fk table, aff table is retention table
drop table if exists aff01;
drop table if exists pri01;
create table pri01(a int primary key, b int unique key);
create table aff01 (a int, b int, foreign key f_a(a) references pri01(a))  with retention period 2 second;
insert into pri01 values (1,1), (2,2), (3,3);
insert into aff01 values (1,1), (2,2), (3,3);
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
-- @ignore:2
select * from mo_catalog.mo_retention;
show tables;
select * from pri01;
drop table pri01;
