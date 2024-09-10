drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

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




-- prepare
drop table if exists prepare01;
prepare s1 from "create table prepare01(col1 int primary key , col2 char default 'a') with retention period 2 second";
execute s1;
show create table prepare01;
show columns from prepare01;
-- @ignore:2
select * from mo_catalog.mo_retention;
drop table prepare01;




-- backup restore, not reach retention time
drop table if exists back01;
create table back01 (a json,b int) with retention period 10 minute;
insert into back01 values ('{"t1":"a"}',1),('{"t1":"b"}',2);
drop snapshot if exists sp01;
create snapshot sp01 for account sys;
drop table back01;
restore account sys from snapshot sp01;
show tables;
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
-- @ignore:2
select * from mo_catalog.mo_retention;
drop snapshot sp01;
drop table back01;




-- backup restore, reach retention time
drop table if exists t1;
create table t1 (a text) with retention period 2 second;
insert into t1 values('abcdef');
insert into t1 values('_bcdef');
insert into t1 values('a_cdef');
select sleep(2);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
drop snapshot if exists sp02;
create snapshot sp02 for account sys;
restore account sys from snapshot sp02;
show tables;
-- @ignore:2
select * from mo_catalog.mo_retention;
drop snapshot sp02;




-- abnormal test
drop table if exists abnormal01;
create table abnormal01 (col1 int, col3 char) with retention period 0.1 day;
create table abnormal01 (col1 int, col3 char) with retention period 0.5 second;
create table abnormal01 (col1 int, col3 char) with retention period 100.1 minute;
create table abnormal01 (col1 int, col3 char) with retention period 0.5 hour;
create table abnormal01 (col1 int, col3 char) with retention period 0.4 week;
create table abnormal01 (col1 int, col3 char) with retention period 0.1 month;
create table abnormal01 (col1 int, col3 char) with retention period 10 years;
create table abnormal01 (col1 int, col3 char) with retention period 10 minutes;
drop database test01;




-- nonsys create retention table
-- @bvt:issue#18651
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists nonsys_test;
create database nonsys_test;
use nonsys_test;
drop table if exists test01;
create table test01(t1 time,t2 time,t3 time) with retention period 2 second;
insert into test01 values("-838:59:59.0000","838:59:59.00","22:00:00");
insert into test01 values("0:00:00.0000","0","0:00");
insert into test01 values(null,NULL,null);
insert into test01 values("23","1122","-1122");
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
-- @session
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:2
select * from mo_catalog.mo_retention;
show tables;
drop database nonsys_test;
-- @session
-- @bvt:issue




-- pub-sub, not subscribed
drop database if exists db01;
create database db01;
use db01;
drop table if exists table01;
create table table01 (col1 int unique key, col2 enum ('a','b','c')) with retention period 2 second;
insert into table01 values(1,'a');
insert into table01 values(2, 'b');

drop publication if exists pub01;
create publication pub01 database db01 table table01 account acc01 comment 'publish to acc01';
-- @ignore:5,6
show publications;
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
-- @ignore:5,6
show publications;
drop publication pub01;
drop database db01;




-- pub-sub, subscribed
drop database if exists db02;
create database db02;
use db02;
drop table if exists table01;
create table table01 (col1 int unique key, col2 enum ('a','b','c')) with retention period 2 second;
insert into table01 values(1,'a');
insert into table01 values(2, 'b');

drop publication if exists pub01;
create publication pub01 database db02 table table01 account acc01 comment 'publish to acc01';
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub01;
create database sub01 from sys publication pub01;
-- @ignore:5,7
show subscriptions;
-- @session

-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
drop database sub01;
-- @session

drop publication pub01;
drop database db02;
drop account acc01;




-- privilege
drop role if exists intern;
drop user if exists anne;
create role intern;
create user anne identified by '111';

grant connect on account * to intern;
grant create table on database * to intern with grant option;
grant create database on account * to intern;
grant drop database on account * to intern with grant option;
grant drop table on database * to intern with grant option;
grant select,insert ,update on table *.* to intern;
grant intern to anne;

-- @session:id=2&user=sys:anne:intern&password=111
drop database if exists anne_db;
create database anne_db;
use anne_db;
drop table if exists t1;
create table t1(col1 int, col2 char) with retention period 1 second;
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
-- @session
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
select sleep(1);
-- @session:id=2&user=sys:anne:intern&password=111
-- @ignore:2
select * from mo_catalog.mo_retention;
use anne_db;
show tables;
drop database anne_db;
-- @session
drop user anne;
drop role intern;




-- create retention table with period minute, year, day, month, hour
drop database if exists test02;
create database test02;
use test02;
drop table if exists table01;
drop table if exists table02;
drop table if exists table03;
drop table if exists table04;
drop table if exists table05;
create table table01 (col1 int unique key, col2 enum ('a','b','c')) with retention period 100 minute;
insert into table01 values(1,'a');
insert into table01 values(2, 'b');
create table table02(col1 int auto_increment, key key1(col1)) with retention period 2 week;
insert into table02 values (1);
insert into table02 values (2);
create table table03 (a text) with retention period 365 day;
insert into table03 values('abcdef'),('_bcdef'),('a_cdef'),('ab_def'),('abcd_f'),('abcde_');
create table table04 (a datetime(0) not null, primary key(a)) with retention period 13 month;
insert into table04 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
create table table05(col1 datetime) with retention period 200 hour;
insert into table05 values('2020-01-13 12:20:59.1234586153121');
insert into table05 values('2023-04-17 01:01:45');
-- @ignore:2
select * from mo_catalog.mo_retention;
-- @ignore:0
select mo_ctl('cn', 'task', ':retention');
-- @ignore:2
select * from mo_catalog.mo_retention;
drop database test02;