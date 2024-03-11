drop database if exists test;
create database test;
use test;

drop table if exists test01;
create table test01(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned,
col9 float,
col10 double
);

insert into test01 values (1,2,3,4,5,6,7,8,10.2131,3824.34324);
insert into test01 values (2,3,4,5,6,7,8,9,2131.3242343,-3824.34324);
show create table test01;
create publication publication01 database test;
-- @ignore:2,3
show publications;

alter table test01 add primary key (col1, col2);
show create table test01;
alter table test01 add unique index `ui`(col1, col3);
show create table test01;
alter table test01 drop primary key;
show create table test01;
alter table test01 add primary key (col10);
show create table test01;
alter table test01 add column newCol int after col1;
select * from test01;
show create table test01;
alter table test01 modify column col1 decimal;
show create table test01;
-- @ignore:2,3
show publications;
drop publication publication01;
drop table test01;
drop database test;


drop account if exists acc0;
create account acc0 admin_name 'root' identified by '111';
drop database if exists sys_db_1;
create database sys_db_1;
use sys_db_1;
create table sys_tbl_1(a int primary key, b decimal, c char, d varchar(20) );
insert into sys_tbl_1 values(1,2,'a','database'),(2,3,'b','test publication'),(3, 4, 'c','324243243');
create publication sys_pub_1 database sys_db_1;
select * from sys_tbl_1;
-- @ignore:2,3
show publications;
select pub_name, database_name, account_list from mo_catalog.mo_pubs;
-- @session:id=2&user=acc0:root&password=111
create database sub1 from sys publication sys_pub_1;
show databases;
-- @session

-- @ignore:3,5
show subscriptions;
use sys_db_1;
alter table sys_tbl_1 drop primary key;
show create table sys_tbl_1;
alter table sys_tbl_1 add primary key(a,b);
show create table sys_tbl_1;
alter table sys_tbl_1 add unique index `b`(b,c);
show create table sys_tbl_1;
alter table sys_tbl_1 modify column a char after c;
show create table sys_tbl_1;

-- @session:id=2&user=acc0:root&password=111
drop database sub1;
-- @session
drop account acc0;
drop publication sys_pub_1;
drop database sys_db_1;


