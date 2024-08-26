drop database if exists test;
create database test;
use test;

create table t1(a int) with retention period 10 month;
-- @ignore:2
select * from mo_catalog.mo_retention;
create table t2(a int) with retention period 1 second;
-- @ignore:2
select * from mo_catalog.mo_retention;
select sleep(2);
select mo_ctl('cn', 'task', 'retention:*/5 * * * * ?');
select sleep(5);
show tables;
-- @ignore:2
select * from mo_catalog.mo_retention;
select mo_ctl('cn', 'task', 'retention:* */10 * * * ?');
create account acc0 admin_name 'root' identified by '111';
-- @session:id=2&user=acc0:root&password=111
create database test;
use test;
-- @ignore:2
select * from mo_catalog.mo_retention;
create table t1(a int) with retention period 10 month;
-- @ignore:2
select * from mo_catalog.mo_retention;
drop table t1;
-- @ignore:2
select * from mo_catalog.mo_retention;
-- @session:id=3&user=sys:dump&password=111
drop account acc0;