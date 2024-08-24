drop database if exists test;
create database test;
use test;

create table t1(a int) with retention period 10 month;
-- @ignore:3
select * from mo_catalog.mo_retention;

create table t2(a int) with retention period 2 second;
-- @ignore:3
select * from mo_catalog.mo_retention;
select mo_ctl('cn', 'task', 'retention:*/5 * * * * ?');
select sleep(10);
show tables;
-- @ignore:3
select * from mo_catalog.mo_retention;
select mo_ctl('cn', 'task', 'retention:* */10 * * * ?');