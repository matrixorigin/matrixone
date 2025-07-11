create database if not exists test;
use test;
drop table if exists t1;
create table t1(a int);
insert into t1 values (1);
drop table if exists t2;
create table t2(a int);
begin;
drop table t1;
-- @session:id=1{
use test;
select * from t1;
insert into t2 select * from t1;
-- @session}
commit;
select * from t2;
begin;
drop table t2;
-- @session:id=1{
-- @wait:0:commit
insert into t2 select * from t2;
-- @session}
commit;