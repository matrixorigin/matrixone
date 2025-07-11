create database if not exists test;
use test;
drop table if exists tt1;
create table tt1(a int);
insert into tt1 values (1);
drop table if exists tt2;
create table tt2(a int);
begin;
drop table tt1;
-- @session:id=1{
use test;
select * from tt1;
insert into tt2 select * from tt1;
-- @session}
commit;
select * from tt2;
begin;
drop table tt2;
-- @session:id=1{
use test;
-- @wait:0:commit
insert into tt2 select * from tt2;
-- @session}
commit;