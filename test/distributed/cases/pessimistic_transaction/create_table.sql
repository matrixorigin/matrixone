create database if not exists test;
use test;
drop table if exists t;
begin;
create table t(a int);
alter table t add column b int;
commit;
select * from t;
drop database test;
