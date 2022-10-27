drop database if exists test_temporary;
create database test_temporary;
use test_temporary;
create temporary table t (a int);
create table t1 (a int);
-- test Temporary tables are not displayed when show table
show tables;
insert into t values (1), (2), (3);
select * from t;
delete from t where a = 1;
select * from t;
update t set a = 4 where a = 3;
select * from t;
insert into t1 values (100);
insert into t select * from t1;
select * from t;
-- test Temporary tables can be seen in different databases
create database test_temporary2;
use test_temporary2;
create temporary table t (a int);
create table t1 (a int);
select * from t;