create database test;
use test;
drop table if exists t1;
create table t1(col int, col2 decimal);
insert into t1 values(1,1);
drop table if exists t2;
create table t2 as select * from test.t1 {as of timestamp '2020-01-01 00:00:00'};
show tables;
drop database test;
