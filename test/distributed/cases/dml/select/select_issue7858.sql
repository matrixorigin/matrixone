-- fix issue 7858
drop database if exists db7858;
create database db7858;
use db7858;
CREATE TABLE t3 (S1 INT);
insert into t3 values (1),(2);
select * from t3;
SELECT * FROM t3  JOIN t3 on t3.S1=t3.S1;
SELECT * FROM t3  JOIN t3 a_t3 on t3.S1=a_t3.S1;
create table t1(a int);
insert into t1 values (1),(2);
SELECT * FROM t3  join t1 on t3.S1 = t1.a JOIN t3 on t3.S1=t3.S1;
SELECT * FROM t3  join t1 on t3.S1 = t1.a JOIN t3 a_t3 on t3.S1=a_t3.S1;
drop database if exists db7858;