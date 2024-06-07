drop database if exists testdb;
create database testdb;
use testdb;

-- test fake pk col linear search filter
create table t1(a int, b int, index(b));
insert into t1 select *, * from generate_series(1, 8192000)g;
-- @ignore:0
select mo_ctl("dn", "flush", "testdb.t1");
select a from t1 where b = 1;
select a from t1 where b between 1 and 3;
select a from t1 where b in (1,2,3);
drop table t1;

create table t2(a varchar, b varchar, index(b));
insert into t2 values('1','2'),('3','4'),('5','6'),('7','8'),('a','b'),('c','d'),('e','f'),('g','h');
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
select count(*) from t2;
-- @ignore:0
select mo_ctl("dn", "flush", "testdb.t2");
select distinct a from t2 where b = '2';
select distinct a from t2 where b between '2' and '6';
select distinct a from t2 where b in ('2','4','6');
drop table t2;

create table t3 (a float, b float, index(b));
insert into t3 select *, * from generate_series(1, 8192000)g;
-- @ignore:0
select mo_ctl("dn", "flush", "testdb.t3");
select a from t3 where b = 1;
select a from t3 where b between 1 and 3;
select a from t3 where b in (1,2,3);
drop table t3;

drop database testdb;