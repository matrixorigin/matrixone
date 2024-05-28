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
drop database testdb;