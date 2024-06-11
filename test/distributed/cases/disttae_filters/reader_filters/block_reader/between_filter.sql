drop database if exists testdb;
create database testdb;
use testdb;

create table t1 (a int primary key, b int);
insert into t1 select *, * from generate_series(1,8192000)g;
-- @ignore:0
select mo_ctl("dn", "flush", "testdb.t1");
select b from t1 where a between 8191 and 8193 order by b asc;
drop table t1;

create table t2 (a varchar primary key, b varchar);
insert into t2 select *, * from generate_series(1,8192000)g;
-- @ignore:0
select mo_ctl("dn", "flush", "testdb.t2");
select b from t2 where a between 1 and 3 order by b asc;
drop table t2;

create table t3 (a decimal primary key, b decimal);
insert into t3 select *, * from generate_series(1,8192000)g;
-- @ignore:0
select mo_ctl("dn", "flush", "testdb.t3");
select b from t3 where a between 10 and 13 order by b asc;
drop table t3;

-- fake pk
create table t4 (a int, b int, index(b));
insert into t4 select *, * from generate_series(1,8192000)g;
-- @ignore:0
select mo_ctl("dn", "flush", "testdb.t4");
select a from t4 where b between 8191 and 8193 order by a asc;
drop table t4;

create table t5 (a varchar, b varchar, index(b));
insert into t5 select *, * from generate_series(1,8192000)g;
-- @ignore:0
select mo_ctl("dn", "flush", "testdb.t5");
select a from t5 where b between 1 and 3 order by a asc;
drop table t5;

create table t6 (a decimal, b decimal, index(b));
insert into t6 select *, * from generate_series(1,8192000)g;
-- @ignore:0
select mo_ctl("dn", "flush", "testdb.t6");
select a from t6 where b between 10 and 13 order by a asc;
drop table t6;

drop database testdb;