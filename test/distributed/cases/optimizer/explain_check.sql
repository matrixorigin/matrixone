drop database if exists d1;
create database d1;
use d1;
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1(c1 int primary key);
create table t2(c1 int primary key, c2 int, c3 int);
create table t3(c1 int, c2 int, c3 int, primary key(c1,c2));
create table t4(c1 bigint primary key, c2 bigint);
insert into t1 select * from generate_series(10000) g;
insert into t4 select c1, c1 from t1;
insert into t2 select c1, c1, c1 from t1;
insert into t2 select c1+10000, c1+10000, c1+10000 from t1;
insert into t3 select c1, c1, c1 from t1;
insert into t3 select c1+10000, c1+10000, c1+10000 from t1;
insert into t3 select c1+20000, c1+20000, c1+20000 from t1;
-- @separator:table
select mo_ctl('dn', 'flush', 'd1.t1');
-- @separator:table
select mo_ctl('dn', 'flush', 'd1.t2');
-- @separator:table
select mo_ctl('dn', 'flush', 'd1.t3');
-- @separator:table
select mo_ctl('dn', 'flush', 'd1.t4');

-- @separator:table
explain select * from t4 where c1 + 2 = 5;

-- @separator:table
explain (check '["= 3"]') select * from t4 where c1 + 2 = 5;

-- the following should fail
-- @separator:table
explain (check '["= 5"]') select * from t4 where c1 + 2 = 5;

-- @separator:table
explain analyze select * from t4 where c1 + 2 = 5;
-- @separator:table
explain (analyze true, check '["= 3"]') select * from t4 where c1 + 2 = 5;

-- the following should fail
-- @separator:table
explain (analyze true, check '["= 5"]') select * from t4 where c1 + 2 = 5;
-- 
-- @separator:table
explain (check '["% 2", "Sort", "Limit: 10", "% 3"]') select * from (select * from t1 where c1%3=0 order by c1 desc limit 10) tmpt where c1 % 2 = 0;

drop database if exists d1;
