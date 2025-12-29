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
select mo_ctl('dn', 'flush', 'd1.t1');
select mo_ctl('dn', 'flush', 'd1.t2');
select mo_ctl('dn', 'flush', 'd1.t3');
select mo_ctl('dn', 'flush', 'd1.t4');

-- basic explain without analyze
explain select * from t4 where c1 + 2 = 5;

-- check filter condition in explain output
explain (check '["Table Scan", "= 3", "Filter Cond"]') select * from t4 where c1 + 2 = 5;

-- the following should fail
explain (check '["= 5"]') select * from t4 where c1 + 2 = 5;

-- check ReadSize format (ReadSize=xx|xx|xx), filter condition, and other key fields
-- verify: ReadSize format with pipe separator, Analyze info fields, Table Scan node, and filter condition
-- Note: ReadSize values may vary (cache state, data distribution), so we only verify format, not specific values

-- @regex("ReadSize=",true)
explain (analyze true, check '["Table Scan", "ReadSize=", "|", "bytes", "InputSize=", "OutputSize=", "MemorySize=", "timeConsumed=", "inputRows=", "outputRows=", "= 3", "Filter Cond"]') select * from t4 where c1 + 2 = 5;

-- the following should fail
explain (analyze true, check '["= 5"]') select * from t4 where c1 + 2 = 5;

-- check complex query plan with Sort, Limit, Filter, and multiple conditions
-- @regex("Sort",true)
explain (check '["% 2", "Sort", "Limit: 10", "% 3"]') select * from (select * from t1 where c1%3=0 order by c1 desc limit 10) tmpt where c1 % 2 = 0;

drop database if exists d1;
