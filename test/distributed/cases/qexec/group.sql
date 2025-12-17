drop database if exists qetest;
create database qetest;
use qetest;

create table t (a int, b int, c varchar(100), d decimal(10, 2), f real);

insert into t select
    result, result % 1000,
    case when result % 10 = 0 then null else 'foobar' || result || 'zoo' end,
    case when result % 10 = 0 then null else result end,
    case when result % 10 = 0 then null else result end
 from generate_series(1, 1000000) gt;

select count(*) from t;

insert into t select * from t;

select count(*) from t;

-- run a bunch of group bys
select count(*), count(a), count(b), count(c),
avg(a), avg(b), avg(d), avg(f),
max(a), max(b), max(c), max(d), max(f)
from t; 

select count(a), sum(d), sum(f) from (
select count(*), a, sum(d) as d, sum(f) as f from t group by a
) tmp;

select count(a), sum(d), sum(f) from (
select count(*), a, sum(d) as d, sum(f) as f from t group by a
having a % 100 > 95
) tmp;

-- a few group by queries 
select count(*) from (select a, b, c from t group by a, b, c) tmpt;
select sum(a), max(c), avg(d), avg(f) from (select a, c, avg(d) as d, avg(f) as f from t group by a, c) tmpt;

select count(a), sum(d), sum(f) from (
select count(*), a, sum(distinct d) d, sum(distinct f) f from t where a < 10000 group by a
) tmpt;

-- force dop to 1, so it is easier to trigger spill
select 'set max_dop to 1 ... ';
set @@max_dop = 1;
-- this will set agg_spill_mem to min allowed (1MB)
select 'set agg_spill_mem to 50MB ... ';
set @@agg_spill_mem = 60000000; 
-- rerun queries

select count(*) from (select a, b, c from t group by a, b, c) tmptt;
select sum(a), max(c), avg(d), avg(f) from (select a, c, avg(d) as d, avg(f) as f from t group by a, c) tmptt;

select count(a), sum(d), sum(f) from (
select count(*), a, sum(distinct d) d, sum(distinct f) f from t where a < 10000 group by a
) tmptt;

-- A much better way is to run explain analyze, but these all depends on issue 22721

-- @bvt:issue#22721
explain analyze select a, b, c from t group by a, b, c;
explain analyze select a, c, avg(d) d, avg(f) f from t group by a, c;
explain analyze select count(*), a, sum(distinct d) d, sum(distinct f) f from t where a < 10000 group by a;
-- @bvt:issue

drop database qetest;
