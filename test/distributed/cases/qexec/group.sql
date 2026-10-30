drop database if exists qetest;
create database qetest;
use qetest;

create table t (a int, b boolean, b2 tinyint, b3 smallint, b4 int, b5 bigint, 
                c varchar(100), d decimal(10, 2), 
                f real, g float,
                ts timestamp, dt datetime, dd date, 
                u uuid, 
                j json, bin varbinary); 

-- Keep AP, range-shuffle, modulo classes and partial vector batches with
-- fewer rows. Exact statistics below preserve the original execution paths.
insert into t select
    result, 
    result % 2 = 0, result % 10, result % 100, result % 1000, result,
    case when result % 10 = 0 then null else concat('foobar', result, 'zoo') end,
    case when result % 10 = 0 then null else result end,
    case when result % 10 = 0 then null else result end,
    case when result % 10 = 0 then null else result end,
    '2021-02-01 11:11:11', '2021-02-01 11:11:11', '2021-02-01',
    '11111111-1111-1111-1111-111111111111',
    concat('{"foo": "bar", "zoo": [1, 2, ', result, ']}'),
    cast(result as varbinary)
 from generate_series(1, 256000) gt;

select count(*) from t;

insert into t select * from t;
-- Keep cardinality and NDV independent of implicit memtable flush timing.
-- @ignore:7,8,10
analyze table t(a,b,c) fullscan;

select count(*) from t;

-- run a bunch of group bys
select count(*), count(a), 
count(b), count(b2), count(b3), count(b4), count(b5),
count(c), count(d), count(f), count(g), 
count(ts), count(dt), count(dd), 
count(u), count(j), count(bin),
avg(a), 
avg(b2), avg(b3), avg(b4), avg(b5),
avg(d), avg(f), avg(g), 
max(a), max(b), max(b2), max(b3), max(b4), max(b5),
max(c), max(d), max(f), max(g), 
max(ts), max(dt), max(dd), 
max(u), max(bin)
from t;

select count(*), count(distinct a), 
count(distinct b), count(distinct b2), count(distinct b3), count(distinct b4), count(distinct b5),
count(distinct c), count(distinct d), count(distinct f), count(distinct g), 
count(distinct ts), count(distinct dt), count(distinct dd), 
count(distinct u), count(distinct j), count(distinct bin),
avg(distinct a), 
avg(distinct b2), avg(distinct b3), avg(distinct b4), avg(distinct b5),
avg(distinct d), avg(distinct f), avg(distinct g), 
max(distinct a), max(distinct b), max(distinct b2), max(distinct b3), max(distinct b4), max(distinct b5),
max(distinct c), max(distinct d), max(distinct f), max(distinct g), 
max(distinct ts), max(distinct dt), max(distinct dd), 
max(distinct u), max(distinct bin)
from t 
where a < 1000;

select count(*), count(a), 
count(b), count(b2), count(b3), count(b4), count(b5),
count(c), count(d), count(f), count(g), 
count(ts), count(dt), count(dd), 
count(u), count(j), count(bin),
avg(a), 
avg(b2), avg(b3), avg(b4), avg(b5),
avg(d), avg(f), avg(g), 
max(a), max(b), max(b2), max(b3), max(b4), max(b5),
max(c), max(d), max(f), max(g), 
max(ts), max(dt), max(dd), 
max(u), max(bin)
from t
group by b2
order by b2
; 

select count(*), count(distinct a), 
count(distinct b), count(distinct b2), count(distinct b3), count(distinct b4), count(distinct b5),
count(distinct c), count(distinct d), count(distinct f), count(distinct g), 
count(distinct ts), count(distinct dt), count(distinct dd), 
count(distinct u), count(distinct j), count(distinct bin),
avg(distinct a), 
avg(distinct b2), avg(distinct b3), avg(distinct b4), avg(distinct b5),
avg(distinct d), avg(distinct f), avg(distinct g), 
max(distinct a), max(distinct b), max(distinct b2), max(distinct b3), max(distinct b4), max(distinct b5),
max(distinct c), max(distinct d), max(distinct f), max(distinct g), 
max(distinct ts), max(distinct dt), max(distinct dd), 
max(distinct u), max(distinct bin)
from t
where a < 1000
group by b2
order by b2
; 

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

-- Preserve the caller's session settings and force byte-budget spilling.
set @group_saved_dop = @@max_dop;
set @group_saved_spill_mem = @@agg_spill_mem;
-- force dop to 1, so it is easier to trigger spill
set @@max_dop = 1;
set @@agg_spill_mem = 16777216;
-- rerun queries

select count(*) from (select a, b, c from t group by a, b, c) tmptt;
select sum(a), max(c), avg(d), avg(f) from (select a, c, avg(d) as d, avg(f) as f from t group by a, c) tmptt;

select count(a), sum(d), sum(f) from (
select count(*), a, sum(d) as d, sum(f) as f from t group by a
) tmptt;

select count(a), sum(d), sum(f) from (
select count(*), a, sum(distinct d) d, sum(distinct f) f from t where a < 10000 group by a
) tmptt;

-- Require observable spilling, not just a low configured threshold.

-- @regex("Group Key:.*shuffle: range[(]t[.]a[)]", true)
-- @regex("SpillRows=[1-9][0-9]*", true)
-- @regex("SpillSize=[1-9][0-9.]* [KMGT]iB", true)
explain (analyze true) select a, b, c from t group by a, b, c;
-- @regex("Group Key:.*shuffle: range[(]t[.]a[)]", true)
-- @regex("SpillRows=[1-9][0-9]*", true)
-- @regex("SpillSize=[1-9][0-9.]* [KMGT]iB", true)
explain (analyze true) select a, c, avg(d) d, avg(f) f from t group by a, c;
-- The filtered DISTINCT control has fewer groups; give it its own byte budget.
set @@agg_spill_mem = 2097152;
-- @regex("SpillRows=[1-9][0-9]*", true)
-- @regex("SpillSize=[1-9][0-9.]* [KMGT]iB", true)
explain (analyze true) select count(*), a, sum(distinct d) d, sum(distinct f) f from t where a < 10000 group by a;

set @@max_dop = @group_saved_dop;
set @@agg_spill_mem = @group_saved_spill_mem;
drop database qetest;
