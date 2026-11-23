-- @suite
-- @case
-- @desc: Grouping-set branches that share one materialized aggregate read a sink compacted to the columns they use
-- @label:bvt

drop database if exists grouping_set_shared_sink;
create database grouping_set_shared_sink;
use grouping_set_shared_sink;

-- Without statistics the planner shares one expanded aggregate between the
-- grouping-set branches and materializes it. A branch-local HAVING keeps the
-- branches as separate Sink Scan readers; selecting only aggregates compacts
-- the sink, so every reader must address the compacted layout.
create table t_empty(a int, b int, c varchar(10));

-- @regex("Sink Scan",true)
explain select count(*) from t_empty group by grouping sets ((a), ()) having count(*) >= 0;

select count(*) from t_empty group by grouping sets ((a), ()) having count(*) >= 0;
select count(*) from t_empty group by grouping sets ((a), ()) having 1 = 0;
select count(*) from t_empty group by grouping sets ((a), ()) having grouping(a) = 0;
select count(*) from t_empty group by grouping sets ((a), ()) having grouping(a) = 1;
select count(*), sum(b) from t_empty group by a with rollup having count(*) >= 0;
select count(*) from t_empty group by a with rollup having 1 = 0;
select count(*) from t_empty group by cube(a, b) having grouping(a, b) = 0;
select count(*) from t_empty group by cube(a, b) having grouping(a, b) = 3;
select b, count(*) from t_empty group by grouping sets ((a), (b), ()) having count(*) >= 0 order by b;
select c, grouping(c), count(*) from t_empty group by grouping sets ((a), (c), ()) having count(*) >= 0 order by grouping(c), c;

-- The same shapes over rows. The planner keeps the shared plan until the
-- table has statistics, so these run through the same compacted sink.
create table t_rows(a int, b int, c varchar(10));
insert into t_rows values (1, 10, 'x'), (1, 20, null), (2, 30, 'y'), (null, 40, 'x');

select count(*) from t_rows group by grouping sets ((a), ()) having count(*) >= 0 order by 1;
select count(*) from t_rows group by grouping sets ((a), ()) having 1 = 0;
select count(*) from t_rows group by grouping sets ((a), ()) having grouping(a) = 1;
select count(*), sum(b) from t_rows group by a with rollup having count(*) >= 2 order by 1, 2;
select sum(b) from t_rows group by cube(a, c) having grouping(a, c) = 2 order by 1;
select b, count(*) from t_rows group by grouping sets ((a), (b), ()) having count(*) >= 1 order by b, 2;
select c, grouping(c), count(*) from t_rows group by grouping sets ((a), (c), ()) having count(*) >= 1 order by grouping(c), c, 3;

drop database grouping_set_shared_sink;
