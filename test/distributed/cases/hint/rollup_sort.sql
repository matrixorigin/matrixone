-- @suite
-- @case
-- @desc: Forced sort ROLLUP preserves GROUPING metadata and empty-input suppression
-- @label:bvt

drop database if exists rollup_sort_bvt;
create database rollup_sort_bvt;
use rollup_sort_bvt;

create table t_rollup_sort (
  a int,
  b int,
  amount int
);

insert into t_rollup_sort values
  (null, 1, 10),
  (null, 1, 20),
  (1, 1, 30),
  (1, 2, 40),
  (2, 1, 50);

set session rollup_algorithm = 'SORT';

-- The typed planner test proves the internal sort_rollup marker. This public
-- shape check proves the same top-level query reaches one Aggregate with an
-- input-side Sort instead of the legacy Union All expansion.
-- @regex("(?s)Aggregate.*Sort",true)
-- @regex("Union All",false)
explain select a, b,
                count(*) as cnt,
                sum(amount) as total,
                grouping(a) as grouping_a,
                grouping(b) as grouping_b
from t_rollup_sort
group by a, b with rollup
order by grouping_a, grouping_b, a, b;

select a, b,
       count(*) as cnt,
       sum(amount) as total,
       grouping(a) as grouping_a,
       grouping(b) as grouping_b
from t_rollup_sort
group by a, b with rollup
order by grouping_a, grouping_b, a, b;

-- DISTINCT compares SQL values: source NULL and rollup NULL collapse unless
-- the grouping provenance is explicitly part of the projected key.
select distinct a
from t_rollup_sort
group by a with rollup
order by a;

select distinct a, grouping(a) as grouping_a
from t_rollup_sort
group by a with rollup
order by a, grouping_a;

truncate table t_rollup_sort;

-- DISTINCT values repeated across detail groups must be deduplicated again
-- at each ROLLUP prefix. Separate single-aggregate queries exercise the
-- optimizer rewrite; a combined COUNT/SUM query would bypass it already.
insert into t_rollup_sort values (1, 7, 0), (2, 7, 0), (2, 9, 0), (2, null, 0);

select a, count(distinct b) as cnt, grouping(a) as grouping_a
from t_rollup_sort group by a with rollup order by grouping_a, a;
select a, sum(distinct b) as total, grouping(a) as grouping_a
from t_rollup_sort group by a with rollup order by grouping_a, a;
select a, cast(avg(distinct b) as decimal(10,2)) as avg_b, grouping(a) as grouping_a
from t_rollup_sort group by a with rollup order by grouping_a, a;

set session rollup_algorithm = 'HASH';
select a, count(distinct b) as cnt, grouping(a) as grouping_a
from t_rollup_sort group by a with rollup order by grouping_a, a;
select a, sum(distinct b) as total, grouping(a) as grouping_a
from t_rollup_sort group by a with rollup order by grouping_a, a;
select a, cast(avg(distinct b) as decimal(10,2)) as avg_b, grouping(a) as grouping_a
from t_rollup_sort group by a with rollup order by grouping_a, a;

set session rollup_algorithm = 'SORT';
truncate table t_rollup_sort;

select a, b, count(*) as cnt, sum(amount) as total,
       grouping(a) as grouping_a, grouping(b) as grouping_b
from t_rollup_sort
group by a, b with rollup;

-- Both spellings and HASH preserve the empty-input contract from main.
select a, b, count(*) as cnt, sum(amount) as total,
       grouping(a) as grouping_a, grouping(b) as grouping_b
from t_rollup_sort
group by rollup(a, b);

set session rollup_algorithm = 'HASH';
select a, b, count(*) as cnt, sum(amount) as total,
       grouping(a) as grouping_a, grouping(b) as grouping_b
from t_rollup_sort
group by a, b with rollup;

-- A real NULL row must still produce all three prefix groups.
insert into t_rollup_sort values (null, null, null);
set session rollup_algorithm = 'SORT';
select a, b, count(*) as cnt, sum(amount) as total,
       grouping(a) as grouping_a, grouping(b) as grouping_b
from t_rollup_sort
group by a, b with rollup
order by grouping_a, grouping_b;
set session rollup_algorithm = 'HASH';
select a, b, count(*) as cnt, sum(amount) as total,
       grouping(a) as grouping_a, grouping(b) as grouping_b
from t_rollup_sort
group by a, b with rollup
order by grouping_a, grouping_b;

-- SAMPLE owns a separate node and needs the legacy detail/grand-total
-- branches even when SORT is requested or ordered input is eligible for COST.
truncate table t_rollup_sort;
insert into t_rollup_sort values (1, 10, 0);
set session rollup_algorithm = 'SORT';
select a, sample(b, 100 percent) as sampled
from t_rollup_sort group by a with rollup;
select a, sample(b, 100 percent) as sampled
from t_rollup_sort group by rollup(a);
select a, sample(b, 100 percent) as sampled
from t_rollup_sort group by a;
set session rollup_algorithm = 'HASH';
select a, sample(b, 100 percent) as sampled
from t_rollup_sort group by a with rollup;
set session rollup_algorithm = 'COST';
select d.a, sample(d.b, 100 percent) as sampled
from (select a, b from t_rollup_sort order by a) d group by rollup(d.a);

set session rollup_algorithm = default;
drop database rollup_sort_bvt;
