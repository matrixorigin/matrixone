-- @suite
-- @case
-- @desc: Forced sort ROLLUP preserves GROUPING metadata and empty-input totals
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

truncate table t_rollup_sort;

select a, b, count(*) as cnt, sum(amount) as total,
       grouping(a) as grouping_a, grouping(b) as grouping_b
from t_rollup_sort
group by a, b with rollup;

set session rollup_algorithm = default;
drop database rollup_sort_bvt;
