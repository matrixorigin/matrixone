-- @suite
-- @setup
drop database if exists test_scalar_cte_row_identity;
create database test_scalar_cte_row_identity;
use test_scalar_cte_row_identity;
create table bag_rows (k int, v int);
insert into bag_rows values (null, 7), (0, null), (2, 10), (2, 10);

-- @case
-- @desc:issue #23083 - private CTE identity preserves duplicate rows across scalar reaggregation
-- @label:bvt
with q as (select * from bag_rows)
select b.*,
       cast(exists(select 1 from q x where x.k = b.k) as signed) as present,
       (select max(v) from q x where x.k <= b.k) as m,
       (select count(*) from q x where x.k <= b.k) as c,
       (select count(v) from q x where x.k <= b.k) as cv
from q b order by b.k, b.v;

-- Empty inner input must distinguish COUNT's zero from MAX's NULL.
with q as (select * from bag_rows where k < 0)
select b.k,
       (select max(v) from q x where x.k <= b.k) as m,
       (select count(*) from q x where x.k <= b.k) as c
from bag_rows b order by b.k, b.v;

-- UNION ALL duplicates native row identity; retain the guarded rejection.
with q as (select k, v from bag_rows union all select k, v from bag_rows)
select (select max(v) from bag_rows x where x.k <= b.k) from q b;

-- @teardown
drop database test_scalar_cte_row_identity;
