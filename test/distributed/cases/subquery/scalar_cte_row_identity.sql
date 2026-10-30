-- @suite
-- @setup
drop database if exists test_scalar_cte_row_identity;
create database test_scalar_cte_row_identity;
use test_scalar_cte_row_identity;
create table bag_rows (k int, v int);
insert into bag_rows values (null, 7), (0, null), (2, 10), (2, 10);

-- @case
-- @desc:相关标量聚合保留 CTE 的重复行
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

-- UNION ALL 重复的外层行不能按物理行标识或可见列合并。
with q as (select k, v from bag_rows union all select k, v from bag_rows)
select b.k, b.v,
       (select max(v) from bag_rows x where x.k <= b.k) as m,
       (select count(*) from bag_rows x where x.k <= b.k) as c,
       (select count(v) from bag_rows x where x.k <= b.k) as cv
from q b order by b.k, b.v;

-- DISTINCT 外层仅保留其语义行数，而内层重复和 NULL 影响各类 COUNT。
with q as (select distinct k from bag_rows)
select b.k,
       (select count(*) from bag_rows x where x.k <= b.k) as c,
       (select count(v) from bag_rows x where x.k <= b.k) as cv,
       (select count(distinct v) from bag_rows x where x.k <= b.k) as cd
from q b order by b.k;

-- @teardown
drop database test_scalar_cte_row_identity;
