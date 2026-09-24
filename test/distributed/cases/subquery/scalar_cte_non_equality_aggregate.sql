-- @suite
-- @setup
drop database if exists test_scalar_cte_non_eq_agg;
create database test_scalar_cte_non_eq_agg;
use test_scalar_cte_non_eq_agg;
create table outer_rows (id int primary key, n int);
create table inner_rows (n int, v int);
insert into outer_rows values (1, 1), (2, 2), (3, 3), (4, 0), (5, null), (6, 2);
insert into inner_rows values (1, 10), (2, null), (3, 30), (4, 40);

-- @case
-- @desc:issue #29129 - a local CTE aggregate remains scalar for non-equality correlation
-- @label:bvt
select o.id, o.n,
       (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n) select m from x) as max_le,
       (with x as (select sum(i.n) as s from inner_rows i where i.n < o.n) select s from x) as sum_lt,
       (with x as (select count(*) as c from inner_rows i where i.n >= o.n) select c from x) as count_ge,
       (with x as (select count(i.v) as c from inner_rows i where i.n <= o.n) select c from x) as count_value
from outer_rows o order by o.id;
select o.id,
       (with x as (select min(i.v) as m from inner_rows i where i.n > o.n) select m from x) as min_gt,
       (with x as (select count(*) as c, sum(i.n) as s from inner_rows i where i.n <= o.n) select s from x) as second_aggregate,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n) select c from x) as count_eq,
       (with x as (select count(i.v) as c from inner_rows i where i.n = o.n) select c from x) as count_eq_value
from outer_rows o order by o.id;
select o.id,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n) select c from x) +
       coalesce((with x as (select sum(i.n) as s from inner_rows i where i.n <= o.n) select s from x), 0) as combined_value
from outer_rows o order by o.id;
select o.id, (select max(i.n) from inner_rows i where i.n <= o.n) as direct_max,
       (with x as (select max(i.n) as m from inner_rows i where i.n = o.n) select m from x) as cte_equal_max
from outer_rows o order by o.id;
select o.id, (select max(i.n) from inner_rows i where i.n <= o.n) as direct_max
from outer_rows o where exists (select 1 from inner_rows e where e.n = o.n) order by o.id;
select o.id, exists (select 1 from inner_rows e where e.n = o.n) as present,
       (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n) select m from x) as cte_max
from outer_rows o order by o.id;
select o.id, p.id,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n) select c from x) as cte_count
from outer_rows o cross join outer_rows p
where o.id in (1, 4) and p.id in (1, 4) order by o.id, p.id;

-- @teardown
drop database test_scalar_cte_non_eq_agg;
