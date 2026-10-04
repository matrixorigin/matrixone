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

-- A second aggregate cannot be replaced by the single raw-row reaggregation.
select o.id, (with a as (select max(i.n) as m from inner_rows i where i.n <= o.n),
              x as (select sum(m) as s from a) select s from x) as nested_value
from outer_rows o order by o.id;

-- An aggregate hidden behind a JOIN also cannot use the scalar rewrite.
select o.id, (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n),
              y as (select 1 as k) select x.m from x cross join y) as joined_value
from outer_rows o order by o.id;

-- An empty equality-correlated COUNT is a row with zero; c+1 must not become NULL.
select o.id, (with x as (select count(*) as c from inner_rows i where i.n = o.n)
              select c + 1 from x) as computed_count
from outer_rows o order by o.id;

-- Equality GROUP BY and null-rejecting HAVING retain their existing scalar semantics.
select o.id,
       (with x as (select max(i.v) as m from inner_rows i where i.n = o.n
                   having max(i.v) > 0) select m from x) as having_max,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n
                   group by i.n) select c from x) as grouped_count
from outer_rows o where o.id in (1, 2, 4) order by o.id;

-- A preceding raw scalar column must survive the range reaggregation.
select o.id, (select i.v from inner_rows i where i.n = o.n) as raw_value,
       (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n)
        select m from x) as range_max
from outer_rows o where o.id in (1, 2, 4) order by o.id;

-- A WHERE disjunction consumes both old scalar values and the new aggregate.
select o.id from outer_rows o where
    (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n)
     select m from x) > 0 or
    (with x as (select sum(i.n) as s from inner_rows i where i.n < o.n)
     select s from x) > 0 order by o.id;
select o.id from outer_rows o where
    exists (select 1 from inner_rows e where e.n = o.n and e.v is null) or
    (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n)
     select m from x) < 0 order by o.id;

-- Every selected COUNT output needs the zero-on-empty reconstruction.
select o.id,
       (0, 0) = (with x as (select count(*) as c, count(i.v) as d
                           from inner_rows i where i.n = o.n)
                 select c, d from x) as zero_pair,
       (1, 0) = (with x as (select count(*) as c, count(i.v) as d
                           from inner_rows i where i.n = o.n)
                 select c, d from x) as null_value_pair
from outer_rows o order by o.id;

-- A lower global aggregate always emits one row, including on empty input.
select o.id, (with a as (select max(i.n) as m from inner_rows i where i.n = o.n),
              x as (select count(*) as c from a) select c from x) as nested_count
from outer_rows o order by o.id;

-- Equality reconstruction must leave the outer row identity available for a
-- following non-equality reaggregation, including unmatched and NULL keys.
select o.id,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n)
        select c + 1 from x) as equality_count_plus_one,
       (with a as (select max(i.n) as m from inner_rows i where i.n = o.n),
             x as (select count(*) as c from a) select c from x) as nested_count,
       (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n)
        select m from x) as range_max
from outer_rows o order by o.id;

select o.id, (with x as (select max(i.n) as m from inner_rows i
                      where i.n = o.n or i.n = o.n + 1) select m from x) as or_max
from outer_rows o order by o.id;

-- Correlation above an independent aggregate filters its completed result;
-- it must not trigger reconstruction of the aggregate's empty-input row.
select o.id, (with x as (select count(*) as c from inner_rows i)
              select c from x where c = o.id) as independent_count
from outer_rows o order by o.id;
select o.id, (with x as (select i.n, max(i.v) as m from inner_rows i group by i.n)
              select m from x where x.n < o.n) as independent_group
from outer_rows o where o.id = 2;

-- @teardown
drop database test_scalar_cte_non_eq_agg;
