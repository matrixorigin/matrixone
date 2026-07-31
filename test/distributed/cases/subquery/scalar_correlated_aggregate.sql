-- @suite
-- @setup
drop database if exists test_scalar_correlated_aggregate;
create database test_scalar_correlated_aggregate;
use test_scalar_correlated_aggregate;
create table outer_t (k int primary key);
create table inner_t (k int, v int);
insert into outer_t values (1), (2), (3);
insert into inner_t values (1, 10), (2, null);

-- @case
-- @desc: issue #25959 - preserve scalar projection semantics for an empty correlated aggregate group
-- @label:bvt
select o.k, (select coalesce(sum(i.v), 0) from inner_t i where i.k = o.k) as sum_value from outer_t o order by o.k;
select o.k, (select ifnull(avg(i.v), 7) from inner_t i where i.k = o.k) as avg_value, (select ifnull(min(i.v), 8) from inner_t i where i.k = o.k) as min_value, (select ifnull(max(i.v), 9) from inner_t i where i.k = o.k) as max_value from outer_t o order by o.k;
select o.k, (select sum(i.v) from inner_t i where i.k = o.k) as raw_sum from outer_t o order by o.k;
select o.k, (select count(*) from inner_t i where i.k = o.k) as row_count, (select count(i.v) from inner_t i where i.k = o.k) as value_count from outer_t o order by o.k;
select o.k, (select coalesce(sum(i.v), 100) + count(*) from inner_t i where i.k = o.k) as mixed_value from outer_t o order by o.k;
with correlated_input as (select k, v from inner_t) select o.k, (select coalesce(sum(i.v), 0) from correlated_input i where i.k = o.k) as cte_sum from outer_t o order by o.k;
select o.k, coalesce((select sum(i.v) from inner_t i where i.k = o.k), 9) as outer_fallback from outer_t o order by o.k;
select o.k, (select sum(i.v) from inner_t i where i.k = o.k group by i.k) as grouped_sum from outer_t o order by o.k;
select o.k, (select sum(i.v) from inner_t i where i.k = o.k having sum(i.v) > 100) as having_sum from outer_t o order by o.k;

-- @teardown
drop database test_scalar_correlated_aggregate;
