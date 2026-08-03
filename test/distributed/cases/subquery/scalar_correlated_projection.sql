-- @suite
-- @setup
drop database if exists test_subq_corr_project;
create database test_subq_corr_project;
use test_subq_corr_project;
create table t1 (a int, b int, c int);
create table t2 (d int);
insert into t1 values (1, 2, 3), (11, 22, 33);
create table parent_agg (id int primary key, corr_key int);
create table child_agg (corr_key int, v int);
insert into parent_agg values (1, 10), (2, 20), (3, 30), (4, 30);
insert into child_agg values (10, 5), (10, 7), (20, null);

-- @case
-- @desc:direct outer column projected by a correlated scalar subquery
-- @label:bvt
select t1.*, (select t1.a from t2 where t2.d > t1.a) as x from t1 order by t1.a;
select t1.*, (select distinct t1.a from t2) as x from t1 order by t1.a;
select t1.*, (select t1.a from t2 limit 1) as x from t1 order by t1.a;
insert into t2 values (99);
select t1.*, (select t1.a from t2 where t2.d > t1.a) as x from t1 order by t1.a;
select t1.*, (select distinct t1.a from t2) as x from t1 order by t1.a;
select t1.*, (select t1.a from t2 limit 1) as x from t1 order by t1.a;
insert into t2 values (100);
select t1.*, (select t1.a from t2 where t2.d > t1.a) from t1;
select t1.*, (select t1.a from t2 where t2.d > t1.a) as x from t1;
select t1.*, (select distinct t1.a from t2 where t2.d > t1.a) as x from t1 order by t1.a;

delete from t2;
insert into t2 values (5), (100);
select t1.*, (select t1.a from t2 where t2.d > t1.a order by t2.d limit 1) as x from t1 order by t1.a;
select (select t1.a from t2 where t2.d > t1.a limit 2) as x from t1 where t1.a = 11;
select (select t1.a from t2 where t2.d > t1.a limit 2) as x from t1 where t1.a = 1;

-- @case
-- @desc:issue #25959 - evaluate the final scalar aggregate projection after LEFT JOIN null extension
-- @label:bvt
select p.id, (select coalesce(sum(c.v), 0) from child_agg c where c.corr_key = p.corr_key) as sum_value from parent_agg p order by p.id;
select p.id, (select ifnull(avg(c.v), 7) from child_agg c where c.corr_key = p.corr_key) as avg_value, (select ifnull(min(c.v), 8) from child_agg c where c.corr_key = p.corr_key) as min_value, (select ifnull(max(c.v), 9) from child_agg c where c.corr_key = p.corr_key) as max_value from parent_agg p order by p.id;
select p.id, (select sum(c.v) from child_agg c where c.corr_key = p.corr_key) as raw_sum from parent_agg p order by p.id;
select p.id, (select count(*) from child_agg c where c.corr_key = p.corr_key) as row_count, (select count(c.v) from child_agg c where c.corr_key = p.corr_key) as value_count from parent_agg p order by p.id;
select p.id, (select count(*) + 1 from child_agg c where c.corr_key = p.corr_key) as count_plus_one, (select coalesce(count(*), 5) from child_agg c where c.corr_key = p.corr_key) as count_fallback from parent_agg p order by p.id;
select p.id, (select coalesce(sum(c.v), 100) + count(*) from child_agg c where c.corr_key = p.corr_key) as mixed_value from parent_agg p order by p.id;
select p.id, (select case when count(*) = 0 then 42 else coalesce(sum(c.v), 0) end from child_agg c where c.corr_key = p.corr_key) as case_value from parent_agg p order by p.id;
select p.id, (select coalesce(json_arrayagg(c.v), convert('[]', json)) from child_agg c where c.corr_key = p.corr_key) as json_value from parent_agg p order by p.id;
with correlated_input as (select corr_key, v from child_agg) select p.id, (select coalesce(sum(c.v), 0) from correlated_input c where c.corr_key = p.corr_key) as cte_sum from parent_agg p order by p.id;
select p.id, (with correlated_input as (select c.v from child_agg c where c.corr_key = p.corr_key) select coalesce(sum(v), 0) from correlated_input) as cte_correlated_sum from parent_agg p order by p.id;
select p.id, (select sum(c.v) from child_agg c where c.corr_key = p.corr_key group by c.corr_key) as grouped_sum from parent_agg p order by p.id;
select p.id, (select sum(c.v) from child_agg c where c.corr_key = p.corr_key having sum(c.v) > 100) as having_sum from parent_agg p order by p.id;

-- @teardown
drop database test_subq_corr_project;
