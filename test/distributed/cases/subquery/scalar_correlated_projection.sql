-- @suite
-- @setup
drop database if exists test_subq_corr_project;
create database test_subq_corr_project;
use test_subq_corr_project;
create table t1 (a int, b int, c int);
create table t2 (d int);
insert into t1 values (1, 2, 3), (11, 22, 33);

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

-- @teardown
drop database test_subq_corr_project;
