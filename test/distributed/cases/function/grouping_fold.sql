-- @suite
-- @case
-- @desc: GROUPING is folded per grouping-set branch; rolled-up keys are plain SQL NULLs
-- @label:bvt

drop database if exists grouping_fold;
create database grouping_fold;
use grouping_fold;
create table t(a int, b int, c varchar(10));
insert into t values (null, 1, 'x'), (1, 2, null), (1, 3, 'y'), (2, 4, 'x'), (null, 5, null);
create table tn(a int not null, b int not null);
insert into tn values (1, 10), (1, 20), (2, 30);
create table tpk(id int primary key, k int);
insert into tpk values (1, 30), (2, 10), (3, 20);
create table te(a int, b int);
select a, grouping(a), sum(b), count(*) from t group by a with rollup order by grouping(a), a;
select a, c, grouping(a, c), grouping(c), sum(b) from t group by a, c with rollup order by grouping(a, c), a, c;
select a, grouping(a), sum(b) from tn group by a with rollup order by grouping(a), a;
select a, b is null, grouping(a), grouping(b), count(*) from tn group by a, b with rollup order by grouping(a), grouping(b), a, b;
select a, sum(b) from t group by a with rollup having grouping(a) = 0 order by a;
select a, sum(b) from t group by a with rollup having grouping(a) = 1;
select a, sum(b) from t group by a with rollup having grouping(a) = 1 and sum(b) > 100;
select a, count(*) from te group by a with rollup;
select a, count(*) from te group by a with rollup having grouping(a) = 0;
select a, count(*) from te group by a with rollup having grouping(a) = 1;
select a, b, count(*) from te group by cube(a, b);
select a, c, grouping(a), grouping(c), grouping(a, c), count(*) from t group by cube(a, c) order by grouping(a, c), a, c;
select a, c, grouping(a, c), sum(b) from t group by grouping sets ((a), (c), ()) order by grouping(a, c), a, c;
select distinct a from t group by a, b with rollup order by a;
select distinct a, grouping(a) from t group by a with rollup order by grouping(a), a;
select ifnull(a, -1) k, grouping(a) g, sum(b) from t group by a with rollup order by g, k;
select a + 1, grouping(a) * 10 + 1, count(*) from t group by a with rollup order by 2, 1;
select x.a, x.s, y.s from (select a, sum(b) s from t group by a with rollup) x join (select a, sum(b) s from t group by a with rollup) y on x.a = y.a order by x.a;
select count(*) from (select a, sum(b) s from t group by a with rollup) x where x.a is null;
select a, sum(b), rank() over (order by sum(b)) from t group by a with rollup order by a, 2;
set session rollup_algorithm = 'SORT';
select a, sum(b), count(*) from t group by a with rollup order by a, 2;
select a, c, sum(b) from t group by a, c with rollup order by a, c, 3;
select a, grouping(a), sum(b) from t group by a with rollup order by grouping(a), a;
select a, count(*) from te group by a with rollup;
set session rollup_algorithm = 'HASH';
-- A primary key lets the optimizer remove the all-active branch aggregate.
select k, id, grouping(k), grouping(id), count(*) from tpk group by id, k with rollup order by grouping(k), grouping(id), id;
-- GROUPING is not allowed in WHERE.
select a from t where grouping(a) = 0 group by a with rollup;
drop database grouping_fold;
