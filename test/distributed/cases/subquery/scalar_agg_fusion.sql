-- @suite
-- @case
-- @desc: identical scalar aggregate inputs preserve empty, NULL and bag semantics
-- @label:bvt
drop database if exists scalar_agg_fusion;
create database scalar_agg_fusion;
use scalar_agg_fusion;
create table t(k int, v decimal(10,2));
create table x(id int);
insert into x values (1),(1),(2);
insert into t values (1,10),(1,10),(1,null),(2,40),(null,90);

-- Same filtered bag: COUNT(*)=3, COUNT(v)=2, AVG(v)=10; preserve outer duplicates.
select id, (select count(*) from t a where a.k=1) c,
           (select count(b.v) from t b where b.k=1) n,
           (select avg(d.v) from t d where d.k=1) av
from x order by id;
select id, c, n, av from x cross join
  (select count(*) c, count(v) n, avg(v) av from t where k=1) a order by id;

-- A second public shape: scalar derived tables joined without a predicate.
select a.c, b.s from (select count(*) c from t where k=1) a,
                    (select sum(v) s from t where k=1) b;

-- Nested fusion must remap references through every eliminated aggregate.
select a.c, b.s, b.m from (select count(*) c from t) a,
  (select s,m from (select sum(v) s from t) c, (select max(k) m from t) d) b;

-- Different predicates are not equivalent: the second AVG must remain 40.
select (select count(*) from t where k=1) c,
       (select avg(v) from t where k=2) av;

-- HAVING can remove the singleton row; its scalar value is NULL, not 0.
select (select count(*) from t having count(*) > 10) c,
       (select count(v) from t) n;

-- A scalar join condition is not disposable even when its inputs match.
select a.c, b.s from (select count(*) c from t) a left join
                    (select sum(v) s from t) b on a.c=b.s;

prepare p from 'select (select count(*) from t where k=?) c, (select avg(v) from t where k=?) av';
set @a=1;
set @b=2;
execute p using @a,@a;
execute p using @a,@b;
deallocate prepare p;

-- Empty input still emits the one global aggregate row, including AVG(NULL).
delete from t;
select (select count(*) from t) c, (select count(v) from t) n,
       (select avg(v) from t) av, (select sum(v) from t) s;
select count(*) c, count(v) n, avg(v) av, sum(v) s from t;
delete from x;
select id, (select count(*) from t) c, (select avg(v) from t) av from x;
drop database scalar_agg_fusion;
