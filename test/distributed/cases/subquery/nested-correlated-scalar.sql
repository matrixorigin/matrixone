-- @suite
-- @setup
drop user if exists nested_correlation_reader_user;
drop role if exists nested_correlation_reader_role;
drop database if exists test_nested_correlated_scalar;
create database test_nested_correlated_scalar;
use test_nested_correlated_scalar;
create table j_dim  (id int);
create table j_fact (id int, dim_id int, ts int, val double);
insert into j_dim values (1), (2), (3);
insert into j_fact values
    (1, 1, 10, 100),
    (2, 1, 20, 200),
    (3, 2, 20, 220),
    (4, 2, 30, 300),
    (5, 99, 0, 900);

-- @case
-- @desc: scalar aggregate nested inside another scalar aggregate may correlate two levels up
-- @label:bvt
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (SELECT MAX(y.ts)
                         FROM j_fact y
                        WHERE y.dim_id = a.id)
       ) AS latest
FROM j_dim a
ORDER BY a.id;

-- APPROX_COUNT returns 0 for the missing key, which matches x.ts=0.
-- The enclosing MAX must therefore return 900 rather than NULL.
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (SELECT APPROX_COUNT(y.ts)
                         FROM j_fact y
                        WHERE y.dim_id = a.id)
       ) AS latest
FROM j_dim a
WHERE a.id = 3;

-- MAX is NULL for the missing key. COALESCE produces 0 for each of the
-- five x rows, so the outer COUNT is 5.
SELECT a.id,
       (SELECT COUNT(COALESCE(
                   (SELECT MAX(y.ts)
                      FROM j_fact y
                     WHERE y.dim_id = a.id),
                   0))
          FROM j_fact x) AS actual
  FROM j_dim a
 WHERE a.id = 3;

-- COALESCE in the consuming predicate also matches the x.ts=0 row.
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = COALESCE(
                   (SELECT MAX(y.ts)
                      FROM j_fact y
                     WHERE y.dim_id = a.id),
                   0)) AS latest
  FROM j_dim a
 WHERE a.id = 3;

-- COALESCE inside the aggregate projection runs on the implicit empty-input
-- aggregate row, giving 0 and selecting the value 900.
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (
               SELECT COALESCE(MAX(y.ts), 0)
                 FROM j_fact y
                WHERE y.dim_id = a.id)) AS latest
  FROM j_dim a
 WHERE a.id = 3;

-- LIMIT applies separately to the implicit scalar aggregate for each outer
-- row; it cannot truncate the set of correlation keys globally.
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (
               SELECT MAX(y.ts)
                 FROM j_fact y
                WHERE y.dim_id = a.id
                LIMIT 1)) AS latest
  FROM j_dim a
 ORDER BY a.id;

-- OFFSET 1 removes that sole aggregate row independently for every key.
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (
               SELECT MAX(y.ts)
                 FROM j_fact y
                WHERE y.dim_id = a.id
                LIMIT 1 OFFSET 1)) AS latest
  FROM j_dim a
 ORDER BY a.id;

-- Ordinary scalar results, skipped lexical levels, and two simultaneous
-- ancestors use the same typed invocation contract.
select d.id,(select (select y.val from j_fact y where y.id=d.id)) as v
  from j_dim d order by d.id;
select d.id,(select (select (select d.id))) as v from j_dim d order by d.id;
select d.id,(select (select d.id+x.ts) from j_fact x where x.id=d.id) as v
  from j_dim d order by d.id;
-- The unselected CASE branch must not observe its scalar cardinality error.
select d.id,(select case when d.id>0 then 7 else
  (select d.id+y.id from j_fact y) end) as v from j_dim d order by d.id;
select (select y.val from j_fact y where y.dim_id=d.id)
  from j_dim d where d.id=1;
select 7 as after_error;

-- Pagination demands output-only subqueries after selecting the output rows.
select d.id,(select (select d.id from j_fact y where y.id<=d.id)) as v
  from j_dim d order by d.id limit 1;
select d.id,case when d.id=2 then 7 else
  (select (select d.id from j_fact y)) end as v
  from j_dim d order by d.id limit 1 offset 1;
-- A subquery used as a sorting key remains demanded for all input rows.
select (select (select d.id from j_fact y where y.id<=d.id)) as v
  from j_dim d order by v limit 1;

-- FOUND_ROWS counts qualifying rows beyond pagination without demanding output tuples.
select sql_calc_found_rows d.id,(select (select d.id from j_fact y where y.id<=d.id)) as v
  from j_dim d order by d.id limit 1;
select found_rows() as n;
select sql_calc_found_rows d.id,case when d.id=2 then (select (select d.id)) else
  (select (select d.id from j_fact y)) end as v
  from j_dim d order by d.id limit 1 offset 1;
select found_rows() as n;
select sql_calc_found_rows (select (select d.id from j_fact y)) as v
  from j_dim d limit 0;
select found_rows() as n;
select sql_calc_found_rows distinct (select (select d.id%2)) as v
  from j_dim d order by v limit 1;
select found_rows() as n;
select sql_calc_found_rows count(*) as n,(select (select y.dim_id)) as k
  from j_fact y group by y.dim_id order by k limit 1;
select found_rows() as n;

-- Tuple operands in ON bind each nested demand before the row comparison.
select count(*) as n from j_dim d full join j_fact y
  on (d.id,(select (select y.id)))=(select d.id,y.id);
select count(*) as n from j_dim d left join j_fact y
  on (d.id,(select (select y.id)))=(select d.id,y.id);

-- Row comparison retains FALSE-before-NULL and UNKNOWN semantics.
select d.id,(d.id,0) not in (select (select d.id+1),NULL) as v
  from j_dim d order by d.id;
select d.id,(d.id,NULL)=(select (select d.id),NULL) as v
  from j_dim d order by d.id;

-- Declaration scope is distinct from the scope of each CTE use.
select d.id,(with x as (select y.id+d.id k from j_fact y where y.id<=2)
  select (select min(k) from x,j_dim d where d.id=3)+(select max(k) from x)) as v
  from j_dim d order by d.id;
select d.id,(with recursive x(k) as (select (select d.id)
  union all select k+1 from x where k<3)
  select (select min(k) from x)+(select max(k) from x)) as v
  from j_dim d order by d.id;

-- Native table functions, APPLY null-extension, and SAMPLE share the capture.
select d.id,(select (select count(*) from generate_series(d.id,3) g)) as v
  from j_dim d order by d.id;
select d.id,(select 10*count(*)+count(g.result) from j_fact y
  outer apply generate_series(y.id,0,1) g where y.id=(select d.id)) as v
  from j_dim d order by d.id;
select d.id,(select sum(x.k) from
  (select sample((select d.id),100 percent) k from j_fact y) x) as v
  from j_dim d order by d.id;

-- View replay and CTAS consume the main output schema, including DECIMAL scale.
-- EXISTS observes row presence and pagination, without evaluating the unused tuple.
select d.id,exists(select (select d.id from j_fact y) from j_fact x order by x.id) as e
from j_dim d order by d.id;
select d.id,exists(select (select d.id from j_fact y) from j_fact x order by x.id limit 1 offset 5) as e
from j_dim d order by d.id;
select d.id,(with q(n) as (select d.id)
select row_number() over(order by count(*)) from q group by n) as rn
from j_dim d order by d.id;
-- Native time-window and fill stages retain the ancestor parameter.
select d.id,(select sum(s) from (select sum((select d.id)) s
from (select cast('2026-01-01' as datetime) ts from j_fact y) x
interval(ts,10,minute)) w) as v from j_dim d order by d.id;
select d.id,(select sum(s) from (select sum((select d.id)) s
from (select case when y.id=1 then cast('2026-01-01 00:00:00' as datetime)
else cast('2026-01-01 00:20:00' as datetime) end ts from j_fact y where y.id in (1,3)) x
interval(ts,10,minute) gapfill(partition) fill(next)) w) as v from j_dim d order by d.id;

-- Unconsumed aggregate/window/UNION ALL tuples do not create scalar demand.
select d.id,exists(select sum((select d.id from j_fact y))
  from j_fact x group by x.dim_id) as e from j_dim d order by d.id;
select d.id,exists(select sum((select d.id from j_fact y)) over ()
  from j_fact x) as e from j_dim d order by d.id;
select d.id,exists(select (select d.id from j_fact y) from j_fact x where x.id<0
  union all select (select d.id from j_fact y) from j_fact x) as e
  from j_dim d order by d.id;
-- Group keys still determine presence and must demand their subqueries.
select exists(select 1 from j_fact x group by (select d.id from j_fact y))
  from j_dim d;
select d.id,(select count(*) from j_fact x group by d.id) as n
  from j_dim d order by d.id;
select d.id,(select count(*) from j_fact x group by (select d.id)) as n
  from j_dim d order by d.id;
-- Each invocation evaluates its own FILL value: 2*d.id + d.id+10.
select d.id,(select sum(s) from (select sum((select d.id)) s
from (select case when y.id=1 then cast('2026-01-01 00:00:00' as datetime)
else cast('2026-01-01 00:20:00' as datetime) end ts from j_fact y where y.id in (1,3)) x
interval(ts,10,minute) gapfill(partition) fill(value,(select (select d.id+10)))) w) as v
from j_dim d order by d.id;
-- Existing CSV source metadata is reusable; only rows 1 and 2 follow the header.
create external table nested_external(id int,label varchar(20),v int)
  infile{'filepath'='$resources/load_data/glob/part_0.csv'}
  fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select d.id,(select (select sum(e.v) from nested_external e where e.id<=d.id)) as v
  from j_dim d order by d.id;
prepare nested_external_query from 'select d.id,(select (select sum(e.v) from nested_external e where e.id<=d.id and e.id<=?)) as v from j_dim d order by d.id';
set @external_limit=1;
execute nested_external_query using @external_limit;
set @external_limit=2;
execute nested_external_query using @external_limit;
deallocate prepare nested_external_query;
drop table nested_external;

create view nested_view as select d.id,(select (select cast(d.id as decimal(12,2)))) as v from j_dim d;
select * from nested_view order by id;
create table nested_ctas as select * from nested_view;
select * from nested_ctas order by id;
select column_name,data_type,numeric_precision,numeric_scale
  from information_schema.columns where table_schema='test_nested_correlated_scalar'
  and table_name='nested_ctas' and column_name in ('id','v') order by ordinal_position;
drop table nested_ctas;
drop view nested_view;

-- Bound dependencies require authorization even on an unselected branch.
create role nested_correlation_reader_role;
create user nested_correlation_reader_user identified by '111' default role nested_correlation_reader_role;
grant connect on account * to nested_correlation_reader_role;
grant select on table test_nested_correlated_scalar.j_dim to nested_correlation_reader_role;
-- @session:id=1&user=nested_correlation_reader_user&password=111
use test_nested_correlated_scalar;
select count(*) as n from j_dim;
select d.id,(select case when d.id>0 then 7 else
  (select max(y.val) from j_fact y where y.dim_id=d.id) end) as v
  from j_dim d order by d.id;
-- @session
grant select on table test_nested_correlated_scalar.j_fact to nested_correlation_reader_role;
-- @session:id=1&user=nested_correlation_reader_user&password=111
prepare nested_authorized from 'select d.id,(select (select max(y.val) from j_fact y where y.dim_id=d.id and y.id<=?)) as v from j_dim d order by d.id';
set @authorized_limit=2;
execute nested_authorized using @authorized_limit;
set @authorized_limit=4;
execute nested_authorized using @authorized_limit;
-- @session
revoke select on table test_nested_correlated_scalar.j_fact from nested_correlation_reader_role;
-- @session:id=1&user=nested_correlation_reader_user&password=111
execute nested_authorized using @authorized_limit;
deallocate prepare nested_authorized;
-- @session
drop user nested_correlation_reader_user;
drop role nested_correlation_reader_role;

-- @teardown
drop database test_nested_correlated_scalar;
