-- issue #27981: sum/avg over a BOOL argument under the mysql_compatible flag.
-- MySQL has no BOOL type, so a predicate there is an integer 0/1 and SUM/AVG
-- over one is ordinary numeric aggregation. MO types the predicate as BOOL and
-- rejects it. mysql_compatible opts in to the MySQL reading.
drop database if exists mysql_compatible_bool_agg;
create database mysql_compatible_bool_agg;
use mysql_compatible_bool_agg;
create table t (i int, j int);
insert into t values (0, 0), (1, 1), (2, 2);

-- the flag is off by default and the strict typing is unchanged
select @@mysql_compatible;
select sum(i<>0) from t;
select avg(i<>0) from t;

set mysql_compatible = 1;
select @@mysql_compatible;

-- the MySQL reading: identical to the explicit cast a user writes today
select sum(i<>0) from t;
select sum(cast(i<>0 as tinyint)) from t;
select avg(i<>0) from t;
select avg(cast(i<>0 as tinyint)) from t;

-- sum(bool) -> bigint and avg(bool) -> double, matching MO's own sum(tinyint)
drop table if exists ctas_types;
create table ctas_types as select sum(i<>0) as s, avg(i<>0) as a from t;
select column_name, data_type from information_schema.columns where table_schema = 'mysql_compatible_bool_agg' and table_name = 'ctas_types' order by column_name;
select * from ctas_types;

-- NULL predicates are skipped, exactly as MySQL skips NULL SUM/AVG inputs
create table n (i int);
insert into n values (0), (1), (null), (2);
select sum(i<>0), avg(i<>0), count(i<>0) from n;

-- no rows aggregates to NULL, not 0
create table e (i int);
select sum(i<>0), avg(i<>0) from e;

-- a BOOL column reads the same way as a predicate
create table b (x bool);
insert into b values (true), (false), (true);
select sum(x), avg(x) from b;

-- constants, DISTINCT, GROUP BY, HAVING, window and subquery positions
select sum(true) from t;
select sum(distinct i<>0) from t;
select j, sum(i<>0) from t group by j order by j;
select j from t group by j having sum(i<>0) > 0 order by j;
select sum(i<>0) over () from t limit 1;
select (select sum(i<>0) from t) as scalar_subquery;

-- INSERT ... SELECT replays the aggregate through a second compilation
create table dst (s bigint);
insert into dst select sum(i<>0) from t;
select * from dst;

-- the flag relaxes SUM and AVG only. Aggregates that already accepted BOOL are
-- unchanged, and those outside its scope still reject it.
select min(i<>0), max(i<>0), count(i<>0) from t;
select bit_and(i<>0) from t;
select bit_or(i<>0) from t;

-- only the BOOL argument is coerced; other rejected types stay rejected
select sum(i) from t;
select sum(cast(i as char)) from t;

-- turning the flag back off restores the strict typing in the same session
set mysql_compatible = 0;
select sum(i<>0) from t;
select avg(i<>0) from t;
create table ctas_off as select sum(i<>0) as s from t;
select min(i<>0), max(i<>0) from t;

drop database mysql_compatible_bool_agg;
