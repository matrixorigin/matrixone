-- issue #27981, #28078: sum/avg over a BOOL argument under ENABLE_BOOL_SUMAVG
-- sql_mode. MySQL has no BOOL type, so a predicate there is an integer 0/1 and
-- SUM/AVG over one is ordinary numeric aggregation. MO types the predicate as
-- BOOL. ENABLE_BOOL_SUMAVG selects the MySQL reading and is enabled by default.
drop database if exists bool_sumavg_sqlmode;
create database bool_sumavg_sqlmode;
use bool_sumavg_sqlmode;
create table t (i int, j int);
insert into t values (0, 0), (1, 1), (2, 2);

-- BVT reuses connections between case files. Re-enter DEFAULT so these
-- assertions prove the product default rather than a preceding case's setting.
set session sql_mode = default;

-- the mode is on by default, including the JSON expression from issue #28078
select @@sql_mode;
select sum(i<>0) from t;
select avg(i<>0) from t;
select sum(json_unquote(json_extract(cast('{"code":"v1"}' as json), '$.code')) = 'v1');

-- one session opting out must neither alter a second session's default nor
-- prevent DEFAULT from restoring the compatibility behavior in the first.
-- @session:id=1{
use bool_sumavg_sqlmode;
select sum(json_unquote(json_extract(cast('{"code":"v1"}' as json), '$.code')) = 'v1');
-- @session}
set session sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';
-- @session:id=1{
select sum(json_unquote(json_extract(cast('{"code":"v1"}' as json), '$.code')) = 'v1');
-- @session}
set session sql_mode = default;
select sum(json_unquote(json_extract(cast('{"code":"v1"}' as json), '$.code')) = 'v1');

-- the mode composes with the modes the session already carries
set session sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ENABLE_BOOL_SUMAVG';
select @@sql_mode;

-- the MySQL reading: identical to the explicit cast a user writes today
select sum(i<>0) from t;
select sum(cast(i<>0 as tinyint)) from t;
select avg(i<>0) from t;
select avg(cast(i<>0 as tinyint)) from t;

-- sum(bool) -> bigint and avg(bool) -> decimal, matching the exact numeric
-- AVG contract for the coerced TINYINT argument.
drop table if exists ctas_types;
create table ctas_types as select sum(i<>0) as s, avg(i<>0) as a from t;
select column_name, data_type from information_schema.columns where table_schema = 'bool_sumavg_sqlmode' and table_name = 'ctas_types' order by column_name;
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

-- the mode relaxes SUM and AVG only. Aggregates that already accepted BOOL are
-- unchanged, and those outside its scope still reject it.
select min(i<>0), max(i<>0), count(i<>0) from t;
select bit_and(i<>0) from t;
select bit_or(i<>0) from t;

-- only the BOOL argument is coerced; other rejected types stay rejected
select sum(i) from t;
select sum(cast(i as char)) from t;

-- dropping the mode restores the strict typing in the same session. The
-- statement text is repeated first so a plan cached under the mode is on
-- record: the session plan cache must not answer it once the mode is gone.
select sum(i<>0) from t;
select sum(i<>0) from t;
set session sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';
select sum(i<>0) from t;
select avg(i<>0) from t;
create table ctas_off as select sum(i<>0) as s from t;
select min(i<>0), max(i<>0) from t;

-- prepared statements take the same decision as direct queries, and EXECUTE
-- follows the session's current mode rather than the PREPARE-time one
prepare strict_stmt from 'select sum(i<>0) from t';
set session sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ENABLE_BOOL_SUMAVG';
prepare relaxed_stmt from 'select sum(i<>0) from t';
execute relaxed_stmt;
set session sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';
execute relaxed_stmt;
set session sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ENABLE_BOOL_SUMAVG';
execute relaxed_stmt;
deallocate prepare relaxed_stmt;

-- restore the session default. mo-tester reuses one connection across case
-- files, so a case that changes sql_mode must reset it or the next case sees
-- the leftover value.
set session sql_mode = default;

drop database bool_sumavg_sqlmode;
