-- Regression: a master-index prefix_in payload must be published in ascending
-- order, or block pruning drops every block and the query returns nothing.
--
-- The prefix_in filter is compiled by readutil (compileFilterExprs, reached from
-- exec_util.go) into seek/stop bounds taken POSITIONALLY from the payload:
-- minPrefix = col[0], maxPrefix = col[len-1]. Out of order, maxPrefix is not the
-- maximum, anyPrefixLEByValue() excludes every block, and EXPLAIN ANALYZE shows
-- inputBlocks=0 on the index scan.
--
-- Two things are needed to reach it, and both are cheap:
--   * NULL in the IN list. Constant folding sorts an IN list before the planner
--     builds the index scan, but deliberately skips a NULLable one to keep its
--     null bitmap aligned, so this is the only literal form that stays unordered.
--   * Narrow zone maps, which come from flush boundaries rather than row counts.
--     Insert-then-flush per value and two rows are enough; without the flushes
--     one wide zone map covers everything and nothing is pruned.
--
-- t_ctl carries the same rows with no index and is the oracle: every pair below
-- must agree.

drop database if exists master_null_in_prune;
create database master_null_in_prune;
use master_null_in_prune;

create table t (id int primary key, a varchar(20));
create index idx using master on t(a);

set @tid = (select rel_id from mo_catalog.mo_tables where relname = 't' and reldatabase = database() limit 1);
set @idx = (select index_table_name from mo_catalog.mo_indexes where table_id = @tid and name = 'idx' limit 1);

insert into t values (1,'a');
-- @separator:table
select mo_ctl('dn','flush', concat(database(), '.t'));
-- @separator:table
select mo_ctl('dn','flush', concat(database(), '.', @idx));

insert into t values (2,'z');
-- @separator:table
select mo_ctl('dn','flush', concat(database(), '.t'));
-- @separator:table
select mo_ctl('dn','flush', concat(database(), '.', @idx));

create table t_ctl (id int primary key, a varchar(20));
insert into t_ctl values (1,'a'),(2,'z');

-- One real value plus NULL: the smallest form that reproduces it. Returned 0 rows
-- before the producer sorted its payload.
select id, a from t where a in ('a', NULL) order by id;
select id, a from t_ctl where a in ('a', NULL) order by id;

-- Two values plus NULL, in both written orders. Neither is reordered by folding.
select id, a from t where a in ('a','z', NULL) order by id;
select id, a from t_ctl where a in ('a','z', NULL) order by id;
select id, a from t where a in ('z','a', NULL) order by id;
select id, a from t_ctl where a in ('z','a', NULL) order by id;

-- Controls that never regressed: folding sorts a NULL-free list, so the payload
-- was already ascending whichever order it was written in.
select id, a from t where a in ('a','z') order by id;
select id, a from t where a in ('z','a') order by id;

-- A needle matching nothing must still prune correctly rather than over-return.
select id, a from t where a in ('q', NULL) order by id;
select id, a from t_ctl where a in ('q', NULL) order by id;

drop database master_null_in_prune;
