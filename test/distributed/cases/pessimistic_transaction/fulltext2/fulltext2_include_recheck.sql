-- A fulltext2 search evaluates INCLUDE-column predicates on index-time values. The base
-- table scan must re-check them on current values, so an in-transaction UPDATE the index
-- cannot see never returns a row that violates the WHERE clause.
set experimental_fulltext2_index = 1;
drop database if exists fulltext2_include_recheck;
create database fulltext2_include_recheck;
use fulltext2_include_recheck;
create table t (id bigint primary key, body text, category int, tag varchar(10));
insert into t values (1, 'needle a', 1, 'x'), (2, 'needle b', 1, 'y'), (3, 'needle c', 2, 'x'), (4, 'needle d', 1, 'x');
create fulltext2 index ftx on t(body) include(category) with parser gojieba;

set fulltext_bloom_filter_pushdown = 0;
begin;
update t set category = 2 where id = 4;
-- @sortkey:0
select id, category, tag from t where match(body) against('needle') and category = 1;
-- @sortkey:0
select id, category, tag from t where match(body) against('needle') and category = 1 limit 10;
rollback;

set fulltext_bloom_filter_pushdown = 1;
begin;
update t set category = 2 where id = 4;
-- @sortkey:0
select id, category, tag from t where match(body) against('needle') and category = 1 and tag = 'x';
rollback;
set fulltext_bloom_filter_pushdown = 0;

-- @sortkey:0
select id, category, tag from t where match(body) against('needle') and category = 1;
-- @regex("Filter Cond: \(t.category = 1\)",true)
explain select id, category, tag from t where match(body) against('needle') and category = 1;

-- Competing scores (id 11 > 12 > 13) under LIMIT 1: an in-transaction DELETE or INCLUDE
-- UPDATE leaves the top candidate stale in the index. The base-table join drops it, and the
-- bounded search over-fetches so the next qualifying row still fills the page.
create table rk (id bigint primary key, body text, category int, tag varchar(10));
insert into rk values (11, 'needle needle needle', 1, 'a'), (12, 'needle needle filler', 1, 'b'), (13, 'needle filler filler', 1, 'c'),
(14, 'other filler filler', 1, 'd'), (15, 'other other filler', 1, 'e');
create fulltext2 index rkx on rk(body) include(category) with parser gojieba;
-- @regex("Candidate Limit: 1",true)
explain select id, tag from rk where match(body) against('needle') order by match(body) against('needle') desc limit 1;
-- @regex("Candidate Limit: 1",true)
explain select id, tag from rk where match(body) against('needle') and category = 1 limit 1;
begin;
delete from rk where id = 11;
select id, tag from rk where match(body) against('needle') order by match(body) against('needle') desc limit 1;
select id, tag from rk where match(body) against('needle') limit 1;
select id, tag from rk where match(body) against('needle') order by match(body) against('needle') desc limit 1 offset 1;
rollback;
begin;
update rk set category = 2 where id = 11;
select id, tag from rk where match(body) against('needle') and category = 1 order by match(body) against('needle') desc limit 1;
select id, tag from rk where match(body) against('needle') and category = 1 limit 1;
rollback;
select id, match(body) against('needle') > 0 as positive from rk where match(body) against('needle') order by match(body) against('needle') desc;

-- Each statement of the design doc's "Index lag (decision)" section, checked from SQL.
-- A transaction's uncommitted writes are never in the index, so they hold the index
-- behind the source deterministically.
create table rk2 (id bigint primary key, body text, tag varchar(10));
insert into rk2 select result, 'needle needle needle', 'top' from generate_series(101, 115) g;
insert into rk2 values (120, 'needle filler filler', 'live');
insert into rk2 select result, 'other filler filler', 'other' from generate_series(200, 240) g;
create fulltext2 index rk2x on rk2(body) with parser gojieba;

-- Rows deleted from the source but still in the index are dropped by the join; with more of
-- them than the over-fetch the bounded page is short. The same query with a secondary ORDER
-- BY key is not bounded and returns the live match 120.
begin;
delete from rk2 where id between 101 and 115;
-- @regex("Candidate Limit: 1",true)
explain select id, tag from rk2 where match(body) against('needle') order by match(body) against('needle') desc limit 1;
select id, tag from rk2 where match(body) against('needle') order by match(body) against('needle') desc limit 1;
select id, tag from rk2 where match(body) against('needle') limit 1;
-- @regex("Candidate Limit",false)
explain select id, tag from rk2 where match(body) against('needle') order by match(body) against('needle') desc, id limit 1;
select id, tag from rk2 where match(body) against('needle') order by match(body) against('needle') desc, id limit 1;
-- Selecting only the primary key still joins the source table.
-- @regex("Table Scan on fulltext2_include_recheck.rk2",true)
explain select id from rk2 where match(body) against('needle') order by match(body) against('needle') desc limit 1;
select id from rk2 where match(body) against('needle') order by match(body) against('needle') desc limit 1;
rollback;

-- A covered search (primary key and INCLUDE columns only) has no join: it returns rows and
-- INCLUDE values as indexed, including a row deleted from the source (11) and the old
-- INCLUDE value of an updated row (12). The source table shows the current values.
begin;
delete from rk where id = 11;
update rk set category = 2 where id = 12;
-- @regex("Table Scan",false)
explain select id, category from rk where match(body) against('needle') order by match(body) against('needle') desc;
select id, category from rk where match(body) against('needle') order by match(body) against('needle') desc;
select id, category from rk where id in (11, 12, 13) order by id;
rollback;

-- A row not in the index is not matched; LIKE on the source sees it.
begin;
insert into rk2 values (130, 'needle needle needle needle', 'new');
select id from rk2 where match(body) against('needle') and id = 130;
select id from rk2 where body like '%needle%' and id = 130;
rollback;

-- A row whose text changed is matched and scored by its indexed text.
begin;
update rk2 set body = 'other filler filler' where id = 120;
select id, tag from rk2 where match(body) against('needle') and tag = 'live';
select id, body like '%needle%' as has_needle from rk2 where id = 120;
rollback;

drop database fulltext2_include_recheck;
