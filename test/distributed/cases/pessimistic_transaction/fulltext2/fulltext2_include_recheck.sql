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

-- More stale top candidates (15) than the LIMIT 1 over-fetch alone: the search keeps room for
-- every row the transaction deleted, so the lower-ranked live match 120 still fills the page.
create table rk2 (id bigint primary key, body text, tag varchar(10));
insert into rk2 select result, 'needle needle needle', 'top' from generate_series(101, 115) g;
insert into rk2 values (120, 'needle filler filler', 'live');
insert into rk2 select result, 'other filler filler', 'other' from generate_series(200, 240) g;
create fulltext2 index rk2x on rk2(body) with parser gojieba;
begin;
delete from rk2 where id between 101 and 115;
select id, tag from rk2 where match(body) against('needle') order by match(body) against('needle') desc limit 1;
select id, tag from rk2 where match(body) against('needle') limit 1;
rollback;

drop database fulltext2_include_recheck;
