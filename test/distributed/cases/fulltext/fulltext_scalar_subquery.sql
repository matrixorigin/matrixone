-- Regression for #27962 on CLASSIC fulltext: a MATCH() AGAINST() inside a scalar
-- subquery was not rewritten to the fulltext index and failed with error 20105,
-- although it works at top level, in IN subqueries, and in derived tables. The
-- fix is algo-agnostic (shared pkg/sql/plan apply path); this pins the classic
-- fulltext engine. (INCLUDE columns are a fulltext2-only feature, so not covered
-- here -- see cases/fulltext2/fulltext2_scalar_subquery for the INCLUDE matrix.)
drop database if exists fulltext_scalar_subquery;
create database fulltext_scalar_subquery;
use fulltext_scalar_subquery;

create table docs(id bigint primary key, body text, score int);
insert into docs values
  (1, 'ready token',  100),
  (2, 'ready steady', 200),
  (3, 'ready go',     300),
  (4, 'other word',   400);
create fulltext index ft on docs(body);

-- Baselines that already worked (must keep working).
select id from docs where match(body) against('ready') order by id;
select id from docs where id in (select id from docs where match(body) against('ready')) order by id;
select id from (select id from docs where match(body) against('ready')) q order by id;

-- The #27962 repro: uncorrelated scalar subquery (was error 20105, expect 3).
select (select count(*) from docs where match(body) against('ready')) as scalar_count;

-- Every aggregate function over a MATCH scalar subquery.
select (select max(score) from docs where match(body) against('ready')) as mx;   -- 300
select (select min(score) from docs where match(body) against('ready')) as mn;   -- 100
select (select sum(score) from docs where match(body) against('ready')) as s;    -- 600
select (select avg(score) from docs where match(body) against('ready')) as a;    -- 200

-- Wrapped MATCH (no bare match anywhere) in a scalar subquery (expect 3).
select (select count(*) from docs where match(body) against('ready') > 0) as wrapped_count;

-- HAVING on a match-filtered aggregate.
select count(*) as c from docs where match(body) against('ready') having count(*) > 0;

-- Correlated scalar subquery: correlation in WHERE (AGAINST needs a literal).
-- cat.cid 1 -> ids{1,2,3}, cid 3 -> ids{3}.
create table cat(cid bigint primary key);
insert into cat values (1), (3);
select cid,
       (select count(*) from docs where match(body) against('ready') and docs.id >= cat.cid) as c
from cat order by cid;

drop database fulltext_scalar_subquery;
