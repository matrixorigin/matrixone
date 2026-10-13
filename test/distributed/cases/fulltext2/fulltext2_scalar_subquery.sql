-- Regression for #27962: a MATCH() AGAINST() inside a scalar subquery was not
-- rewritten to the fulltext2 index and failed with error 20105, even though the
-- same predicate works at top level, in IN subqueries, and in derived tables.
-- The fix anchors the rewrite on the AGG node (uncorrelated: JOIN->AGG->SCAN) and
-- on LEFT/SINGLE join children (correlated: AGG->JOIN->SCAN). fulltext2 CREATE
-- builds synchronously from existing rows, so these queries are deterministic.
set experimental_fulltext2_index = 1;
drop database if exists fulltext2_scalar_subquery;
create database fulltext2_scalar_subquery;
use fulltext2_scalar_subquery;

create table docs(id bigint primary key, body text, status int, prio int);
insert into docs values
  (1, 'ready token',  1, 100),
  (2, 'ready steady', 0, 200),
  (3, 'ready go',     1, 300),
  (4, 'other word',   1, 400);
create fulltext2 index ft on docs(body) include(status, prio);

-- Baselines that already worked (must keep working).
select id from docs where match(body) against('ready') order by id;
select id from docs where id in (select id from docs where match(body) against('ready')) order by id;
select id from (select id from docs where match(body) against('ready')) q order by id;

-- The #27962 repro: uncorrelated scalar subquery (was error 20105, expect 3).
select (select count(*) from docs where match(body) against('ready')) as scalar_count;

-- Every aggregate function over a MATCH scalar subquery.
select (select max(prio) from docs where match(body) against('ready')) as mx;   -- 300
select (select min(prio) from docs where match(body) against('ready')) as mn;   -- 100
select (select sum(prio) from docs where match(body) against('ready')) as s;    -- 600
select (select avg(prio) from docs where match(body) against('ready')) as a;    -- 200

-- Wrapped MATCH (no bare match anywhere) in a scalar subquery (expect 3).
select (select count(*) from docs where match(body) against('ready') > 0) as wrapped_count;

-- In-index prefilter on an INCLUDE column inside the scalar subquery (status=1 -> ids 1,3).
select (select count(*) from docs where match(body) against('ready') and status = 1) as inc_count;
-- Aggregating an INCLUDE column over the match (prio of ids 1,3 = 400).
select (select sum(prio) from docs where match(body) against('ready') and status = 1) as inc_sum;

-- HAVING on a match-filtered aggregate.
select count(*) as c from docs where match(body) against('ready') having count(*) > 0;

-- Correlated scalar subquery: correlation in WHERE (AGAINST needs a literal), with
-- an INCLUDE-column prefilter. cat.cid 1 -> ids{1,3}, cid 3 -> id{3}.
create table cat(cid int primary key);
insert into cat values (1), (3);
select cid,
       (select count(*) from docs where match(body) against('ready') and status = 1 and docs.id >= cat.cid) as c
from cat order by cid;

drop database fulltext2_scalar_subquery;
