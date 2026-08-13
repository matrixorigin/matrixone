-- fulltext2 INCLUDE columns: the actual typed value of chosen scalar columns is
-- stored inside the index (docmap), so a WHERE predicate on an INCLUDE column is
-- evaluated inside fulltext2_search (in-index prefilter) and a SELECT of one is
-- served from the index (covering projection) -- no base-table JOIN. This case
-- pins correctness against the plain base-table semantics: every MATCH+INCLUDE
-- query MUST return exactly what the same predicate over the base rows gives.
-- Covers: covering projection, numeric prefilter (= > BETWEEN IN), varchar
-- prefilter (= LIKE prefix), primary-key prefilter, NULL 3-valued logic, a
-- NON-included residual filter, and DDL round-trip (SHOW CREATE).
set experimental_fulltext2_index = 1;
drop database if exists fulltext2_include;
create database fulltext2_include;
use fulltext2_include;

-- ============================================================================
-- INCLUDE validation guards (all rejected at CREATE): the primary key, an
-- indexed text column (single or one of several), an unsupported column type,
-- and a duplicate column.
-- ============================================================================
create table g (id bigint primary key, body text not null, title varchar(100), status varchar(20), prio int, amt decimal(10,2), created date);
create fulltext2 index gi1 on g (body) include (id);             -- pk
create fulltext2 index gi2 on g (body) include (body);           -- indexed text column
create fulltext2 index gi3 on g (body, title) include (title);   -- indexed column (multi-column index)
create fulltext2 index gi4 on g (body) include (amt);            -- unsupported type: decimal
create fulltext2 index gi5 on g (body) include (created);        -- unsupported type: date
create fulltext2 index gi6 on g (body) include (status, status); -- duplicate
drop table g;

create table docs (
  id bigint primary key,
  body text not null,
  status varchar(20),          -- INCLUDE (varchar, nullable)
  prio int,                    -- INCLUDE (numeric, nullable)
  category varchar(20) not null -- NOT included -> residual post-filter
);
insert into docs values
(1, 'machine learning basics',        'active',   10, 'tech'),
(2, 'deep learning networks',         'active',   20, 'tech'),
(3, 'learning to cook french food',   'archived', 30, 'food'),
(4, 'machine learning for finance',   'active',   40, 'finance'),
(5, 'reinforcement learning games',   null,       50, 'tech'),
(6, 'learning sports analytics',      'active',   null, 'sports'),
(7, 'transfer learning models',       'pending',  15, 'tech'),
(8, 'classical cooking learning',     'archived', 25, 'food');

create fulltext2 index ftidx on docs (body) include (status, prio);

-- DDL round-trip: the INCLUDE clause is reconstructed by SHOW CREATE (a
-- rebuild from clause-less DDL would silently drop the covering columns).
show create table docs;

-- covering projection: status/prio served from the index (incl. NULLs), no JOIN.
select id, status, prio from docs where match(body) against('learning') order by id;

-- numeric include prefilter (peeled into the TVF): = > BETWEEN IN.
select id, prio from docs where match(body) against('learning') and prio = 20 order by id;
select id, prio from docs where match(body) against('learning') and prio > 25 order by id;
select id, prio from docs where match(body) against('learning') and prio between 15 and 30 order by id;
select id, prio from docs where match(body) against('learning') and prio in (10, 40) order by id;

-- varchar include prefilter: equality + LIKE prefix (real value stored, so both work).
select id, status from docs where match(body) against('learning') and status = 'active' order by id;
select id, status from docs where match(body) against('learning') and status like 'arch%' order by id;

-- primary-key prefilter (pk sentinel col=-1): IN and range.
select id from docs where match(body) against('learning') and id in (1, 3, 5) order by id;
select id from docs where match(body) against('learning') and id > 6 order by id;

-- NULL include values: 3-valued predicate logic + projection emits SQL NULL.
select id, status from docs where match(body) against('learning') and status is null order by id;
select id, prio from docs where match(body) against('learning') and prio is null order by id;
select id, status from docs where match(body) against('learning') and status is not null order by id;

-- NON-included column (category) -> residual post-filter, still correct.
select id, category from docs where match(body) against('learning') and category = 'tech' order by id;

-- mixed: varchar include + numeric include + non-included residual together.
select id, status, prio, category from docs where match(body) against('learning') and status = 'active' and prio > 15 order by id;
select id from docs where match(body) against('learning') and status = 'active' and category = 'tech' order by id;

-- aggregate over an in-index prefilter.
select count(*) from docs where match(body) against('learning') and status = 'active';
select count(*) from docs where match(body) against('learning') and prio between 15 and 30;

drop database fulltext2_include;
