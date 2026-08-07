-- fulltext2 INCLUDE covered fast path (Phase 6): a fully-covered query (projection =
-- pk/score/include cols, all filters peeled into the TVF, single MATCH) drops the
-- base-table JOIN and reads pk/score/include values straight from fulltext2_search
-- (plan: Project -> Sort(score DESC) -> Table Function, no Join / no base Table Scan).
-- These cases exercise the covered-path specifics that the two P6 bugs lived in:
--   * column pruning: projecting a SUBSET of the include cols (the unprojected one is
--     pruned from the TVF output — the runtime must map the rest BY NAME, not position);
--   * reversed projection order vs the index INCLUDE order;
--   * streaming (no-LIMIT) vs non-streaming (LIMIT) include emission.
-- Every result MUST equal the plain base-table semantics.
set experimental_fulltext2_index = 1;
drop database if exists fulltext2_covered;
create database fulltext2_covered;
use fulltext2_covered;

create table docs (
  id bigint primary key,
  body text not null,
  status varchar(20),           -- INCLUDE
  prio int,                     -- INCLUDE
  category varchar(20) not null -- NOT included
);
insert into docs values
(1, 'quick brown fox',      'active',   10, 'tech'),
(2, 'lazy fox sleeps',      'archived', 20, 'food'),
(3, 'red fox runs fast',    'active',   30, 'tech'),
(4, 'fox fox everywhere',   'active',   40, 'tech'),
(5, 'quiet sleeping cat',   'active',   50, 'home'),
(6, 'a clever silver fox',  null,       60, 'tech'),
(7, 'the lazy brown dog',   'archived', null, 'home');

create fulltext2 index ftidx on docs (body) include (status, prio);

-- covered, project ONE include col (prio) -> the other (status) is pruned from the TVF
-- output; streaming (no LIMIT). This is the column-pruning regression case.
select id, prio from docs where match(body) against('fox') and prio > 5 order by id;

-- covered, project the OTHER include col (status) -> prio pruned.
select id, status from docs where match(body) against('fox') and prio > 5 order by id;

-- covered, all include cols.
select id, status, prio from docs where match(body) against('fox') order by id;

-- covered, projection order REVERSED vs INCLUDE(status, prio) order -> name mapping.
select id, prio, status from docs where match(body) against('fox') order by id;

-- covered, pk-only projection.
select id from docs where match(body) against('fox') order by id;

-- covered with LIMIT (non-streaming include path).
select id, prio from docs where match(body) against('fox') and prio > 5 order by id limit 3;

-- covered numeric prefilter variety (all peel into the TVF), projecting include cols.
select id, prio from docs where match(body) against('fox') and prio = 30 order by id;
select id, prio from docs where match(body) against('fox') and prio between 15 and 45 order by id;
select id, prio from docs where match(body) against('fox') and prio in (10, 40, 60) order by id;

-- covered pk prefilter (pk sentinel) + include projection.
select id, status from docs where match(body) against('fox') and id in (1, 4, 6) order by id;

-- covered NULL include values surface as SQL NULL (doc 6 status, doc 7 prio).
select id, status, prio from docs where match(body) against('fox') and (status is null or prio is null) order by id;

-- covered aggregate over the fast path.
select count(*) from docs where match(body) against('fox') and prio > 25;

drop database fulltext2_covered;
