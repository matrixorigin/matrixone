-- fulltext2 INCLUDE covered fast path (Phase 6): a fully-covered query (projection =
-- pk/score/include cols, all filters peeled into the TVF, single MATCH) drops the
-- base-table JOIN and reads pk/score/include values straight from fulltext2_search
-- (plan: Project -> Sort(score DESC) -> Table Function, no Join / no base Table Scan).
-- These cases exercise the covered-path specifics that the two P6 bugs lived in:
--   * column pruning: projecting a SUBSET of the include cols (the unprojected one is
--     pruned from the TVF output — the runtime must map the rest BY NAME, not position);
--   * reversed projection order vs the index INCLUDE order;
--   * streaming (no-LIMIT) vs non-streaming (LIMIT) include emission.
-- Ordering: fulltext is score-ranked, so the covered path is shown working WITHOUT an
-- explicit ORDER BY (the planner adds the score-DESC sort), and multi-row results are made
-- deterministic with `ORDER BY match(...) DESC, id` (score-primary; id is only a tiebreaker,
-- NOT an ORDER BY id that would hide score-path behavior). Every result MUST equal the plain
-- base-table semantics.
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

-- The covered fast path fires with NO explicit ORDER BY: the planner adds the score-DESC
-- sort and the rewrite reads pk/score/include straight from the TVF -> no Join, no base
-- Table Scan (this is the plan shape the covered path is all about).
explain select id, status, prio from docs where match(body) against('fox');

-- ... and it EXECUTES correctly without an explicit ORDER BY. Single-doc match ('silver' is
-- only in doc 6) so the result is order-independent and deterministic.
select id, status, prio from docs where match(body) against('silver');

-- covered, project ONE include col (prio) -> the other (status) is pruned from the TVF
-- output; streaming (no LIMIT). This is the column-pruning regression case.
select id, prio from docs where match(body) against('fox') and prio > 5 order by match(body) against('fox') desc, id;

-- covered, project the OTHER include col (status) -> prio pruned.
select id, status from docs where match(body) against('fox') and prio > 5 order by match(body) against('fox') desc, id;

-- covered, all include cols.
select id, status, prio from docs where match(body) against('fox') order by match(body) against('fox') desc, id;

-- covered, projection order REVERSED vs INCLUDE(status, prio) order -> name mapping.
select id, prio, status from docs where match(body) against('fox') order by match(body) against('fox') desc, id;

-- covered, pk-only projection.
select id from docs where match(body) against('fox') order by match(body) against('fox') desc, id;

-- covered with LIMIT (non-streaming include path).
select id, prio from docs where match(body) against('fox') and prio > 5 order by match(body) against('fox') desc, id limit 3;

-- covered numeric prefilter variety (all peel into the TVF), projecting include cols.
select id, prio from docs where match(body) against('fox') and prio = 30 order by match(body) against('fox') desc, id;
select id, prio from docs where match(body) against('fox') and prio between 15 and 45 order by match(body) against('fox') desc, id;
select id, prio from docs where match(body) against('fox') and prio in (10, 40, 60) order by match(body) against('fox') desc, id;

-- covered pk prefilter (pk sentinel) + include projection.
select id, status from docs where match(body) against('fox') and id in (1, 4, 6) order by match(body) against('fox') desc, id;

-- covered NULL include values surface as SQL NULL (doc 6 status, doc 7 prio).
select id, status, prio from docs where match(body) against('fox') and (status is null or prio is null) order by match(body) against('fox') desc, id;

-- covered aggregate over the fast path (no ORDER BY needed — order-independent).
select count(*) from docs where match(body) against('fox') and prio > 25;

-- covered VARCHAR include prefilter (peeled into the TVF -> 0-join): =, IN, LIKE prefix.
select id, status from docs where match(body) against('fox') and status = 'active' order by match(body) against('fox') desc, id;
select id, status from docs where match(body) against('fox') and status in ('active', 'archived') order by match(body) against('fox') desc, id;
select id, status from docs where match(body) against('fox') and status like 'ar%' order by match(body) against('fox') desc, id;
-- covered mixed: varchar = + numeric range (both peel).
select id, status, prio from docs where match(body) against('fox') and status = 'active' and prio > 15 order by match(body) against('fox') desc, id;

-- ORDER BY sort-key NOT in the SELECT list: the binder projects it as a hidden INTERNAL
-- column, so coverage still applies. An ORDER BY on an INCLUDE col that is not selected stays
-- COVERED (0-join): status is not projected here yet the plan still reads it from the TVF.
explain select id from docs where match(body) against('fox') order by status;

-- FALLBACK: an ORDER BY on a NON-covered base column (category is not an include col, and is
-- not even in the SELECT list) is NOT covered — it declines the fast path and uses the 2-JOIN
-- path (base Table Scan + Join on doc_id), which still returns correct rows. The EXPLAIN shows
-- the Join; the result matches.
explain select id, status from docs where match(body) against('fox') order by category;
select id, status, category from docs where match(body) against('fox') order by category, id;

-- byte-exact: MO compares varchar byte-exact regardless of declared collation, so a covered
-- '=' / LIKE matches only the exact bytes (case-sensitive), NOT case-folded.
create table cs (id bigint primary key, body text not null, tag varchar(10));
insert into cs values (1, 'red fox', 'Active'), (2, 'blue fox', 'active'), (3, 'grey fox', 'ACTIVE');
create fulltext2 index csidx on cs (body) include (tag);
select id, tag from cs where match(body) against('fox') and tag = 'active' order by match(body) against('fox') desc, id;  -- only id 2
select id, tag from cs where match(body) against('fox') and tag like 'Act%' order by match(body) against('fox') desc, id; -- only id 1
drop table cs;

drop database fulltext2_covered;
