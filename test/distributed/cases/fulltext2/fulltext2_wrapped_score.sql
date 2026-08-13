-- Relevance algorithm pinned FIRST: every score value and threshold below depends on it.
set ft2_relevancy_algorithm="BM25";
-- MATCH() returns a FLOAT relevance score, so it can be wrapped like any other scalar
-- expression: round(match(...), 3), cast(match(...) as decimal), match(...) * 100, or a
-- comparison on it. The rewrite has to see THROUGH that wrapping, because the placeholder it
-- replaces has no evaluable implementation -- left inside a cast or a comparison it reaches
-- execution and raises "MATCH() AGAINST() function cannot be replaced by FULLTEXT INDEX".
--
-- This is the fulltext twin of vector_ivf_wrapped_dist (#26961), which pins the same shapes
-- for wrapped vector distances; the expression walk is shared between the two.
--
-- Every EXPLAIN asserts the index is still reached, so a regression that "fixes" these by
-- silently falling back to a scan fails here too.
set experimental_fulltext2_index = 1;

drop database if exists ft2_wrapped;
create database ft2_wrapped;
use ft2_wrapped;

create table docs(id int primary key, body text);
insert into docs values (1,'hello a'),(2,'other text'),(3,'hello hello b');
create fulltext2 index ft on docs(body);

-- ---------------- baseline: the bare score ------------------------------------
select id, match(body) against('hello') as sc from docs where match(body) against('hello') order by id;

-- ---------------- wrapped in the PROJECTION -----------------------------------
select id, round(match(body) against('hello'), 3) as r from docs where match(body) against('hello') order by id;
select id, cast(match(body) against('hello') as decimal(10,3)) as r from docs where match(body) against('hello') order by id;
select id, match(body) against('hello') * 100 as r from docs where match(body) against('hello') order by id;
select id, round(match(body) against('hello') + 1, 3) as r from docs where match(body) against('hello') order by id;

-- ordering by the wrapped alias must still rank by relevance
select id, round(match(body) against('hello'), 3) as r from docs where match(body) against('hello') order by r desc;

-- @separator:table
-- @regex("Table Function on fulltext2_search", true)
explain select id, round(match(body) against('hello'), 3) as r from docs where match(body) against('hello');

-- ---------------- wrapped in a FILTER above a view ----------------------------
-- Thresholds are chosen to EXCLUDE a row. `sc > 0` is satisfied by every matching row, so a
-- predicate that is silently never applied returns the same rows as a correct one -- which is
-- exactly how an earlier version of this fix passed while attaching the lifted predicate to a
-- plan field nothing evaluates. Keep one `> 0` as a baseline; the rest must change the row set.
-- Inlining substitutes the score alias with its definition and pushes it onto the base scan,
-- where the bare-match test cannot see it. It is lifted onto the join instead.
create view v as select id, match(body) against('hello') as sc from docs where match(body) against('hello');

select id from v where sc > 0 order by id;              -- baseline: every match
select id from v where sc > 0.037 order by id;          -- only the higher scorer
select id from v where round(sc, 4) > 0.037 order by id;
select id from v where cast(sc as double) > 0.037 order by id;
select id from v where sc * 2 > 0.074 order by id;
select id from v where sc > 0.037 and id > 1 order by id;
select id from v where round(cast(sc as double) * 2, 3) > 0.074 order by id;

-- @separator:table
-- @regex("Table Function on fulltext2_search", true)
explain select id from v where round(sc, 4) > 0;

-- ---------------- DISCRIMINATING thresholds ------------------------------------
-- `sc > 0` is worthless as a test: every matching row satisfies it, so a predicate that is
-- silently never applied returns exactly the same rows as a correct one. That is how an
-- earlier version of this fix -- which attached the lifted predicate to a Node_JOIN
-- FilterList, a field nothing evaluates -- passed a whole suite of shapes while returning
-- rows the predicate excluded. These thresholds must CHANGE the row set.
select id from v where sc > 0.037 order by id;      -- only the higher-scoring row
select id from v where sc > 0.5 order by id;     -- none
select id from v where round(sc, 4) > 0.037 order by id;
select id from v where cast(sc as double) > 0.5 order by id;

-- the same shape with no view at all
select id from (
    select id, match(body) against('hello') as sc from docs where match(body) against('hello')
) x where sc > 0 order by id;

drop database ft2_wrapped;

-- Restore the default so this case does not leak its setting to whatever runs next --
-- the failure mode that made an earlier version of this file order-dependent.
set ft2_relevancy_algorithm="BM25";
