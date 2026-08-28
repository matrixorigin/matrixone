-- Relevance algorithm pinned FIRST: it is a session variable other cases in this suite
-- change (fulltext_bm25 sets BM25), and every score value and threshold below depends on it.
set ft_relevancy_algorithm="TF-IDF";
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
set experimental_fulltext_index = 1;

drop database if exists ft_wrapped;
create database ft_wrapped;
use ft_wrapped;

create table docs(id int primary key, body text);
insert into docs values (1,'hello a'),(2,'other text'),(3,'hello hello b');
create fulltext index ft on docs(body);

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
-- @regex("Table Function on fulltext_index_scan", true)
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
select id from v where sc > 0.05 order by id;           -- only the higher scorer
select id from v where round(sc, 4) > 0.05 order by id;
select id from v where cast(sc as double) > 0.05 order by id;
select id from v where sc * 2 > 0.1 order by id;
select id from v where sc > 0.05 and id > 1 order by id;
select id from v where round(cast(sc as double) * 2, 3) > 0.1 order by id;

-- @separator:table
-- @regex("Table Function on fulltext_index_scan", true)
explain select id from v where round(sc, 4) > 0;

-- ---------------- DISCRIMINATING thresholds ------------------------------------
-- `sc > 0` is worthless as a test: every matching row satisfies it, so a predicate that is
-- silently never applied returns exactly the same rows as a correct one. That is how an
-- earlier version of this fix -- which attached the lifted predicate to a Node_JOIN
-- FilterList, a field nothing evaluates -- passed a whole suite of shapes while returning
-- rows the predicate excluded. These thresholds must CHANGE the row set.
select id from v where sc > 0.05 order by id;      -- only the higher-scoring row
select id from v where sc > 0.5 order by id;     -- none
select id from v where round(sc, 4) > 0.05 order by id;
select id from v where cast(sc as double) > 0.5 order by id;

-- the same shape with no view at all
select id from (
    select id, match(body) against('hello') as sc from docs where match(body) against('hello')
) x where sc > 0 order by id;

-- ---------------- WHICH match a wrapped score refers to -------------------------
-- A wrapped MATCH is rewritten to the score of the index scan built for THAT match, compared
-- on pattern/mode/index parts (the same test that builds eqmap). An earlier version rewrote
-- by function NAME under a "only one served match" count guard, which handed a wrapped
-- against('world') the relevance of the accompanying where ... against('hello') -- a wrong
-- number, silently, on every row.
create table two(id int primary key, body text);
insert into two values (1,'hello'),(2,'hello hello'),(3,'hello hello hello'),(4,'world'),(5,'hello hello world');
create fulltext index ft_two on two(body);

-- per-term baselines: 'hello' and 'world' score differently, which is the whole point
select id, match(body) against('hello') as h from two where match(body) against('hello') order by id;
select id, match(body) against('world') as w from two where match(body) against('world') order by id;

-- both matches served: each wrapped copy must report ITS OWN score, not the other's
select id, round(match(body) against('hello'),3) as h, round(match(body) against('world'),3) as w
from two where match(body) against('hello') and match(body) against('world') order by id;

-- a bare AND a wrapped copy of the SAME match in one projection. The old count guard saw
-- "2 served matches" here (the filter and the projection copy, already collapsed onto one
-- index scan by eqmap) and skipped the sweep, so the wrapped copy threw.
select id, match(body) against('hello') as sc, round(match(body) against('hello'),3) as r
from two where match(body) against('hello') order by id;

-- a wrapped MATCH on a DIFFERENT term is answered by a stream of its OWN, reporting its own
-- relevance. Note the row set: two streams are INNER JOINed, so only documents matching BOTH
-- survive -- the same restriction bare multi-stream MATCH already applies.
select id, round(match(body) against('world'),3) as r from two where match(body) against('hello') order by id;
select id from two where match(body) against('hello') and match(body) against('world') > 0.1 order by id;
select id from two where match(body) against('hello') and match(body) against('world') > 0.9 order by id;

-- the same with the second match ALSO named bare: the wrapped predicate must reuse that
-- stream rather than build a second one, and still filter by the world score.
select id, round(match(body) against('world'),3) as w from two
where match(body) against('hello') and match(body) against('world')
  and match(body) against('world') > 0.1 order by id;
select id from two
where match(body) against('hello') and match(body) against('world')
  and match(body) against('world') > 0.9 order by id;

-- ORDER BY a wrapped MATCH with no alias to hide behind
select id from two where match(body) against('hello') order by round(match(body) against('hello'),3) desc, id;

-- ---------------- a wrapped MATCH drives the rewrite on its own ------------------
-- Discovery recognises only a BARE fulltext_match at a top-level position, so a query whose
-- ONLY match is wrapped never entered the rewrite and raised 20105: a wrapped MATCH could
-- work solely as a passenger, alongside a bare MATCH with identical arguments to drive it.
-- getWrappedFullTextMatches registers it as its own stream instead.
select id from two where match(body) against('hello') > 0.015 order by id;   -- drops the lowest scorer
select id from two where match(body) against('hello') > 0.9 order by id;     -- none
select id, round(match(body) against('hello'),3) as r from two order by id;  -- projection, no WHERE MATCH
select count(*) as n from two where match(body) against('hello') > 0.015;    -- and through an aggregate
select count(*) as n from two where match(body) against('hello') > 0.9;

-- with no ORDER BY the rows must come back by RELEVANCE, exactly as a bare MATCH does. The
-- stream built for a wrapped-only match belongs to neither the filter nor the projection list,
-- so the SORT above the join was built with NO keys: it sorted by nothing and handed back join
-- order. Both selects below must return the same sequence.
select id from two where match(body) against('hello') > 0.005;
select id from two where match(body) against('hello');

-- @separator:table
-- @regex("Table Function on fulltext_index_scan", true)
explain select id from two where match(body) against('hello') > 0.015;

-- a MATCH no INDEX can serve is still left alone and still raises 20105
create table nodx(id int primary key, txt text);
insert into nodx values (1,'hello');
select id from nodx where match(txt) against('hello') > 0.1;

-- ---------------- only a MEMBERSHIP-IMPLYING predicate may drive ------------------
-- The stream a driver builds is INNER JOINed to the base scan, which ANDs "this document is in
-- the index result" onto the query. That is sound for `score > c`, which a non-matching
-- document (relevance 0) cannot satisfy anyway -- and WRONG anywhere else.
--
-- Harvesting a MATCH out of an OR or a NOT turned these into the EMPTY SET, dropping id 4
-- ('world', which contains no 'hello' and so is not in the stream at all) even though both
-- queries plainly select it. An error is the honest answer until the stream can be LEFT joined
-- with a relevance of 0 for the missing rows, which is also what MySQL reports.
select id from two where match(body) against('hello') > 0.5 or id = 4;
select id from two where not match(body) against('hello');
select id from two where id = 4 or match(body) against('hello') > 0.5;

-- `score < c` is true for a non-matching document too, so it cannot drive either
select id from two where match(body) against('hello') < 0.015 order by id;

-- ...but with a bare MATCH already restricting the rows, membership is implied and every
-- comparison is free to filter among the matching documents
select id from two where match(body) against('hello') and match(body) against('hello') < 0.015 order by id;
select id from two where match(body) against('hello') and (match(body) against('hello') < 0.015 or id = 3) order by id;
select id from two where match(body) against('hello') and not (match(body) against('hello') > 0.015) order by id;

-- a negative or zero threshold is not membership-implying: relevance 0 satisfies both
select id from two where match(body) against('hello') >= 0 order by id;

-- ---------------- wrapped MATCH on a join child ----------------------------------
-- Filters on either input of an INNER/SEMI join are rewritten per scan by
-- applyFullTextFiltersForScanInJoin, which also looked for bare matches only -- so this shape
-- threw even though the same predicate works without the join. There is no project node on
-- that path, so the predicate itself is what has to be discovered.
create table jb(id int primary key, note varchar(20));
insert into jb values (1,'x'),(2,'y'),(3,'z');
select id, match(body) against('hello') as sc from two where match(body) against('hello') order by id;

select t.id, jb.note from two t join jb on t.id=jb.id
 where match(t.body) against('hello') > 0.015 order by t.id;
select t.id from two t join jb on t.id=jb.id
 where match(t.body) against('hello') > 0.9 order by t.id;
-- IN subqueries are flattened to SEMI joins and take the same path
select id from two where match(body) against('hello') > 0.015 and id in (select id from jb) order by id;

-- and the membership rule holds here too: `< c` cannot drive a join child either
select t.id from two t join jb on t.id=jb.id
 where match(t.body) against('hello') < 0.015 order by t.id;

-- ---------------- lifted predicate vs the candidate LIMIT ------------------------
-- Lifting a wrapped predicate off the scan empties FilterList, which used to re-enable the
-- LIMIT pushdown into the fulltext TVF. The predicate runs ABOVE the join, so a capped stream
-- hands it only the top-relevance rows: `< 0.015 limit 1` capped the stream to the single
-- highest-scoring document and then rejected it, returning nothing while id 1 qualified.
-- id 1 is the UNIQUE LOWEST scorer on purpose -- with a surviving cap these return 0 rows.
select id from two where match(body) against('hello') and match(body) against('hello') < 0.015 limit 1;
select id from two where match(body) against('hello') and match(body) against('hello') < 0.015;
select count(*) as n from (
    select id from two where match(body) against('hello') and match(body) against('hello') < 0.02 limit 1
) q;
-- positive control: LIMIT still works at the top of the range, where the cap agreed anyway
select id from two where match(body) against('hello') and match(body) against('hello') > 0.02 limit 1;

drop database ft_wrapped;

-- Restore the default so this case does not leak its setting to whatever runs next --
-- the failure mode that made an earlier version of this file order-dependent.
set ft_relevancy_algorithm="TF-IDF";
