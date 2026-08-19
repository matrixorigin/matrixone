-- Relevance algorithm pinned FIRST: every score value and threshold below depends on it.
set ft2_relevancy_algorithm="BM25";
-- fulltext2 half of #27027. MATCH() AGAINST() binds to the same unevaluable placeholders as
-- classic fulltext and is resolved by the same matcher, so a view definition no fulltext2
-- index can serve is equally unrunnable and must be refused rather than persisted.
--
-- The refusal itself comes from a body both fulltext plugins share, so this file is NOT
-- trying to prove the refusal twice. What it covers that the classic case cannot is the
-- fulltext2 ENGINE end to end: that a fulltext2 index really does satisfy MATCH, that the
-- rewrite fires and the view returns rows, and that fulltext2's own DDL (CREATE FULLTEXT2
-- INDEX) participates correctly. A regression in fulltext2's matcher or index metadata
-- would show up here and nowhere else.
--
-- MySQL rejects the same no-index CREATE / ALTER / CREATE OR REPLACE VIEW with
-- ER_FT_MATCHING_KEY_NOT_FOUND (1191); MatrixOne now returns that same code and text.
set experimental_fulltext2_index = 1;

drop database if exists ft2_view_ddl;
create database ft2_view_ddl;
use ft2_view_ddl;

create table docs (id bigint primary key, body text, title text);
insert into docs values
    (1, 'hello matrixone', 'alpha'),
    (2, 'other text', 'hello title'),
    (3, 'hello hello database', 'gamma');

-- ---------------- CREATE VIEW leaves no object behind --------------------------
create view v_create_bad as
select id, match(body) against('hello') as score from docs where match(body) against('hello');
select count(*) as leftover from information_schema.views
where table_schema = 'ft2_view_ddl' and table_name = 'v_create_bad';

-- ---------------- ALTER VIEW preserves the old definition ----------------------
create view v_alter as select id, body from docs;
select count(*) as before_alter from v_alter;
alter view v_alter as select id from docs where match(body) against('hello');
select count(*) as after_alter from v_alter;

-- ---------------- CREATE OR REPLACE VIEW preserves it too ----------------------
create view v_replace as select id, body from docs;
select count(*) as before_replace from v_replace;
create or replace view v_replace as select id from docs where match(body) against('hello');
select count(*) as after_replace from v_replace;

-- ---------------- filter-only and projection-only ------------------------------
create view v_filter_only as select id from docs where match(body) against('hello');
create view v_project_only as select id, match(body) against('hello') as score from docs;

-- ---------------- a MATCH hidden in a window spec ------------------------------
-- OVER (ORDER BY MATCH(...)) does not land in the node's OrderBy: it sits inside the
-- window spec expression held in WinSpecList, which a naive walk misses entirely.
create view v_window as
select id, row_number() over (order by match(body) against('hello')) as rn from docs;

-- an ordinary window with no MATCH is untouched
create view v_window_ok as select id, row_number() over (order by id) as rn from docs;
select id, rn from v_window_ok order by id;

-- ---------------- an index on the WRONG column does not satisfy it -------------
create fulltext2 index ft2_title on docs(title);
create view v_wrong_col as select id from docs where match(body) against('hello');

-- ---------------- with a matching fulltext2 index every form must WORK ---------
-- This is the half unique to fulltext2: its own index type must satisfy MATCH, the rewrite
-- must fire, and the views must return rows.
create fulltext2 index ft2_body on docs(body);

create view v_good_filter as select id from docs where match(body) against('hello');
select id from v_good_filter order by id;

create view v_good_score as
select id, match(body) against('hello') as score from docs where match(body) against('hello');
select id from v_good_score order by id;

alter view v_good_score as
select id, match(body) against('hello') as score from docs where match(body) against('hello');
select id from v_good_score order by id;

create or replace view v_good_score as
select id, match(body) against('hello') as score from docs where match(body) against('hello');
select id from v_good_score order by id;

-- the fulltext2 index is genuinely used, not a full scan. Note the TVF name: fulltext2 is
-- served by fulltext2_search, NOT the classic fulltext_index_scan -- asserting the wrong one
-- here is what first proved this file covers ground the classic case cannot.
-- @separator:table
-- @regex("Table Function on fulltext2_search", true)
explain select id from v_good_filter;

-- a MATCH combined with a window function stays refused even WITH the index: the rewrite
-- does not reach through a Window node, so the direct query fails the same way and the view
-- could never run.
create view v_window_idx as
select id, row_number() over (order by match(body) against('hello')) as rn from docs;

create view v_window_where as
select id, row_number() over (order by id) as rn from docs where match(body) against('hello');

-- ---------------- a filter on the projected score, above the view ---------------
-- `WHERE sc > 0` on a view's score column is a predicate that WRAPS the MATCH rather than
-- being one. Inlining pushes it onto the base scan, where getFullTextMatchFiltersFromScanNode
-- -- which only recognises a bare fulltext_match -- cannot see it, so it survived the rewrite
-- and threw at execution even though the index scan was built right beside it. It is now
-- lifted onto the join and rewritten to reference the score column.
--
-- MATCH returns a FLOAT relevance score in MatrixOne (DESC on such a view reports FLOAT),
-- as in MySQL, so comparing it is meaningful rather than a bool coercion.
-- @separator:table
-- @regex("Table Function on fulltext2_search", true)
explain select id from v_good_score where score > 0;

select id from v_good_score where score > 0 order by id;
-- and a threshold that must EXCLUDE a row, so a never-applied predicate cannot pass:
select id from v_good_score where score > 0.037 order by id;

-- the same shape without a view at all, which is where it also failed
select id from (
    select id, match(body) against('hello') as sc from docs where match(body) against('hello')
) x where sc > 0 order by id;

-- ---------------- views with no MATCH are untouched ----------------------------
create view v_plain as select id, body from docs where id > 1;
select count(*) as plain_rows from v_plain;

drop database ft2_view_ddl;

-- Restore the default so this case does not leak its setting to whatever runs next --
-- the failure mode that made an earlier version of this file order-dependent.
set ft2_relevancy_algorithm="BM25";
