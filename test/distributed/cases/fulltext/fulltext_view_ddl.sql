-- View DDL must not persist a MATCH() AGAINST() that no FULLTEXT index can serve (#27027).
-- Unlike a vector index, fulltext has no brute-force fallback: fulltext_match is a
-- placeholder with no implementation, so such a view is not slow, it is unrunnable --
-- every query against it fails with "MATCH() AGAINST() function cannot be replaced by
-- FULLTEXT INDEX". Before the fix all three DDL forms reported success, and ALTER /
-- CREATE OR REPLACE additionally destroyed a working definition on the way.
--
-- The rejection is raised before any DDL plan is emitted, which is what makes ALTER and
-- REPLACE atomic: the old view must survive a refused statement, so each case below checks
-- the previous definition still returns its rows.
set experimental_fulltext_index = 1;

drop database if exists ft_view_ddl;
create database ft_view_ddl;
use ft_view_ddl;

create table docs (id int primary key, body text, title text);
insert into docs values
    (1, 'hello matrixone', 'alpha'),
    (2, 'other text', 'hello title'),
    (3, 'hello hello database', 'gamma');

-- ---------------- CREATE VIEW leaves no object behind --------------------------
create view v_create_bad as
select id, match(body) against('hello') as score from docs where match(body) against('hello');
select count(*) as leftover from information_schema.views
where table_schema = 'ft_view_ddl' and table_name = 'v_create_bad';

-- ---------------- ALTER VIEW preserves the old definition ----------------------
create view v_alter as select id, body from docs;
select count(*) as before_alter from v_alter;
alter view v_alter as
select id, match(body) against('hello') as score from docs where match(body) against('hello');
select count(*) as after_alter from v_alter;

-- ---------------- CREATE OR REPLACE VIEW preserves it too ----------------------
create view v_replace as select id, body from docs;
select count(*) as before_replace from v_replace;
create or replace view v_replace as
select id, match(body) against('hello') as score from docs where match(body) against('hello');
select count(*) as after_replace from v_replace;

-- ---------------- a MATCH only in the WHERE, and only in the SELECT ------------
-- Both forms are unrunnable without an index: the filter keeps fulltext_match, while an
-- unmatched projection is converted to the equally unevaluable fulltext_match_score.
create view v_filter_only as select id from docs where match(body) against('hello');
create view v_project_only as select id, match(body) against('hello') as score from docs;

-- ---------------- a MATCH hidden in a window spec ------------------------------
-- OVER (ORDER BY MATCH(...)) does not land in the node's OrderBy: WinSpecList holds an
-- Expr_W and the MATCH sits inside ITS OrderBy. The first version of the guard walked
-- neither, so this shape sailed through and persisted an unusable view exactly as #27027
-- describes. It needs both the field and the descent into the spec.
create view v_window as
select id, row_number() over (order by match(body) against('hello')) as rn from docs;

-- an ordinary window with no MATCH is untouched
create view v_window_ok as select id, row_number() over (order by id) as rn from docs;
select id, rn from v_window_ok order by id;

-- ---------------- an index on the WRONG column does not satisfy it -------------
create fulltext index ft_title on docs(title);
create view v_wrong_col as select id from docs where match(body) against('hello');

-- ---------------- with a matching index every form must WORK -------------------
-- The guard reuses the planner's own matching decision, so it must not reject a view the
-- index can serve. A successful rewrite abandons its pre-rewrite FILTER node in the plan
-- arena, still holding the original fulltext_match; reading that corpse rejected every one
-- of these, so these rows are the regression test for that.
create fulltext index ft_body on docs(body);

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

-- A MATCH combined with a window function stays refused even WITH the matching index. The
-- fulltext rewrite fires for project -> scan, sort -> scan and agg -> scan; a Window node
-- between the project and the scan defeats resolveFullTextIndexPath, so fulltext_match
-- survives as a raw filter and the query cannot run. The DIRECT query fails identically
-- (ERROR 20105) with no view involved, so nothing is lost by refusing here -- these views
-- never worked. Both spellings are pinned: MATCH inside the window spec, and MATCH in the
-- WHERE alongside an unrelated window function.
--
-- If the rewrite is ever taught to see through a Window node, these two become servable
-- and will simply start being accepted; this guard needs no change for that.
create view v_window_idx as
select id, row_number() over (order by match(body) against('hello')) as rn from docs;

create view v_window_where as
select id, row_number() over (order by id) as rn from docs where match(body) against('hello');

-- a window function with no MATCH is unaffected and must still work
create view v_window_plain as select id, row_number() over (order by id) as rn from docs;
select id, rn from v_window_plain order by id;

-- the index is genuinely used, not a full scan
-- @separator:table
-- @regex("Table Function on fulltext_index_scan", true)
explain select id from v_good_filter;

-- ---------------- views with no MATCH are untouched ----------------------------
create view v_plain as select id, body from docs where id > 1;
select count(*) as plain_rows from v_plain;

drop database ft_view_ddl;
