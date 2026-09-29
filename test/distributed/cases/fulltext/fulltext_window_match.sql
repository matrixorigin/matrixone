-- Regression for #28974 (classic FULLTEXT): a window function (ROW_NUMBER() OVER (...)) in the same
-- SELECT as a top-level WHERE MATCH() AGAINST() predicate must not block the fulltext-index rewrite.
-- Before the fix the planner left fulltext_match on the base-table scan beneath the WINDOW operator
-- and execution failed with ERROR 20105; the MATCH must be rewritten to the fulltext index scan
-- under the window so the query returns the ranked rows. (FULLTEXT2 sibling: fulltext2_window_match.)
set experimental_fulltext_index = 1;
drop database if exists ft_window;
create database ft_window;
use ft_window;

create table docs (id bigint primary key, body text, v vecf32(3));
insert into docs values
  (1,'alpha','[0,0,0]'),(2,'beta','[0.1,0,0]'),(3,'alpha','[0.2,0,0]'),(4,'alpha','[0.3,0,0]');
create fulltext index ft on docs(body) with parser ngram;

-- Control: a plain top-level MATCH is served by the fulltext index and returns 1,3,4.
select id from docs where match(body) against('alpha' in boolean mode) order by id;

-- The bug: MATCH + a window function. Must return (1,1),(3,2),(4,3), not ERROR 20105.
select id, row_number() over (order by l2_distance(v,'[0,0,0]'), id) as rn
from docs where match(body) against('alpha' in boolean mode) order by id;

-- The MATCH beneath the WINDOW is served by the fulltext index scan (the rewrite fired); no bare
-- fulltext_match survives on the base-table scan.
-- @separator:table
-- @regex("fulltext_index_scan",true)
explain select id, row_number() over (order by l2_distance(v,'[0,0,0]'), id) as rn
from docs where match(body) against('alpha' in boolean mode) order by id;

-- A MATCH that ALSO appears inside the window's OVER order-by is served by the same index scan and
-- rewritten to the score column, so it too returns rows instead of 20105.
select id, row_number() over (order by match(body) against('alpha' in boolean mode) desc, id) as rn
from docs where match(body) against('alpha' in boolean mode) order by id;

-- #28974 P2: OVER (PARTITION BY ...) makes the binder insert a PARTITION node between the WINDOW and
-- the scan. The rewrite must descend that PARTITION to serve the WHERE MATCH and reparent below it;
-- the partitioned form returned ERROR 20105 before the fix.
select id, row_number() over (partition by body order by id) as rn
from docs where match(body) against('alpha' in boolean mode) order by id;

-- @separator:table
-- @regex("fulltext_index_scan",true)
explain select id, row_number() over (partition by body order by id) as rn
from docs where match(body) against('alpha' in boolean mode) order by id;

-- #28974 P2: a MATCH projected in the SELECT list above the WINDOW is resolved to the served score
-- (published into ftJoinServed), not left as a bare fulltext_match that throws 20105.
select id, (match(body) against('alpha' in boolean mode) > 0) as hit,
       row_number() over (order by id) as rn
from docs where match(body) against('alpha' in boolean mode) order by id;

-- #28974 P2: a MATCH in an ancestor ORDER BY above the WINDOW is resolved the same way.
select id, row_number() over (order by id) as rn
from docs where match(body) against('alpha' in boolean mode)
order by match(body) against('alpha' in boolean mode) desc, id;

-- #28974 P2: stacked window functions each referencing the served MATCH in their own OVER clause;
-- the inner window serves the scan, the outer window resolves against the published scores.
select id,
       row_number() over (order by match(body) against('alpha' in boolean mode) desc, id) as r1,
       rank() over (order by match(body) against('alpha' in boolean mode) desc, id) as r2
from docs where match(body) against('alpha' in boolean mode) order by id;

-- #28974 P2: a post-window predicate that ALSO references a window column (rn) cannot be pushed
-- below the WINDOW, so it stays on WINDOW.FilterList and is evaluated AFTER the window. Predicate
-- pushdown expands the projected `score` alias back to the raw MATCH there; that post-window copy
-- must be rewritten to the served index score too, or it reaches execution as ERROR 20105. Returns
-- (1,1),(3,2),(4,3).
select id, rn from (
  select id, match(body) against('alpha' in boolean mode) as score,
         row_number() over (order by id) as rn
  from docs where match(body) against('alpha' in boolean mode)
) q where rn = 1 or score > 0 order by id;

-- @separator:table
-- @regex("fulltext_index_scan",true)
explain select id, rn from (
  select id, match(body) against('alpha' in boolean mode) as score,
         row_number() over (order by id) as rn
  from docs where match(body) against('alpha' in boolean mode)
) q where rn = 1 or score > 0 order by id;

-- #28974 P2: an outer predicate that references NEITHER a window column NOR a partition key cannot be
-- pushed below the WINDOW and stays in an INDEPENDENT FILTER above it (unlike `rn = 1 or score > 0`,
-- which stays on WINDOW.FilterList). Predicate pushdown inlines the projected `score` back to the raw
-- MATCH there; that copy must be rewritten to the served index score too, or it reaches execution as
-- ERROR 20105. Returns (1,1),(3,2),(4,3).
select id, rn from (
  select id, match(body) against('alpha' in boolean mode) as score,
         row_number() over (order by id) as rn
  from docs where match(body) against('alpha' in boolean mode)
) q where score > 0 order by id;

-- @separator:table
-- @regex("fulltext_index_scan",true)
-- @regex("fulltext_match",false)
explain select id, rn from (
  select id, match(body) against('alpha' in boolean mode) as score,
         row_number() over (order by id) as rn
  from docs where match(body) against('alpha' in boolean mode)
) q where score > 0 order by id;

drop database ft_window;
