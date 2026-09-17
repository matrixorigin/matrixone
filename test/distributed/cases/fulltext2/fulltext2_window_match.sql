-- Regression for #28974: a window function (ROW_NUMBER() OVER (...)) in the same SELECT as a
-- top-level WHERE MATCH() AGAINST() predicate must not block the fulltext-index rewrite. Before the
-- fix the planner left fulltext_match on the base-table scan beneath the WINDOW operator and
-- execution failed with ERROR 20105; the MATCH must be rewritten to the fulltext2 index scan under
-- the window so the query returns the ranked rows.
set experimental_fulltext2_index = 1;
drop database if exists ft2_window;
create database ft2_window;
use ft2_window;

create table docs (id bigint primary key, body text, v vecf32(3), fulltext2 ft(body) with parser ngram);
insert into docs values
  (1,'alpha','[0,0,0]'),(2,'beta','[0.1,0,0]'),(3,'alpha','[0.2,0,0]'),(4,'alpha','[0.3,0,0]');
alter table docs alter reindex ft fulltext2 force_sync;

-- Control: a plain top-level MATCH is served by the fulltext2 index and returns 1,3,4.
select id from docs where match(body) against('alpha' in boolean mode) order by id;

-- The bug: MATCH + a window function. Must return (1,1),(3,2),(4,3), not ERROR 20105.
select id, row_number() over (order by l2_distance(v,'[0,0,0]'), id) as rn
from docs where match(body) against('alpha' in boolean mode) order by id;

-- The MATCH beneath the WINDOW is served by the fulltext2 index scan (the rewrite fired); no bare
-- fulltext_match survives on the base-table scan.
-- @separator:table
-- @regex("fulltext2_search",true)
explain select id, row_number() over (order by l2_distance(v,'[0,0,0]'), id) as rn
from docs where match(body) against('alpha' in boolean mode) order by id;

-- A MATCH that ALSO appears inside the window's OVER order-by (not just the WHERE clause) is served
-- by the same index scan and rewritten to the score column, so it too returns rows instead of 20105.
select id, row_number() over (order by match(body) against('alpha' in boolean mode) desc, id) as rn
from docs where match(body) against('alpha' in boolean mode) order by id;

drop database ft2_window;
