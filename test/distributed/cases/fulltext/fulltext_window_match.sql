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

drop database ft_window;
