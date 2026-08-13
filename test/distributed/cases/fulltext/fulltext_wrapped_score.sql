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
-- Pin the relevance algorithm: it is a session variable other cases in this suite change
-- (fulltext_bm25 sets BM25), so without this the scores printed below depend on run order.
set ft_relevancy_algorithm="TF-IDF";

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
-- Inlining substitutes the score alias with its definition and pushes it onto the base scan,
-- where the bare-match test cannot see it. It is lifted onto the join instead.
create view v as select id, match(body) against('hello') as sc from docs where match(body) against('hello');

select id from v where sc > 0 order by id;
select id from v where round(sc, 4) > 0 order by id;
select id from v where cast(sc as double) > 0 order by id;
select id from v where sc * 2 > 0 order by id;
select id from v where sc > 0 and id > 1 order by id;
select id from v where round(cast(sc as double) * 2, 3) > 0 order by id;

-- @separator:table
-- @regex("Table Function on fulltext_index_scan", true)
explain select id from v where round(sc, 4) > 0;

-- the same shape with no view at all
select id from (
    select id, match(body) against('hello') as sc from docs where match(body) against('hello')
) x where sc > 0 order by id;

drop database ft_wrapped;
