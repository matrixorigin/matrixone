-- Relevance algorithm pinned FIRST: it is a session variable other cases in this suite change
-- (fulltext_bm25 sets BM25), and every score value below depends on it.
set ft_relevancy_algorithm="TF-IDF";
-- Selecting the relevance when the query also has a JOIN.
--
-- A join takes a different route than a single scan: applyFullTextFiltersForJoinChildren
-- rewrites each TABLE_SCAN child on its own, with no project node in hand, so nothing replaced
-- a MATCH sitting in the SELECT list above it. Filtering by relevance across a join worked
-- while selecting it raised "MATCH() AGAINST() function cannot be replaced by FULLTEXT INDEX".
--
-- applyIndices recurses children before parents, so the fulltext scans already exist when the
-- PROJECT is visited; resolveProjectMatchesOverJoin resolves each MATCH against them.
--
-- The comparison is binding-tag-aware. Index parts are otherwise compared by column NAME, and
-- with two tables in scope match(a.body) and match(b.body) look identical by name -- one
-- table's relevance would be reported for the other's. The cross-table case below is what
-- catches that: the two columns must hold DIFFERENT numbers on the same row.
set experimental_fulltext_index = 1;

drop database if exists ft_join;
create database ft_join;
use ft_join;

create table a(id int primary key, body text);
create table b(id int primary key, note varchar(20), body text);
insert into a values (1,'hello'),(2,'hello hello'),(3,'world');
insert into b values (1,'x','hello hello hello'),(2,'y','zzz'),(3,'z','hello');
create fulltext index fa on a(body);
create fulltext index fb on b(body);

-- ---------------- per-table truth ------------------------------------------------
select id, match(body) against('hello') as sc from a where match(body) against('hello') order by id;
select id, match(body) against('hello') as sc from b where match(body) against('hello') order by id;

-- ---------------- the score in the SELECT list, over a join ----------------------
select a.id, match(a.body) against('hello') as sc
from a join b on a.id=b.id where match(a.body) against('hello') order by a.id;

select a.id, round(match(a.body) against('hello'),3) as r
from a join b on a.id=b.id where match(a.body) against('hello') order by a.id;

-- ---------------- CROSS-TABLE: each side reports ITS OWN relevance ---------------
-- sa and sb must DIFFER on row 1 (a: 'hello' -> 0.031, b: 'hello hello hello' -> 0.093).
-- Matching MATCHes by column name alone would put the same number in both columns.
select a.id, match(a.body) against('hello') as sa, match(b.body) against('hello') as sb
from a join b on a.id=b.id
where match(a.body) against('hello') and match(b.body) against('hello') order by a.id;

-- @separator:table
-- @regex("Table Function on fulltext_index_scan", true)
explain select a.id, match(a.body) against('hello') as sa, match(b.body) against('hello') as sb
from a join b on a.id=b.id
where match(a.body) against('hello') and match(b.body) against('hello');

-- ---------------- ORDER BY the relevance, over a join ----------------------------
-- ORDER BY match(...) is bound as its own expression rather than a reference to the
-- projection, so it needs resolving too. id 2 scores higher than id 1.
select a.id from a join b on a.id=b.id
where match(a.body) against('hello') order by match(a.body) against('hello') desc;

-- ---------------- a self join: the same table twice ------------------------------
-- Two bindings of one table. Equal scores here are CORRECT (same rows, same term); the point
-- is that two distinct bindings resolve independently instead of colliding.
select x.id, match(x.body) against('hello') as sx, match(y.body) against('hello') as sy
from a x join a y on x.id=y.id
where match(x.body) against('hello') and match(y.body) against('hello') order by x.id;

-- ---------------- an outer join still returns the right scores -------------------
select a.id, match(a.body) against('hello') as sc
from a left join b on a.id=b.id where match(a.body) against('hello') order by a.id;

-- ---------------- a MATCH no index can serve still raises 20105 ------------------
create table nox(id int primary key, txt text);
insert into nox values (1,'hello');
select a.id, match(nox.txt) against('hello') as s from a join nox on a.id=nox.id where match(a.body) against('hello');

drop database ft_join;

-- Restore the default so this case does not leak its setting to whatever runs next.
set ft_relevancy_algorithm="TF-IDF";
