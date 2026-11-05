-- #31715: a MATCH selected and filtered through a pass-through view, nested view, derived table
-- or CTE is served by the fulltext index, as on the base table.
drop database if exists ft_view_score;
create database ft_view_score;
use ft_view_score;

create table docs(id bigint primary key, body text);
insert into docs values (1,'needle anchor'),(2,'needle other'),(3,'anchor other'),(4,'needle anchor'),(5,'filler text');
create fulltext index ft on docs(body);
create view v as select * from docs;
create view v2 as select * from v;
create view inside_v as select id, match(body) against('needle') as score from docs where match(body) against('needle');

-- Base table control.
select id, match(body) against('needle') as score from docs where match(body) against('needle') order by id;

-- View, nested view, derived table, CTE.
-- @separator:table
-- @regex("Fulltext Index Scan on",true)
explain select id, match(body) against('needle') as score from v where match(body) against('needle') order by id;
select id, match(body) against('needle') as score from v where match(body) against('needle') order by id;
select id, match(body) against('needle') as score from v2 where match(body) against('needle') order by id;
select id, match(body) against('needle') as score from (select * from docs) d where match(body) against('needle') order by id;
with d as (select * from docs) select id, match(body) against('needle') as score from d where match(body) against('needle') order by id;

-- Renamed derived columns, different projected term, wrapped score, ORDER BY score, LIMIT.
select x, match(b) against('needle') as score from (select id as x, body as b from docs) d where match(b) against('needle') order by x;
select id, match(body) against('anchor') as score from v where match(body) against('needle') order by id;
select id, round(match(body) against('needle'), 3) as score from v where match(body) against('needle') order by id;
select id, match(body) against('needle') as score from v where match(body) against('needle') order by score desc, id;
select id, match(body) against('needle') as score from v where match(body) against('needle') order by id limit 2;

-- Filter-only, score-only, and score defined inside the view.
select id from v where match(body) against('needle') order by id;
select id, match(body) against('needle') as score from v order by id;
select * from inside_v order by id;

-- WINDOW over a filtered view.
select id, row_number() over (order by id) as rn from v where match(body) against('needle') order by id;

-- Derived table with LIMIT/OFFSET pages relevance order, as on the base table; distinct scores.
create table pdocs(id bigint primary key, body text);
insert into pdocs values (1,'needle filler filler filler'),(2,'needle needle needle'),(3,'needle needle filler'),(4,'other text');
create fulltext index pft on pdocs(body);
select id, match(body) against('needle') as score from pdocs where match(body) against('needle') limit 2;
select id, match(body) against('needle') as score from (select * from pdocs where match(body) against('needle') limit 2) d;
select id, match(body) against('needle') as score from (select * from pdocs where match(body) against('needle') limit 2) d order by id;
select id, match(body) against('needle') as score from (select * from pdocs where match(body) against('needle') limit 1 offset 1) d;
select id from (select * from pdocs where match(body) against('needle') limit 2) d order by id;

drop database ft_view_score;
