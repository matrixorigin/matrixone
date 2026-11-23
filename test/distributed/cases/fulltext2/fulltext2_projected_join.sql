-- #31689: a select-list MATCH over an IN-subquery, EXISTS, NOT EXISTS, or JOIN drives its own
-- fulltext2 search stream on the scan it references, as it does over a single table.
drop database if exists ft2_proj_join;
create database ft2_proj_join;
use ft2_proj_join;
set experimental_fulltext2_index = 1;
create table docs(id bigint primary key, body text);
insert into docs values (1,'needle anchor'),(2,'needle other'),(3,'anchor other'),(4,'needle anchor'),(5,'filler text');
create table members(k bigint);
insert into members values (1),(1),(2),(4),(null);
create fulltext2 index ft on docs(body);
alter table docs alter reindex ft fulltext2 force_sync;

-- Single-table controls.
select d.id, match(d.body) against('anchor') as score from docs d where match(d.body) against('needle') order by d.id;
select d.id, match(d.body) against('anchor') as score from docs d where match(d.body) against('needle') and d.id in (1,2,4) order by d.id;

-- IN-subquery, different filter term.
-- @separator:table
-- @regex("Fulltext Index Scan on",true)
explain select d.id, match(d.body) against('anchor') as score from docs d where match(d.body) against('needle') and d.id in (select k from members) order by d.id;
select d.id, match(d.body) against('anchor') as score from docs d where match(d.body) against('needle') and d.id in (select k from members) order by d.id;

-- IN-subquery, same term; and both terms in WHERE.
select d.id, match(d.body) against('needle') as score from docs d where match(d.body) against('needle') and d.id in (select k from members) order by d.id;
select d.id, match(d.body) against('anchor') as score from docs d where match(d.body) against('needle') and match(d.body) against('anchor') and d.id in (select k from members) order by d.id;

-- IN-subquery, no WHERE MATCH.
select d.id, match(d.body) against('anchor') as score from docs d where d.id in (select k from members) order by d.id;

-- Correlated EXISTS and NOT EXISTS.
select d.id, match(d.body) against('anchor') as score from docs d where match(d.body) against('needle') and exists (select 1 from members m where m.k = d.id) order by d.id;
select d.id, match(d.body) against('anchor') as score from docs d where not exists (select 1 from members m where m.k = d.id) order by d.id;

-- INNER JOIN: distinct derived table, duplicate members, both terms in WHERE.
select d.id, match(d.body) against('anchor') as score from docs d join (select distinct k from members) m on m.k = d.id where match(d.body) against('needle') order by d.id;
select d.id, match(d.body) against('anchor') as score from docs d join members m on m.k = d.id where match(d.body) against('needle') order by d.id;
select d.id, match(d.body) against('anchor') as score from docs d join (select distinct k from members) m on m.k = d.id where match(d.body) against('needle') and match(d.body) against('anchor') order by d.id;

-- LEFT JOIN with docs on the preserved side.
select d.id, m.k, match(d.body) against('anchor') as score from docs d left join members m on m.k = d.id order by d.id, m.k;

-- Wrapped and multiple projected MATCHes; ORDER BY score with LIMIT/OFFSET.
select d.id, round(match(d.body) against('anchor'), 3) as a, match(d.body) against('needle') as n from docs d where d.id in (select k from members) order by d.id;
select d.id, match(d.body) against('anchor') as score from docs d where d.id in (select k from members) order by score desc, d.id limit 1 offset 1;

-- OR, NOT and `= 0` WHERE predicates keep their rows, scored 0 when the term does not match.
select d.id, match(d.body) against('needle') as score from docs d where (match(d.body) against('needle') or match(d.body) against('filler')) and d.id in (select id from docs) order by d.id;
select d.id, match(d.body) against('needle') as score from docs d where not match(d.body) against('needle') and d.id in (select id from docs) order by d.id;
select d.id, match(d.body) against('needle') as score from docs d where match(d.body) against('needle') = 0 and d.id in (select id from docs) order by d.id;
select d.id, match(d.body) against('needle') as score from docs d join (select id as k from docs) m on m.k = d.id where match(d.body) against('needle') = 0 order by d.id;

-- LEFT JOIN with docs on the null-extended side is not driven.
select m.k, match(d.body) against('anchor') as score from members m left join docs d on m.k = d.id order by m.k;

-- Vector ORDER BY ... LIMIT: the projected MATCH is part of the vector pre-filter, so LIMIT is
-- filled from matching rows.
set experimental_ivf_index = 1;
create table vdocs(id bigint primary key, body text, v vecf32(3));
insert into vdocs values (1,'needle anchor','[1,0,0]'),(2,'needle other','[2,0,0]'),(3,'anchor other','[3,0,0]'),(4,'needle anchor','[4,0,0]'),(5,'filler text','[5,0,0]');
create fulltext2 index ftv on vdocs(body);
alter table vdocs alter reindex ftv fulltext2 force_sync;
create index vi using ivfflat on vdocs(v) lists=1 op_type 'vector_l2_ops';
create table vmembers(k bigint);
insert into vmembers values (1),(1),(2),(3),(4),(5),(null);
select d.id, match(d.body) against('anchor') as score from vdocs d where d.id in (select k from vmembers) order by l2_distance(d.v, '[0,0,0]') limit 2;
select d.id, match(d.body) against('anchor') as score from vdocs d where exists (select 1 from vmembers m where m.k = d.id) order by l2_distance(d.v, '[0,0,0]') limit 2;
select d.id, match(d.body) against('anchor') as score from vdocs d join vmembers m on m.k = d.id order by l2_distance(d.v, '[0,0,0]'), d.id limit 3;
select d.id, match(d.body) against('anchor') as score from vdocs d where match(d.body) against('needle') and d.id in (select k from vmembers) order by l2_distance(d.v, '[0,0,0]') limit 2;

-- A derived table's LIMIT/OFFSET applies before a MATCH above it: not served, 20105.
select q.id, match(q.body) against('other') as score from (select d.id, d.body from docs d join members m on m.k = d.id order by d.id limit 1) q;
select q.id, match(q.body) against('other') as score from (select d.id, d.body from docs d join members m on m.k = d.id order by d.id limit 1 offset 1) q;
select d.id, match(d.body) against('other') as score from (select * from docs order by id limit 1) d join members m on m.k = d.id;
-- An unpaginated derived table is served, paginated after the MATCH.
select q.id, match(q.body) against('other') as score from (select d.id, d.body from docs d join members m on m.k = d.id) q order by q.id limit 1;

drop database ft2_proj_join;
