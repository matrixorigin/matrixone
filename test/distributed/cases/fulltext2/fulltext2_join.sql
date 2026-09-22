-- fulltext2 JOIN rewrite, incl. #20687: a WHERE MATCH on the ROW-PRESERVED side of an outer join is
-- served by the fulltext2 index (fulltext2_search), matchers of the preserved input with the other
-- side null-extended. Sibling of the classic fulltext_join case.
set experimental_fulltext2_index = 1;
drop database if exists ft2_join;
create database ft2_join;
use ft2_join;

create table docs (id int primary key, base_id int, title varchar(200), fulltext2 index ft2_idx(title));
insert into docs values
    (1, 10, 'alpha one'),
    (2, 20, 'beta two'),
    (3, 30, 'alpha three'),
    (5, 50, 'alpha five');
create table base (id int primary key, name varchar(50));
-- base rows for 10 and 30 only; docs id 5 (a matcher, base_id 50) has NO base partner.
insert into base values (10, 'b-ten'), (20, 'b-twenty'), (30, 'b-thirty');
-- fulltext2 builds asynchronously; force it synchronous so the queries below see a populated index.
alter table docs alter reindex ft2_idx fulltext2 force_sync;

-- Control: INNER join with the MATCH is served (matchers that also have a base row).
select d.id, b.name from docs d join base b on b.id = d.base_id
where match(d.title) against('alpha' in boolean mode) order by d.id;

-- #20687: LEFT join, MATCH on the preserved (left) fulltext2 side. Matchers are 1,3,5; id 5 has no
-- base partner and must survive null-extended (an INNER-join filter would wrongly drop it).
select d.id, b.name from docs d left join base b on b.id = d.base_id
where match(d.title) against('alpha' in boolean mode) order by d.id;

-- the preserved-side MATCH is served by the fulltext2 index scan, nested under the LEFT join.
-- @separator:table
-- @regex("fulltext2_search", true)
explain select d.id, b.name from docs d left join base b on b.id = d.base_id
where match(d.title) against('alpha' in boolean mode) order by d.id;

-- symmetric RIGHT join: the fulltext2 (preserved) table is the right child.
select d.id, b.name from base b right join docs d on b.id = d.base_id
where match(d.title) against('alpha' in boolean mode) order by d.id;

-- a MATCH on the preserved side combined with a projected score is served too.
select d.id, (match(d.title) against('alpha' in boolean mode) > 0) as hit, b.name
from docs d left join base b on b.id = d.base_id
where match(d.title) against('alpha' in boolean mode) order by d.id;

-- #20687 SINGLE join: a scalar subquery decorrelates to a SINGLE join whose PRESERVED (outer) child
-- carries the WHERE MATCH; the outer matcher with no subquery match keeps count 0 (id 5).
select d.id, (select count(*) from base b where b.id = d.base_id) as c
from docs d where match(d.title) against('alpha' in boolean mode) order by d.id;

drop database ft2_join;
