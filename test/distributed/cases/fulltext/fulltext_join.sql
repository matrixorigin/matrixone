-- fulltext JOIN rewrite: issue 24731

drop database if exists ft_join_test;
create database ft_join_test;
use ft_join_test;

create table ft (
    id varchar(191) primary key,
    base_id varchar(191),
    title varchar(512),
    body longtext,
    fulltext index ft_idx(title, body)
);

create table base (
    id varchar(191) primary key,
    name varchar(191),
    note text,
    body longtext,
    fulltext index base_ft_idx(name, note)
);

create table extra (
    id varchar(191) primary key,
    label varchar(191)
);

create table noft (
    id varchar(191) primary key,
    base_id varchar(191),
    title varchar(512),
    body longtext
);

insert into base values
    ('b1', 'Base One', 'base hello note', 'base hello body'),
    ('b2', 'Base Two', 'unrelated note', 'base unrelated body');

insert into extra values
    ('b1', 'extra one'),
    ('b2', 'extra two');

insert into ft values
    ('f1', 'b1', 'hello title', 'hello body'),
    ('f2', 'b2', 'other title', 'other body');

insert into noft values
    ('n1', 'b1', 'hello title', 'hello body');

-- original issue shape: fulltext table on the left side of JOIN
select f.id, b.name
from ft f join base b on b.id = f.base_id
where match(f.title, f.body) against('hello')
order by f.id;

-- reverse join order: fulltext table on the right side of JOIN
select f.id, b.name
from base b join ft f on b.id = f.base_id
where match(f.title, f.body) against('hello')
order by f.id;

-- explain should show that fulltext_match was rewritten to fulltext_index_scan
-- @separator:table
explain select f.id, b.name
from ft f join base b on b.id = f.base_id
where match(f.title, f.body) against('hello');

-- fulltext filter plus normal filter on the same scan
select f.id, b.name
from ft f join base b on b.id = f.base_id
where match(f.title, f.body) against('hello')
  and f.base_id = 'b1'
order by f.id;

-- multiple fulltext filters on the same scan
select f.id, b.name
from ft f join base b on b.id = f.base_id
where match(f.title, f.body) against('hello')
  and match(f.title, f.body) against('body')
order by f.id;

-- fulltext filters on both JOIN children
select f.id, b.name
from ft f join base b on b.id = f.base_id
where match(f.title, f.body) against('hello')
  and match(b.name, b.note) against('base')
order by f.id;

-- nested JOIN: inner JOIN is rewritten before the outer JOIN is visited
select f.id, b.name, e.label
from ft f join base b on b.id = f.base_id
join extra e on e.id = b.id
where match(f.title, f.body) against('hello')
order by f.id;

-- outer joins: a WHERE MATCH on the ROW-PRESERVED side is now served (#20687) -- ft is the
-- preserved child in both shapes below (left of the LEFT join, right of the RIGHT join).
select f.id, b.name
from ft f left join base b on b.id = f.base_id
where match(f.title, f.body) against('hello') order by f.id;

select f.id, b.name
from base b right join ft f on b.id = f.base_id
where match(f.title, f.body) against('hello') order by f.id;

-- no matching FULLTEXT INDEX: keep existing unsupported scalar behavior
select n.id, b.name
from noft n join base b on b.id = n.base_id
where match(n.title, n.body) against('hello');

-- fulltext index exists but columns do not match the MATCH list
select f.id, b.name
from ft f join base b on b.id = f.base_id
where match(f.title) against('hello');

-- cross-table MATCH should not be rewritten using only one table's index,
-- even when the other table has a column name that matches the index parts
select f.id, b.name
from ft f join base b on b.id = f.base_id
where match(f.title, b.body) against('hello');

-- #20687 null-extension counterexample: a WHERE MATCH on the preserved side of an outer join keeps
-- a matcher that has NO join partner, null-extending the other side (it must NOT be dropped -- an
-- INNER-join filter would drop it). lj_docs id 5 matches 'alpha' but has no lj_side row.
create table lj_docs (id int primary key, title varchar(200), fulltext index (title));
insert into lj_docs values (1,'alpha one'),(2,'beta'),(3,'alpha three'),(5,'alpha five');
create table lj_side (id int primary key, tag varchar(20));
insert into lj_side values (1,'p1'),(3,'p3');
-- matchers are 1,3,5; expected (1,p1),(3,p3),(5,NULL).
select d.id, s.tag from lj_docs d left join lj_side s on d.id = s.id
where match(d.title) against('alpha' in boolean mode) order by d.id;
-- the preserved-side MATCH is served by the fulltext index scan, not a full table scan.
-- @separator:table
-- @regex("fulltext_index_scan", true)
explain select d.id, s.tag from lj_docs d left join lj_side s on d.id = s.id
where match(d.title) against('alpha' in boolean mode) order by d.id;
-- symmetric RIGHT join: the fulltext (preserved) table is the right child.
select d.id, s.tag from lj_side s right join lj_docs d on d.id = s.id
where match(d.title) against('alpha' in boolean mode) order by d.id;

-- #20687 SINGLE join: a scalar subquery decorrelates to a SINGLE join whose PRESERVED (outer) child
-- carries the WHERE MATCH. The outer matcher with no subquery match keeps count 0 (id 5).
select d.id, (select count(*) from lj_side s where s.id = d.id) as c
from lj_docs d where match(d.title) against('alpha' in boolean mode) order by d.id;

drop database ft_join_test;
