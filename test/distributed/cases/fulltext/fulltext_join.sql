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

-- outer joins are intentionally not rewritten in the first phase
select f.id, b.name
from ft f left join base b on b.id = f.base_id
where match(f.title, f.body) against('hello');

select f.id, b.name
from base b right join ft f on b.id = f.base_id
where match(f.title, f.body) against('hello');

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

drop database ft_join_test;
