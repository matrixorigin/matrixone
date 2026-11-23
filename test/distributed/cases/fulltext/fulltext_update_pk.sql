-- #25617: an UPDATE that changes a primary key on a table with a SYNCHRONOUS
-- fulltext / IVF index re-keys the hidden index entries in the same statement,
-- so index-served reads return the new key and no entry is left under the old key.

set experimental_fulltext_index = 1;
set experimental_ivf_index = 1;

drop database if exists db25617;
create database db25617;
use db25617;

-- synchronous FULLTEXT index
create table ft (id bigint primary key, body text, tag int, fulltext fti(body));
insert into ft values (1, 'alpha one', 0), (2, 'beta two', 0);
update ft set id = 5 where id = 1;
select id from ft order by id;
select id from ft where match(body) against('alpha');
-- a non-pk, non-indexed column update still works
update ft set tag = 7 where id = 5;
select id, tag from ft order by id;
select id from ft where match(body) against('alpha');

-- synchronous IVF_FLAT index
create table iv (id bigint primary key, v vecf32(3));
create index ix using ivfflat on iv(v) lists = 2 op_type 'vector_l2_ops';
insert into iv values (1, '[1,1,1]'), (2, '[9,9,9]');
update iv set id = 5 where id = 1;
select id from iv order by id;
select id from iv order by l2_distance(v, '[1,1,1]') limit 1;

-- control: a plain table (no irregular index) can still update its primary key
create table plain (id bigint primary key, x int);
insert into plain values (1, 1);
update plain set id = 9 where id = 1;
select id from plain order by id;

drop database db25617;
