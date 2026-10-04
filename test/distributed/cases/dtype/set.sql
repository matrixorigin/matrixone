-- @suit
-- @case
-- @desc: datatype:set
-- @label:bvt

drop table if exists set01;
create table set01 (
    id int primary key,
    colors set('red', 'green', 'blue')
);

show create table set01;
desc set01;
select table_name, column_name, data_type, column_type, is_nullable
from information_schema.columns
where table_name = 'set01' and column_name not like '__mo%'
order by column_name;

insert into set01 values
    (1, 'red'),
    (2, 'blue,red'),
    (3, 3),
    (4, ''),
    (5, null);

select * from set01 order by id;
select * from set01 where colors = 'red,green' order by id;
-- SET keeps its comma-separated display value in string comparisons, but uses
-- its member bitmap for arithmetic, bitwise, and numeric comparison contexts.
select id, colors + 0, colors & 1, colors = 'red,blue' from set01 order by id;
select id from set01 where colors & 1 order by id;
select id from set01 where colors = 3 order by id;
select * from set01 order by colors;
-- SET bit-order, including multi-member values, must survive a derived table.
select id, colors from (select id, colors from set01) d order by colors, id;
select colors from (select colors from set01 group by colors) d order by colors;
with c as (select colors from set01)
select group_concat(colors order by colors separator '|') as ordered_values
from c where colors is not null and colors <> '';

-- SET('', 'a') maps bitmap 0 and bitmap 1 to the same display value. Direct
-- sorting and transparent projections retain raw identity. Equality boundaries
-- sort the canonical bitmap of the surviving display, without splitting it.
drop table if exists set_empty_member_order;
create table set_empty_member_order (id int primary key, tags set('', 'a'));
insert into set_empty_member_order values (2, 0), (1, 1), (3, 2);
select id, tags from set_empty_member_order order by tags, id;
select id, tags from (select id, tags from set_empty_member_order) d order by tags, id;
with c as (select id, tags from set_empty_member_order)
select id, tags from c order by tags, id;
select tags, count(*) as cnt from set_empty_member_order group by tags order by tags;
select distinct tags from set_empty_member_order order by tags;
drop table set_empty_member_order;

-- An empty member in the middle distinguishes raw, canonical and lexical order.
drop table if exists set_boundary_order;
create table set_boundary_order (id int primary key, s set('z', '', 'a'));
insert into set_boundary_order values (1, 2), (2, 0), (3, 1), (4, 4), (5, null);
select id, s, s + 0 as bitmap from (select id, s from set_boundary_order) d order by s, id;
select s, count(*) as cnt from set_boundary_order group by s order by s;
select distinct s from set_boundary_order order by s;
select s from (select distinct s from set_boundary_order) d order by s;
select cast(s as unsigned) as bitmap from (select distinct s from set_boundary_order) d order by bitmap;
select id, s, s + 0 as bitmap, cast(s as unsigned) as cast_bitmap
from (select id, s from set_boundary_order order by id limit 5) d order by s, id;
select id, s from set_boundary_order order by cast(s as char), id;
delete from set_boundary_order;
insert into set_boundary_order values (5, null), (4, 4), (3, 1), (2, 0), (1, 2);
select s, count(*) as cnt from set_boundary_order group by s order by s;
select distinct s from set_boundary_order order by s;
drop table set_boundary_order;

drop table if exists set_idx;
create table set_idx (
    id int primary key,
    colors set('red', 'green', 'blue'),
    key idx_colors (colors)
);
show create table set_idx;
insert into set_idx values (1, 'red'), (2, 'red,green');
select * from set_idx where colors = 'red,green' order by id;
drop table set_idx;

drop table if exists set_load;
create table set_load (
    id int primary key,
    colors set('red', 'green', 'blue')
);
load data infile '$resources/load_data/set.csv'
into table set_load
fields terminated by ','
optionally enclosed by '"';
select * from set_load order by id;
drop table set_load;

update set01 set colors = 'green,red' where id = 1;
update set01 set colors = 5 where id = 4;
select * from set01 order by id;

insert into set01 values (6, 'yellow');

drop table if exists set02;
create table set02 as select colors from set01;
show create table set02;
select * from set02 order by colors;
drop table set02;

drop table if exists set03;
create table set03 (
    id int primary key,
    tags set('x', 'y') not null default 'x'
);
insert into set03 values (1, default);
insert into set03 values (2, 3);
show create table set03;
select * from set03 order by id;
drop table set03;

-- ============================================================
-- Additional coverage: ALTER MODIFY shrink, DISTINCT, GROUP BY, LOAD DATA error
-- ============================================================

-- 1a. ALTER TABLE MODIFY COLUMN: shrink member list (must error)
--     Old data has 'c' (bit 4), after modify to set('a','b') the bitmask is invalid.
--     This must NOT silently keep the stale bitmask.
drop table if exists set_modify;
create table set_modify (id int primary key, tags set('a','b','c'));
insert into set_modify values (1, 'a,c'), (2, 'b');
alter table set_modify modify column tags set('a','b');
drop table set_modify;

-- 1b. ALTER TABLE MODIFY COLUMN: expand member list (must succeed)
drop table if exists set_modify2;
create table set_modify2 (id int primary key, tags set('a','b'));
insert into set_modify2 values (1, 'a,b'), (2, 'a');
alter table set_modify2 modify column tags set('a','b','c');
insert into set_modify2 values (3, 'a,c');
select * from set_modify2 order by id;
drop table set_modify2;

-- 2. DISTINCT on SET column
select distinct colors from set01 order by colors;
select distinct concat('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', colors)
from set01
order by concat('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', colors);

-- 3. GROUP BY on SET column
select colors, count(*) as cnt from set01 group by colors order by colors;

-- 4. LOAD DATA with invalid member (error expected)
drop table if exists set_load_err;
create table set_load_err (id int primary key, colors set('red','green','blue'));
load data infile '$resources/load_data/set_bad.csv'
into table set_load_err
fields terminated by ','
optionally enclosed by '"';
select * from set_load_err;
drop table set_load_err;

drop table set01;
