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
select * from set01 order by colors;

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

-- 3. GROUP BY on SET column
select colors, count(*) as cnt from set01 group by colors order by colors;

-- 4. LOAD DATA with invalid member (row should be skipped, table stays empty)
drop table if exists set_load_err;
create table set_load_err (id int primary key, colors set('red','green','blue'));
load data infile '$resources/load_data/set_bad.csv'
into table set_load_err
fields terminated by ','
optionally enclosed by '"';
select * from set_load_err;
drop table set_load_err;

drop table set01;
