-- #20567: block-scaled vector columns vecf8 (MXFP8) and vecf4 (NVFP4).
drop database if exists vecblock_db;
create database vecblock_db;
use vecblock_db;

create table t (id int primary key, a vecf8(4), b vecf4(4), c vecf32(4));
show create table t;
desc t;

insert into t values (1, '[1,-3,0,6]', '[1,-3,0,6]', '[1,-3,0,6]');
insert into t values (2, '[0.5,0.25,-0.75,1]', '[0.5,0.25,-0.75,1]', '[2,1,-1,0.5]');
insert into t values (3, null, null, null);
insert into t (id, a, b) values (4, '[3000,-12,0.001,1000000]', '[3000,-12,0.001,1000000]');
-- flush writes the block (zonemap bounds for every column)
-- @ignore:0
select mo_ctl('dn', 'flush', 'vecblock_db.t');
select * from t order by id;

-- casts
select cast('[1,2,3]' as vecf8(3)), cast('[1,2,3]' as vecf4(3));
select id, cast(a as vecf32(4)), cast(b as vecf32(4)) from t order by id;
select id, cast(c as vecf8(4)), cast(c as vecf4(4)) from t order by id;
select cast(a as vecf4(4)), cast(b as vecf8(4)) from t where id = 1;

-- rejected values
insert into t values (5, '[1,2,3]', '[1,2,3,4]', '[1,2,3,4]');
insert into t values (5, '[1,2,3,4]', '[1,2,3]', '[1,2,3,4]');
select cast('[1,2,3]' as vecf8(4));
insert into t (id, a) values (5, '[1,2,3,nan]');
insert into t (id, b) values (5, '[1,2,3,inf]');

-- arithmetic promotes to vecf32
select a + a, a * 2, b - c, a / 2, a + b from t where id = 1;

-- inner_product (MO returns the negated dot product; c is the vecf32 control)
select id, inner_product(c, c), inner_product(a, c), inner_product(b, c), inner_product(a, b), inner_product(a, '[1,1,1,1]') from t order by id;

-- ordering by value and grouping, as for vecf32
select id from t order by a, id;
select id from t order by b desc, id;
select id from t order by c, id;
select a, count(*) from t group by a order by a;
select count(distinct b) from t;
select id, rank() over (partition by a order by id) from t order by id;

-- aggregates
select count(a), count(b) from t;
select group_concat(a order by id separator ';') from t;
select any_value(b) from t where id = 1;

-- updates and schema change
update t set a = '[4,3,2,1]', b = '[4,3,2,1]' where id = 2;
select a, b from t where id = 2;
alter table t add column d vecf4(4) not null;
select id, d from t order by id;

-- prepared statement
prepare s from 'select id from t where inner_product(a, ?) < 0 order by id';
set @q = '[1,1,1,1]';
execute s using @q;
deallocate prepare s;

-- not supported
select l2_distance(a, c) from t;
select cosine_distance(b, b) from t;
select l1_distance(a, a) from t;
select sum(a) from t;
select avg(b) from t;
select id from t where a = a;
create index idx on t(a);
create unique index uidx on t(b);
create table t2 (a vecf8(4) primary key);
create table t3 (id int primary key, v vecf4(65536));

drop database vecblock_db;
