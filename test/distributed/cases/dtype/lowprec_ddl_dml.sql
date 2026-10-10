-- @suite
-- @case
-- @desc: DDL and DML over bf16/float16/float8/float4 and vecf8/vecf4 columns (#20567)
-- @label:bvt

drop database if exists lowprec_ddl_dml;
create database lowprec_ddl_dml;
use lowprec_ddl_dml;

-- create with defaults, NOT NULL and comments
create table t (
    id int primary key,
    b bf16 default 1.5 comment 'bf16 col',
    h float16 not null default -2,
    e float8 default 448,
    f float4,
    v vecf8(4) comment 'vecf8 col',
    w vecf4(4) not null
);
show create table t;
desc t;
insert into t (id, w) values (1, '[1,2,3,4]');
insert into t values (2, 0.1, 0.1, 0.1, 0.5, '[0.1,0.2,0.3,0.4]', '[0.5,1,1.5,2]');
insert into t values (3, null, 65504, -448, -6, null, '[6,-6,0,0.5]');
select * from t order by id;
-- NOT NULL and range are checked
insert into t (id, h, w) values (4, null, '[1,2,3,4]');
insert into t (id, w) values (4, null);
insert into t (id, e, w) values (4, 1000, '[1,2,3,4]');
insert into t (id, v, w) values (4, '[1,2,3]', '[1,2,3,4]');

-- insert from select: values cast from wider types
create table src (id int, d double, x vecf32(4));
insert into src values (10, 1.1, '[1.1,2.2,3.3,4.4]'), (11, -0.0, '[-0,0,1,-1]');
insert into t select id, d, d, d, d, x, x from src;
select * from t where id >= 10 order by id;

-- on duplicate key update and replace
insert into t values (2, 9, 9, 9, 6, '[9,9,9,9]', '[6,6,6,6]') on duplicate key update b = values(b) + 1, v = values(v);
replace into t values (3, 2, 2, 2, 2, '[2,2,2,2]', '[2,2,2,2]');
select * from t where id in (2, 3) order by id;

-- update and delete by the new types
-- @ignore:0
select mo_ctl('dn', 'flush', 'lowprec_ddl_dml.t');
update t set b = b * 2, e = e / 2 where f = 0.5;
update t set v = '[4,3,2,1]', w = cast('[4,3,2,1]' as vecf32(4)) where id = 1;
update t set h = 3 where v = cast('[2,2,2,2]' as vecf8(4));
select * from t order by id;
insert into t (id, e, w) values (12, 1, '[6,6,6,6]');
select count(*) from t where w = cast('[6,6,6,6]' as vecf4(4));
delete from t where e = 448;
delete from t where w = cast('[6,6,6,6]' as vecf4(4));
select count(*) from t where w = cast('[6,6,6,6]' as vecf4(4));
select id from t order by id;

-- alter table: add, modify, change, rename and drop columns
alter table t add column g bf16 default 0.25 after id;
alter table t add column u vecf8(2) default '[1,2]';
alter table t add column z float8 first;
show create table t;
select * from t order by id;
alter table t modify column f float8;
alter table t modify column e double;
alter table t modify column v vecf4(4);
alter table t modify column u vecf32(2);
alter table t change column h hh bf16 not null default 0;
alter table t rename column b to bb;
alter table t drop column z;
show create table t;
select * from t order by id;
-- narrowing a column rounds each value once; an out-of-range value is an error
create table nar (id int primary key, d double, x vecf32(2));
insert into nar values (1, 1.0625000001, '[1.0625000001,-3]'), (2, 300, '[300,0.5]');
alter table nar modify column d float8;
alter table nar modify column x vecf8(2);
select * from nar order by id;
insert into nar values (3, 1000, '[1,1]');
alter table nar modify column d float4;
select * from nar order by id;
-- keys and indexes on the new types are rejected by alter table as by create table
alter table t add index ib (bb);
alter table t add unique key uv (v);
alter table t drop primary key, add primary key (id, hh);
alter table t add column k float4 primary key;

-- like, as select, rename, truncate
create table t_like like t;
show create table t_like;
insert into t_like select * from t;
create table t_as as select id, bb, hh, v, w from t;
show create table t_as;
select * from t_as order by id;
rename table t_as to t_as2;
select count(*) from t_as2;
truncate table t_like;
select count(*) from t_like;

drop database lowprec_ddl_dml;
