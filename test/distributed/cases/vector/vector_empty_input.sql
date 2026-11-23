-- An empty string or an empty BLOB is not a vector: every insert and cast path is an error,
-- for every vector type, as for an INT column. LOAD reads an empty field as NULL, as for INT.
drop database if exists vector_empty_input;
create database vector_empty_input;
use vector_empty_input;

create table vi (id int, v vecf32(2), n int);
insert into vi values (1, '[1,2]', '');
insert into vi select 1, '[1,2]', '';

create table e32 (id int, v vecf32(2));
insert into e32 values (1, '');
insert into e32 select 2, cast('' as varchar(3));
prepare s from 'insert into e32 values (3, ?)';
set @e = '';
execute s using @e;
deallocate prepare s;
insert into e32 values (4, cast(unhex('') as blob));
select count(*) from e32;

create table e64 (id int, v vecf64(2));
insert into e64 values (1, '');
insert into e64 select 2, cast('' as varchar(3));
create table ebf (id int, v vecbf16(2));
insert into ebf select 2, cast('' as varchar(3));
create table ef16 (id int, v vecf16(2));
insert into ef16 select 2, cast('' as varchar(3));
create table ei8 (id int, v vecint8(2));
insert into ei8 select 2, cast('' as varchar(3));
create table eu8 (id int, v vecuint8(2));
insert into eu8 select 2, cast('' as varchar(3));
create table ef8 (id int, v vecf8(2));
insert into ef8 values (1, '');
insert into ef8 select 2, cast('' as varchar(3));
insert into ef8 values (4, cast(unhex('') as blob));
create table ef4 (id int, v vecf4(2));
insert into ef4 values (1, '');
insert into ef4 select 2, cast('' as varchar(3));
select (select count(*) from e64) + (select count(*) from ebf) + (select count(*) from ef16) + (select count(*) from ei8) + (select count(*) from eu8) + (select count(*) from ef8) + (select count(*) from ef4) as stored;

-- UPDATE: an error, the row unchanged
create table up (id int, v vecf32(2), w vecf8(2));
insert into up values (1, '[1,2]', '[1,2]');
update up set v = cast('' as varchar(3)) where id = 1;
update up set w = '' where id = 1;
select id, v, w from up;

-- NULL stays NULL
insert into e32 values (5, null);
insert into ef8 select 5, cast(null as varchar(3));
select id, v is null from e32 order by id;
select id, v is null from ef8 order by id;

-- LOAD: an empty field is NULL
create table l32 (id int, v vecf32(2), n int);
load data infile '$resources/vector/vector_empty_field.csv' into table l32 fields terminated by ',' enclosed by '"';
select id, v, n from l32 order by id;
create table l8 (id int, v vecf8(2), n int);
load data infile '$resources/vector/vector_empty_field.csv' into table l8 fields terminated by ',' enclosed by '"';
select id, v, n from l8 order by id;

drop database vector_empty_input;
