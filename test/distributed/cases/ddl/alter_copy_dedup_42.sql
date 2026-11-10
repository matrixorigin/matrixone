-- COPY must validate changed primary and unique key values before committing.
drop database if exists alter_copy_dedup_42;
create database alter_copy_dedup_42;
use alter_copy_dedup_42;
create table u (id int primary key, val decimal(5,2), unique key uv(val));
insert into u values (1,1.21),(2,1.24);
alter table u modify val decimal(5,1);
show create table u;
select id, cast(val * 100 as signed) from u force index(uv) order by id;
insert into u values (3,2.50);
select id, cast(val * 100 as signed) from u ignore index(uv) order by id;
create table ok (id int primary key, val decimal(5,2), unique key uv(val));
insert into ok values (1,1.20),(2,2.30);
alter table ok modify val decimal(5,1);
select id, cast(val * 10 as signed) from ok force index(uv) order by id;
alter table ok rename column val to value1, add column extra int default 9;
select id, cast(value1 * 10 as signed), extra from ok force index(uv) order by id;
drop database alter_copy_dedup_42;
