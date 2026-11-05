-- A replacement column remains a new value source across later ALTER clauses.
drop database if exists alter_copy_source_42;
create database alter_copy_source_42;
use alter_copy_source_42;
create table plain (id int primary key, v int);
insert into plain values (1,10),(2,20);
alter table plain drop column v, add column v int default 7;
select * from plain order by id;
create table u (id int primary key, v int, unique key uv(v));
insert into u values (1,10),(2,20);
alter table u drop column v, add column v int default 7 unique, modify column v int default 7;
show create table u;
select * from u force index(uv) order by id;
insert into u values (3,30);
select * from u ignore index(uv) order by id;
drop database alter_copy_source_42;
