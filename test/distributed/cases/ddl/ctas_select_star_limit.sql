-- CTAS SELECT * LIMIT 0 must resolve source columns in the internal executor.
drop database if exists ctas_select_star_limit;
create database ctas_select_star_limit;
use ctas_select_star_limit;

create table src (a int, b varchar(8));
insert into src values (1, 'x');
create table dst_empty as select * from src limit 0;
desc dst_empty;
select * from dst_empty;

create table dst_rows as select * from src;
select * from dst_rows;

create table src_key (id int primary key, v varchar(8), unique key(v));
insert into src_key values (1, 'k');
create table dst_key_empty as select * from src_key limit 0;
desc dst_key_empty;

create table dst_explicit as select a, b from src limit 0;
desc dst_explicit;

-- A rejected CTAS must not create its target and must not poison the session.
create table dst_missing as select * from missing_src limit 0;
show tables like 'dst_missing';
select count(*) from src;

drop database ctas_select_star_limit;
