-- @label:bvt
drop database if exists view_star_static_columns;
create database view_star_static_columns;
use view_star_static_columns;

create table t(a int primary key, b varchar(5));
insert into t values (1,'x'),(2,'yy');
create view v_star as select * from t;
create view v_named(x,y) as select * from t;

select rel_createsql not like '%*%' as no_star, rel_createsql like '%`t`.`a`%' as has_a, rel_createsql like '%`t`.`b`%' as has_b from mo_catalog.mo_tables where reldatabase = 'view_star_static_columns' and relname = 'v_star';
select column_name, ordinal_position from information_schema.columns where table_schema = 'view_star_static_columns' and table_name = 'v_star' order by ordinal_position;

alter table t add column z int default 9 first;
alter table t add column c int default 7 after a;
select * from v_star order by a;
select * from v_named order by x;
select column_name, ordinal_position from information_schema.columns where table_schema = 'view_star_static_columns' and table_name = 'v_star' order by ordinal_position;
-- error ER_BAD_FIELD_ERROR
select c from v_star order by a;

create table target2(a int,b varchar(5));
insert into target2 select * from v_star;
select count(*) as target_rows from target2;

create table copied as select * from v_star;
select column_name, ordinal_position from information_schema.columns where table_schema = 'view_star_static_columns' and table_name = 'copied' order by ordinal_position;

create view v_nested as select * from v_star;
select * from v_nested order by a;

alter table t rename column c to c2;
alter table t drop column c2;
alter table t rename column z to z2;
alter table t drop column z2;
select * from v_star order by a;

drop view v_nested;
drop view v_named;
drop view v_star;
drop table copied;
drop table target2;
drop table t;
drop database view_star_static_columns;
