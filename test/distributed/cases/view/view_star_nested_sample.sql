-- @label:bvt
drop database if exists view_star_nested_sample;
create database view_star_nested_sample;
use view_star_nested_sample;

create table t(a int primary key, b varchar(5));
create table u(id int primary key);
insert into t values (1, 'x'), (2, 'yy');
insert into u values (1);

create view v_sample as select sample(*, 100 percent) from t;
select (
    select rel_createsql like '%sample%'
       and rel_createsql not like '%sample(*%'
       and rel_createsql like '%`t`.`a`%'
       and rel_createsql like '%`t`.`b`%'
    from mo_catalog.mo_tables
    where reldatabase = 'view_star_nested_sample' and relname = 'v_sample'
) as sample_sql_ok;

create view v_outer as
select * from t
where exists (select sample(*, 100 percent) from u);

select (
    select rel_createsql like '%sample%'
       and rel_createsql like '%`t`.`a`%'
       and rel_createsql like '%`t`.`b`%'
       and rel_createsql not like '%select * from t%'
    from mo_catalog.mo_tables
    where reldatabase = 'view_star_nested_sample' and relname = 'v_outer'
) as outer_sample_sql_ok;

alter table t add column c int default 7;
select (
    select group_concat(column_name order by ordinal_position separator ',')
    from information_schema.columns
    where table_schema = 'view_star_nested_sample' and table_name = 'v_sample'
) = 'a,b' as sample_cols_ok;
select count(*) = 2 as sample_rows_ok from v_sample;
select * from v_sample order by a;
-- error ER_BAD_FIELD_ERROR
select c from v_sample;
select (
    select group_concat(column_name order by ordinal_position separator ',')
    from information_schema.columns
    where table_schema = 'view_star_nested_sample' and table_name = 'v_outer'
) = 'a,b' as outer_sample_cols_ok;
select count(*) = 2 as outer_sample_rows_ok from v_outer;
select * from v_outer order by a;

alter view v_outer as
select * from t
where exists (select sample(*, 100 percent) from u);
alter table t add column d int default 8;
select (
    select group_concat(column_name order by ordinal_position separator ',')
    from information_schema.columns
    where table_schema = 'view_star_nested_sample' and table_name = 'v_outer'
) = 'a,b,c' as altered_outer_sample_cols_ok;
select count(*) = 2 as altered_outer_sample_rows_ok from v_outer;

drop view v_outer;
drop view v_sample;
drop table u;
drop table t;
drop database view_star_nested_sample;
