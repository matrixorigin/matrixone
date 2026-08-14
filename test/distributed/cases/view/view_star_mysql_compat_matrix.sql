-- @label:bvt
drop database if exists view_star_mysql_compat_matrix;
create database view_star_mysql_compat_matrix;
use view_star_mysql_compat_matrix;

create table t(a int primary key, b varchar(5));
create table u(a int primary key, c varchar(5));
insert into t values (1,'x'),(2,'yy');
insert into u values (1,'u');

create view v_direct as select * from t;
create view v_where as select * from t where a = 1 or a = 2;
create view v_named(x,y) as select * from t;
create view v_table_star as select tt.* from t tt;
create view v_join as select * from t join u using(a);
create view v_subquery as select * from (select * from t) sub;
create view v_count as select count(*) as cnt from t;
create view v_nested as select * from v_direct;
create view v_union as select * from t union all select * from t;
create view v_sample as select sample(*, 100 percent) from t;

select (select rel_createsql not like '%*%' and rel_createsql like '%`t`.`a`%' and rel_createsql like '%`t`.`b`%' from mo_catalog.mo_tables where reldatabase = 'view_star_mysql_compat_matrix' and relname = 'v_direct') as direct_sql_ok, (select rel_createsql not like '%*%' and rel_createsql like '%`t`.`a`%' and rel_createsql like '%`t`.`b`%' and rel_createsql like '%where%' from mo_catalog.mo_tables where reldatabase = 'view_star_mysql_compat_matrix' and relname = 'v_where') as where_sql_ok, (select rel_createsql not like '%*%' and rel_createsql like '%`tt`.`a`%' and rel_createsql like '%`tt`.`b`%' from mo_catalog.mo_tables where reldatabase = 'view_star_mysql_compat_matrix' and relname = 'v_table_star') as table_star_sql_ok, (select rel_createsql like '%count(*)%' and rel_createsql not like '%`t`.`a`%' from mo_catalog.mo_tables where reldatabase = 'view_star_mysql_compat_matrix' and relname = 'v_count') as count_star_boundary_ok, (select rel_createsql not like '%*%' from mo_catalog.mo_tables where reldatabase = 'view_star_mysql_compat_matrix' and relname = 'v_union') as union_sql_ok, (select rel_createsql like '%sample%' and rel_createsql like '%*%' from mo_catalog.mo_tables where reldatabase = 'view_star_mysql_compat_matrix' and relname = 'v_sample') as sample_sql_preserved;
select count(*) = 2 as sample_rows_ok from v_sample;

alter table t add column z int default 9 first;
alter table t add column d int default 7 after a;
alter table u add column e int default 8;

select (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_direct') = 'a,b' as direct_cols_ok, (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_where') = 'a,b' as where_cols_ok, (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_named') = 'x,y' as named_cols_ok, (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_table_star') = 'a,b' as table_star_cols_ok, (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_join') = 'a,b,c' as join_cols_ok, (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_subquery') = 'a,b' as subquery_cols_ok, (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_nested') = 'a,b' as nested_cols_ok, (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_union') = 'a,b' as union_cols_ok, (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_sample') = 'a,b' as sample_cols_ok, (select group_concat(column_name order by ordinal_position separator ',') from information_schema.columns where table_schema = 'view_star_mysql_compat_matrix' and table_name = 'v_count') = 'cnt' as count_cols_ok;

select (select count(*) from v_direct) = 2 as direct_rows_ok, (select count(*) from v_where) = 2 as where_rows_ok, (select count(*) from v_named) = 2 as named_rows_ok, (select count(*) from v_table_star) = 2 as table_star_rows_ok, (select count(*) from v_join) = 1 as join_rows_ok, (select count(*) from v_subquery) = 2 as subquery_rows_ok, (select count(*) from v_nested) = 2 as nested_rows_ok, (select count(*) from v_union) = 4 as union_rows_ok, (select cnt from v_count) = 2 as count_value_ok;
select * from v_union order by a;
select count(*) = 2 as sample_rebind_ok from v_sample;

-- error ER_BAD_FIELD_ERROR
select d from v_direct;
-- error ER_BAD_FIELD_ERROR
select z from v_table_star;
-- error ER_BAD_FIELD_ERROR
select e from v_join;

drop view v_nested;
drop view v_sample;
drop view v_union;
drop view v_count;
drop view v_subquery;
drop view v_join;
drop view v_table_star;
drop view v_named;
drop view v_where;
drop view v_direct;
drop table u;
drop table t;
drop database view_star_mysql_compat_matrix;
