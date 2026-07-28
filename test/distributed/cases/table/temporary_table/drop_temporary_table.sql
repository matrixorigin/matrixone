drop database if exists drop_temporary_table_db;
create database drop_temporary_table_db;
use drop_temporary_table_db;

create table t_shadow (id int primary key, v varchar(20));
insert into t_shadow values (1, 'base');
create temporary table t_shadow (id int primary key, v varchar(20));
insert into t_shadow values (2, 'temp');

select id, v from t_shadow order by id;
drop temporary table t_shadow;
select id, v from t_shadow order by id;

-- IF EXISTS must not drop the permanent table when no temporary table exists.
drop temporary table if exists t_shadow;
select id, v from t_shadow order by id;

drop database drop_temporary_table_db;
