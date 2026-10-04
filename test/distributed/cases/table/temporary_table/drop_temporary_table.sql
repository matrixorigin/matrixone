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

-- Non-admin temporary-table DDL must not change a shadowed permanent table's
-- ownership, for either DROP spelling or a mixed multi-target DROP.
set global enable_privilege_cache = off;
drop database if exists drop_temporary_owner_db;
drop user if exists drop_temporary_worker_user;
drop role if exists drop_temporary_base_owner, drop_temporary_worker;
create role drop_temporary_base_owner, drop_temporary_worker;
create database drop_temporary_owner_db;
create table drop_temporary_owner_db.t_shadow (id int primary key);
create table drop_temporary_owner_db.t_persistent (id int primary key);
grant ownership on table drop_temporary_owner_db.t_shadow to drop_temporary_base_owner;
grant ownership on table drop_temporary_owner_db.t_persistent to drop_temporary_base_owner;
create user drop_temporary_worker_user identified by '123456' default role drop_temporary_worker;
grant connect on account * to drop_temporary_worker;
grant create table, drop table on database drop_temporary_owner_db to drop_temporary_worker;
grant insert, select on table drop_temporary_owner_db.* to drop_temporary_worker;

-- @session:id=2&user=sys:drop_temporary_worker_user&password=123456
use drop_temporary_owner_db;
create temporary table t_shadow (id int primary key);
insert into t_shadow values (2);
-- @session
select p.role_name from mo_catalog.mo_role_privs p join mo_catalog.mo_tables t on p.obj_id = t.rel_id where p.privilege_name = 'table ownership' and t.reldatabase = 'drop_temporary_owner_db' and t.relname = 't_shadow' order by p.role_name;

-- @session:id=2&user=sys:drop_temporary_worker_user&password=123456
drop table t_shadow;
-- @session
select p.role_name from mo_catalog.mo_role_privs p join mo_catalog.mo_tables t on p.obj_id = t.rel_id where p.privilege_name = 'table ownership' and t.reldatabase = 'drop_temporary_owner_db' and t.relname = 't_shadow' order by p.role_name;

-- @session:id=2&user=sys:drop_temporary_worker_user&password=123456
create temporary table t_shadow (id int primary key);
drop temporary table t_shadow;
-- @session
select p.role_name from mo_catalog.mo_role_privs p join mo_catalog.mo_tables t on p.obj_id = t.rel_id where p.privilege_name = 'table ownership' and t.reldatabase = 'drop_temporary_owner_db' and t.relname = 't_shadow' order by p.role_name;

-- @session:id=2&user=sys:drop_temporary_worker_user&password=123456
create temporary table t_shadow (id int primary key);
drop table t_shadow, t_persistent;
-- @session
select p.role_name from mo_catalog.mo_role_privs p join mo_catalog.mo_tables t on p.obj_id = t.rel_id where p.privilege_name = 'table ownership' and t.reldatabase = 'drop_temporary_owner_db' and t.relname = 't_shadow' order by p.role_name;
select count(*) from mo_catalog.mo_tables where reldatabase = 'drop_temporary_owner_db' and relname = 't_persistent';

drop database drop_temporary_owner_db;
drop user drop_temporary_worker_user;
drop role drop_temporary_base_owner, drop_temporary_worker;
set global enable_privilege_cache = on;
