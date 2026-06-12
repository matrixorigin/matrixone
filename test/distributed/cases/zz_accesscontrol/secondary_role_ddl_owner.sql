set global enable_privilege_cache = off;
drop database if exists ddl_owner_db;
drop user if exists ddl_owner_user;
drop role if exists ddl_owner_primary,ddl_owner_secondary;
create role ddl_owner_primary,ddl_owner_secondary;
create database ddl_owner_db;
create user ddl_owner_user identified by '123456' default role ddl_owner_primary;
grant connect on account * to ddl_owner_primary;
grant connect on account * to ddl_owner_secondary;
grant create table on database ddl_owner_db to ddl_owner_secondary;
grant insert,select on table ddl_owner_db.* to ddl_owner_secondary;
grant ddl_owner_secondary to ddl_owner_user;
-- @session:id=2&user=sys:ddl_owner_user&password=123456
set secondary role all;
select current_role();
create table ddl_owner_db.t1(a int);
insert into ddl_owner_db.t1 values(1);
select * from ddl_owner_db.t1;
-- @session
select r.role_name from mo_catalog.mo_tables t join mo_catalog.mo_role r on t.owner = r.role_id where t.reldatabase = 'ddl_owner_db' and t.relname = 't1';
select role_name, privilege_name, privilege_level from mo_catalog.mo_role_privs where role_name in ('ddl_owner_primary','ddl_owner_secondary') and privilege_name = 'table ownership' order by role_name, privilege_level;
revoke ddl_owner_secondary from ddl_owner_user;
-- @session:id=2&user=sys:ddl_owner_user&password=123456
set secondary role all;
select * from ddl_owner_db.t1;
-- @session
drop database if exists ddl_owner_db;
drop user if exists ddl_owner_user;
drop role if exists ddl_owner_primary,ddl_owner_secondary;
set global enable_privilege_cache = on;
