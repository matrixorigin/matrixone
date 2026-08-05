set global enable_privilege_cache = off;
drop database if exists secondary_role_disabled_db;
drop user if exists secondary_role_disabled_user;
drop role if exists secondary_role_disabled_primary,secondary_role_disabled_secondary;

create database secondary_role_disabled_db;
create table secondary_role_disabled_db.t(a int);
insert into secondary_role_disabled_db.t values (1);
create role secondary_role_disabled_primary,secondary_role_disabled_secondary;
create user secondary_role_disabled_user identified by '123456' default role secondary_role_disabled_primary;
grant connect on account * to secondary_role_disabled_primary;
grant select on table secondary_role_disabled_db.t to secondary_role_disabled_secondary;
grant secondary_role_disabled_secondary to secondary_role_disabled_user;

-- @session:id=2&user=sys:secondary_role_disabled_user:secondary_role_disabled_primary&password=123456
select current_role();
-- A secondary role cannot be enabled; privileges remain limited to the one
-- current role until the user explicitly switches it with SET ROLE.
set secondary role all;
select current_role();
select * from secondary_role_disabled_db.t;
set secondary role none;
select current_role();
set role secondary_role_disabled_secondary;
select current_role();
select * from secondary_role_disabled_db.t;
-- @session

drop database secondary_role_disabled_db;
drop user secondary_role_disabled_user;
drop role secondary_role_disabled_primary,secondary_role_disabled_secondary;
set global enable_privilege_cache = on;
