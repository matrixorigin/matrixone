set global enable_privilege_cache = off;

-- cleanup
drop user if exists user_complex;
drop role if exists role_level1, role_level2, role_level3;
drop database if exists db_complex;

-- setup
create role role_level1;
create role role_level2;
create role role_level3;
create user user_complex identified by '111' default role role_level3;

create database db_complex;
use db_complex;
create table t1 (a int, b varchar(20));
insert into t1 values (1, 'base');
create view v1 as select * from t1;
create view v2 as select a from t1 where a > 0;
create view v3 as select * from v1; -- view on view

-- ==========================================================
-- Scenario 1: Multi-level Role Inheritance
-- ==========================================================
grant select on view db_complex.v1 to role_level1;
grant role_level1 to role_level2;
grant role_level2 to role_level3;
grant role_level3 to user_complex;
grant connect on account * to role_level1;

-- verify storage
select obj_type, privilege_name from mo_catalog.mo_role_privs where role_name = 'role_level1';

-- @session:id=1&user=user_complex&password=111
-- check current role
select current_role();
-- Should succeed due to inheritance
select * from db_complex.v1;
-- @session

-- ==========================================================
-- Scenario 2: Database level wildcard for Views
-- ==========================================================
grant select on view db_complex.* to role_level1;

-- @session:id=1&user=user_complex&password=111
-- Should succeed for all views in db_complex
select * from db_complex.v2;
select * from db_complex.v3;
-- @session

-- ==========================================================
-- Scenario 3: Multiple privileges and partial revoke
-- ==========================================================
grant select, update, insert on view db_complex.v2 to role_level2;

-- @session:id=1&user=user_complex&password=111
-- show grants;
select * from db_complex.v2;
-- @session

-- Revoke only one privilege
revoke update on view db_complex.v2 from role_level2;

-- @session:id=1&user=user_complex&password=111
-- select should still succeed
select * from db_complex.v2;
-- @session

-- ==========================================================
-- Scenario 4: GRANT ALL and OWNERSHIP on View
-- ==========================================================
grant all on view db_complex.v3 to role_level1;
grant ownership on view db_complex.v3 to role_level2;

-- @session:id=1&user=user_complex&password=111
select * from db_complex.v3;
-- @session

-- cleanup
drop user user_complex;
drop role role_level1, role_level2, role_level3;
drop database db_complex;
set global enable_privilege_cache = on;
