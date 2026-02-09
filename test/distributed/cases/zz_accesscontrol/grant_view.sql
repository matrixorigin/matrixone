set global enable_privilege_cache = off;

-- cleanup
drop user if exists user_v1, user_no_db, user_invoker;
drop role if exists role_v1, role_no_db, role_invoker;
drop database if exists db_v1;

-- setup
create role role_v1;
create user user_v1 identified by '111' default role role_v1;
grant role_v1 to user_v1;

create database db_v1;
use db_v1;
create table t1 (a int, b varchar(20));
insert into t1 values (1, 'base');
create view v1 as select * from t1;
create view v_on_v as select * from v1;

-- ==========================================================
-- Test 1: Basic View SELECT (DEFINER security)
-- ==========================================================
-- grant only view privilege, NO physical table privilege
grant select on view db_v1.v1 to role_v1;
-- grant connect privilege on account level for 'USE'
grant connect on account * to role_v1;

-- @session:id=1&user=user_v1&password=111
-- verify role is active
select current_role();
use db_v1;
-- this should succeed now with the new short-circuit logic
select * from v1;
-- @session

-- ==========================================================
-- Test 2: Full path SELECT without USE database permission
-- ==========================================================
create role role_no_db;
create user user_no_db identified by '111' default role role_no_db;
grant role_no_db to user_no_db;
grant select on view db_v1.v1 to role_no_db;
-- DO NOT grant connect/database privilege to user_no_db

-- @session:id=2&user=user_no_db&password=111
-- USE should fail (no database permission)
use db_v1;
-- Full path SELECT should succeed
select * from db_v1.v1;
-- @session

-- ==========================================================
-- Test 3: View on View (Lineage)
-- ==========================================================
grant select on view db_v1.v_on_v to role_v1;

-- @session:id=1&user=user_v1&password=111
use db_v1;
select * from v_on_v;
-- @session

-- ==========================================================
-- Test 4: INVOKER security type
-- ==========================================================
use db_v1;
set view_security_type = 'INVOKER';
create view v_invoker as select * from t1;
set view_security_type = 'DEFINER';

create role role_invoker;
create user user_invoker identified by '111' default role role_invoker;
grant role_invoker to user_invoker;
grant connect on account * to role_invoker;
grant select on view db_v1.v_invoker to role_invoker;

-- @session:id=3&user=user_invoker&password=111
use db_v1;
-- This should FAIL because user has no privilege on t1
select * from v_invoker;
-- @session

-- Now grant privilege on t1
grant select on table db_v1.t1 to role_invoker;

-- @session:id=3&user=user_invoker&password=111
use db_v1;
-- This should SUCCEED now
select * from v_invoker;
-- @session

-- ==========================================================
-- Test 5: Other privilege types (INSERT/UPDATE/ALL)
-- ==========================================================
grant all on view db_v1.v1 to role_v1;
show grants for role_v1;

-- ==========================================================
-- Test 6: Revoke and cleanup
-- ==========================================================
-- First revoke 'all' to be sure
revoke all on view db_v1.v1 from role_v1;
-- Now grant and revoke select
grant select on view db_v1.v1 to role_v1;
revoke select on view db_v1.v1 from role_v1;

-- @session:id=1&user=user_v1&password=111
use db_v1;
-- Should fail
select * from v1;
-- @session

-- cleanup
drop user user_v1;
drop user user_no_db;
drop user user_invoker;
drop role role_v1;
drop role role_no_db;
drop role role_invoker;
drop database db_v1;
set global enable_privilege_cache = on;
