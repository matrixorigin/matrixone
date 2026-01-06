set global enable_privilege_cache = off;

-- cleanup
drop user if exists user_v1;
drop role if exists role_v1;
drop database if exists db_v1;

-- setup
create role role_v1;
create user user_v1 identified by '111' default role role_v1;
grant role_v1 to user_v1;

create database db_v1;
use db_v1;
create table t1 (a int);
insert into t1 values (1);
create view v1 as select * from t1;

-- test: grant permissions
grant all on database db_v1 to role_v1;
grant select on view db_v1.v1 to role_v1;

-- test: verify actual select
-- @session:id=1&user=user_v1&password=111
set role role_v1;
use db_v1;
select * from v1;
-- @session

-- ==========================================================
-- Contrast Test: Full path SELECT vs USE database
-- ==========================================================
-- setup new user with NO database permission
drop user if exists user_no_db;
create user user_no_db identified by '111';
create role role_no_db;
grant role_no_db to user_no_db;
alter user user_no_db default role role_no_db;

-- grant ONLY view privilege using full path
grant select on view db_v1.v1 to role_no_db;

-- @session:id=2&user=user_no_db&password=111
-- 1. USE should fail
use db_v1;
-- 2. Full path SELECT should succeed thanks to short-circuit logic
select * from db_v1.v1;
-- @session

-- cleanup
drop user user_no_db;
drop role role_no_db;
drop user user_v1;
drop role role_v1;
drop database db_v1;
set global enable_privilege_cache = on;