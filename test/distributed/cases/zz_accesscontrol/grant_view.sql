set global enable_privilege_cache = off;

-- cleanup
drop user if exists user_v1;
drop role if exists role_v1;
drop database if exists db_v1;

-- setup
create user user_v1 identified by '111';
create role role_v1;
grant role_v1 to user_v1;
create database db_v1;
use db_v1;
create table t1 (a int);
insert into t1 values (1);
create view v1 as select * from t1;

-- test case 1: default no permission
-- @session:id=1&user=user_v1&password=111
use db_v1;
-- @ignore:error
select * from v1;
-- @session

-- test case 2: grant select on view to role (using default object type inference)
grant select on db_v1.v1 to role_v1;

-- @session:id=1&user=user_v1&password=111
use db_v1;
select * from v1;
-- @session

-- test case 3: revoke select on view from role
revoke select on db_v1.v1 from role_v1;

-- @session:id=1&user=user_v1&password=111
use db_v1;
-- @ignore:error
select * from v1;
-- @session

-- test case 4: grant select on view to user directly
grant select on db_v1.v1 to user_v1;

-- @session:id=1&user=user_v1&password=111
use db_v1;
select * from v1;
-- @session

-- test case 5: show grants
show grants for user_v1;

-- cleanup
drop user user_v1;
drop role role_v1;
drop database db_v1;
set global enable_privilege_cache = on;
