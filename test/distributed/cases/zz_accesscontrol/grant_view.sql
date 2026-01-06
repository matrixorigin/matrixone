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

-- cleanup
drop user user_v1;
drop role role_v1;
drop database db_v1;
set global enable_privilege_cache = on;