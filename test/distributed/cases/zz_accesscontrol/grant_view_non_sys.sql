set global enable_privilege_cache = off;

-- cleanup
drop account if exists acc_view_test;

-- setup: create tenant
create account acc_view_test admin_name = 'admin' identified by '111';

-- switch to tenant admin
-- @session:id=1&user=acc_view_test:admin&password=111
-- setup inside tenant
create role role_v1;
create user user_v1 identified by '111' default role role_v1;
grant role_v1 to user_v1;

create database db_v1;
use db_v1;
create table t1 (a int);
insert into t1 values (1);
create view v1 as select * from t1;

-- test case 1: grant permissions
grant all on database db_v1 to role_v1;
grant select on view db_v1.v1 to role_v1;

-- test case 2: verify permission
-- @session:id=2&user=acc_view_test:user_v1&password=111
set role role_v1;
use db_v1;
select * from v1;
-- @session

-- cleanup
-- @session:id=1&user=acc_view_test:admin&password=111
drop database db_v1;
-- @session

-- cleanup: drop account
drop account acc_view_test;
set global enable_privilege_cache = on;