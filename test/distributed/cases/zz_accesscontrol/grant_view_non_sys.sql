set global enable_privilege_cache = off;

-- cleanup
drop account if exists acc_view_test;

-- setup: create tenant
create account acc_view_test admin_name = 'admin' identified by '111';

-- switch to tenant admin
-- @session:id=1&user=acc_view_test:admin&password=111
select account_id, user_name, role_name from mo_catalog.mo_user where user_name = 'admin';

-- setup inside tenant
create user user_v1 identified by '111';
create role role_v1;
grant role_v1 to user_v1;
create database db_v1;
use db_v1;
create table t1 (a int);
insert into t1 values (1);
create view v1 as select * from t1;

-- test case 1: default no permission for normal user
-- @session:id=2&user=acc_view_test:user_v1&password=111
use db_v1;
-- @ignore:error
select * from v1;
-- @session

-- back to tenant admin to grant
-- @session:id=1&user=acc_view_test:admin&password=111
use db_v1;
grant select on db_v1.v1 to role_v1;
-- @session

-- test case 2: verify permission
-- @session:id=2&user=acc_view_test:user_v1&password=111
use db_v1;
select * from v1;
-- @session

-- back to tenant admin to revoke
-- @session:id=1&user=acc_view_test:admin&password=111
revoke select on db_v1.v1 from role_v1;
-- @session

-- test case 3: verify revoked
-- @session:id=2&user=acc_view_test:user_v1&password=111
use db_v1;
-- @ignore:error
select * from v1;
-- @session

-- cleanup inside tenant (optional, dropping account cleans all)
-- @session

-- cleanup: drop account
drop account acc_view_test;
set global enable_privilege_cache = on;
