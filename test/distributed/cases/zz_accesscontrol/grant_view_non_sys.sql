set global enable_privilege_cache = off;

-- cleanup
drop account if exists acc_v1;

-- setup: create tenant
create account acc_v1 admin_name = 'admin' identified by '111';

-- @session:id=1&user=acc_v1:admin&password=111
create role role_v1;
create user user_v1 identified by '111' default role role_v1;
grant role_v1 to user_v1;

create database db_v1;
use db_v1;
create table t1 (a int);
insert into t1 values (1);
create view v1 as select * from t1;

-- grant permissions
grant connect on account * to role_v1;
grant select on view db_v1.v1 to role_v1;

-- @session:id=2&user=acc_v1:user_v1&password=111
use db_v1;
-- verify select succeeds
select * from v1;
-- @session

-- back to tenant admin
-- @session:id=1&user=acc_v1:admin&password=111
revoke select on view db_v1.v1 from role_v1;
-- @session

-- verify revoked in NEW session to avoid session-level cache issues
-- @session:id=3&user=acc_v1:user_v1&password=111
use db_v1;
-- should fail
select * from v1;
-- @session

-- cleanup
-- @session:id=1&user=acc_v1:admin&password=111
drop database db_v1;
-- @session

drop account acc_v1;
set global enable_privilege_cache = on;