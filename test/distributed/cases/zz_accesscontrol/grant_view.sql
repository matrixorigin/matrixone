set global enable_privilege_cache = off;

-- cleanup
drop user if exists user_v1;
drop database if exists db_v1;

-- setup
create user user_v1 identified by '111';
create database db_v1;
use db_v1;
create table t1 (a int);
insert into t1 values (1);
create view v1 as select * from t1;

-- test case 1: only view privilege (already failed)
grant select on view db_v1.v1 to public;

-- test case 2: grant underlying table privilege
grant select on table db_v1.t1 to public;

-- test: verify actual select
-- @session:id=1&user=user_v1&password=111
use db_v1;
select * from v1;
-- @session

-- cleanup
drop user user_v1;
drop database db_v1;
set global enable_privilege_cache = on;
