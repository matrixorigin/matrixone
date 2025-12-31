-- Test: Table privileges should persist after TRUNCATE operation
-- This test verifies that using rel_logical_id instead of rel_id for obj_id
-- in mo_role_privs ensures privileges are not lost after TRUNCATE

set global enable_privilege_cache = off;

-- ============================================================
-- Test Case 1: Basic truncate privilege persistence (sys tenant)
-- ============================================================

-- Cleanup
drop user if exists truncate_test_user;
drop role if exists truncate_test_role;
drop database if exists truncate_test_db;

-- Setup: Create database, table, role and user
create database truncate_test_db;
use truncate_test_db;
create table test_table (id int, name varchar(50));
insert into test_table values (1, 'before_truncate');

-- Create role and grant SELECT privilege on the specific table
create role truncate_test_role;
grant select on table truncate_test_db.test_table to truncate_test_role;

-- Create user and assign the role
create user truncate_test_user identified by '123456';
grant truncate_test_role to truncate_test_user;

-- Verify privilege is granted
select privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'truncate_test_role';

-- Test 1: User can select before truncate
-- @session:id=1&user=sys:truncate_test_user:truncate_test_role&password=123456
select * from truncate_test_db.test_table;
-- @session

-- Now truncate the table (this changes rel_id but should keep rel_logical_id)
truncate table test_table;
insert into test_table values (2, 'after_truncate');
alter table test_table add column age int DEFAULT 0;

-- Test 2: User should still be able to select after truncate
-- @session:id=2&user=sys:truncate_test_user:truncate_test_role&password=123456
select * from truncate_test_db.test_table;
-- @session

-- Verify privilege record still exists and is valid
select privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'truncate_test_role';

-- Cleanup
drop user if exists truncate_test_user;
drop role if exists truncate_test_role;
drop database if exists truncate_test_db;

-- ============================================================
-- Test Case 2: Table owner truncate privilege persistence (sub-account)
-- This tests the scenario where a user creates a table and truncates it
-- ============================================================

drop account if exists truncate_account_1;
create account truncate_account_1 ADMIN_NAME admin IDENTIFIED BY '111111';

-- @session:id=3&user=truncate_account_1:admin&password=111111
create role role1;
grant create database on account * to role1;
create user user1 identified by '123456' default role role1;
-- @session

-- User creates database and table, then truncates it
-- @session:id=4&user=truncate_account_1:user1:role1&password=123456
create database db1;
create table db1.t1(a int);
insert into db1.t1 values(1);
select * from db1.t1;
truncate table db1.t1;
insert into db1.t1 values(2);
select * from db1.t1;
-- @session

-- Cleanup
drop account if exists truncate_account_1;

set global enable_privilege_cache = on;
