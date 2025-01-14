-- account
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop pitr if exists pitr01;
create pitr pitr01 for account acc01 range 1 'h';
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
-- @ignore:0,1
select account_id, account_name from mo_catalog.mo_account where account_name = 'acc01';
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
-- @ignore:0,1
select account_id, account_name from mo_catalog.mo_account where account_name = 'acc01';
drop account if exists acc01;
drop pitr if exists pitr01;

-- database
drop database if exists abc1;
create database abc1;
drop pitr if exists pitr02;
create pitr pitr02 for database abc1 range 1 'h';
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
drop database abc1;
create database abc1;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
drop database abc1;
drop pitr if exists pitr02;

-- table
drop database if exists abc1;
create database abc1;
use abc1;
create table test1(a timestamp);
drop pitr if exists pitr03;
create pitr pitr03 for table abc1 test1 range 1 'h';
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
drop table test1;
create table test1(a timestamp);
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
drop database abc1;
drop pitr if exists pitr03;

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- @session:id=1&user=acc01:test_account&password=111
-- database
drop database if exists abc1;
create database abc1;
drop pitr if exists pitr02;
create pitr pitr02 for database abc1 range 1 'h';
-- @session
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
-- @session:id=1&user=acc01:test_account&password=111
drop database abc1;
create database abc1;
-- @session
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
-- @session:id=1&user=acc01:test_account&password=111
drop database abc1;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
-- table
drop database if exists abc1;
create database abc1;
use abc1;
create table test1(a timestamp);
drop pitr if exists pitr03;
create pitr pitr03 for table abc1 test1 range 1 'h';
-- @session
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
-- @session:id=1&user=acc01:test_account&password=111
drop table test1;
create table test1(a timestamp);
-- @session
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
-- @session:id=1&user=acc01:test_account&password=111
drop database abc1;
-- @session
drop account if exists acc01;


create pitr pitr_mo_catalog for database mo_catalog range 1 'h';
create pitr pitr_mysql for database mysql range 1 'h';
create pitr pitr_system for database system range 1 'h';
create pitr pitr_system_metrics for database system_metrics range 1 'h';
create pitr pitr_mo_task for database mo_task range 1 'h';
create pitr pitr_mo_debug for database mo_debug range 1 'h';
create pitr pitr_information_schema for database information_schema range 1 'h';
create pitr pitr_mo_catalog for table mo_catalog  mo_pitr range 1 'h';
create pitr pitr_mo_catalog for table mo_catalog  mo_snapshots range 1 'h';
-- @ignore:1,2
show pitr;
