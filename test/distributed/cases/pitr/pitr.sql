drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- sys creates pitr for cluster, show pitr
drop pitr if exists pitr01;
create pitr pitr01 for cluster range 1 'h';
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
alter pitr pitr01 range 10 'd';
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
drop pitr pitr01;
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';


-- sys creates pitr for account, show pitr
drop pitr if exists p02;
create pitr p02 for account acc01 range 1 'd';
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
alter pitr p02 range 100 'd';
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
drop pitr p02;
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';


-- nonsys creates pitr for current account
-- @session:id=1&user=acc01:test_account&password=111
drop pitr if exists `select`;
create pitr `select` for account range 10 'd';
-- @ignore:1,2
show pitr;
-- @session
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
-- @session:id=1&user=acc01:test_account&password=111
alter pitr `select` range 30 'd';
-- @ignore:1,2
show pitr;
-- @session
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
-- @session:id=1&user=acc01:test_account&password=111
drop pitr `select`;
-- @session
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';


drop database if exists test01;
create database test01;
drop pitr if exists account;
create pitr account for database test01 range 1 'mo';
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
alter pitr account range 4 'mo';
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
drop pitr account;
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';


use test01;
create table t1 (col1 int, col2 decimal);
insert into t1 values (1,2);
insert into t1 values (2,3);
drop pitr if exists `$%^#`;
create pitr `$%^#` for table test01  t1 range 1 'y';
select * from t1;
show create table t1;
truncate t1;
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
alter pitr `$%^#` range 2 'mo';
drop pitr `$%^#`;
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
drop table t1;
drop database test01;
-- @session


-- verify data
drop database if exists test;
create database test;
use test;
drop table if exists s3t;
create table s3t (a int, b int, c int, primary key(a, b));
insert into s3t select result, 2, 12 from generate_series(1, 30000, 1) g;
select count(*) from s3t;
select sum(a) from s3t;
drop pitr if exists p03;
create pitr p03 for table test s3t range 2 'h';
select count(*) from s3t;
select sum(a) from s3t;
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
drop pitr p03;
drop database test;


-- abnromal test
-- create pitr for not exist account/database/table
drop pitr if exists p04;
create pitr p04 for account acc10 range 10 'd';


-- create pitr for not exists database
drop pitr if exists p05;
create pitr p05 database t1 range 3 'h';


-- create pitr for not exists table
drop database if exists test01;
create database test01;
drop pitr if exists p06;
create pitr p06 table database test01 table01 range 20 'y';
drop database test01;


-- abnormal test: time beyond the time range when creating pitr
create pitr p07 for account range 102 'd';
create pitr p07 for account range 200 'h';
create pitr p07 for account range 300 'y';
create pitr p07 for account range 500 'mo';
create pitr p07 for account range -1 'd';
create pitr p07 for account range -2 'h';
create pitr p07 for account range -3 'y';
create pitr p07 for account range 0 'mo';
create pitr p07 for account range 1 'day';
create pitr p07 for account range 2 'hour';
create pitr p07 for account range 3 'year';
create pitr p07 for account range 20 'month';


-- abnormal test: sys create duplicate pitr for sys (object: account)
drop pitr if exists p01;
create pitr p01 for cluster range 1 'd';
create pitr p01 for cluster range 10 'h';
drop pitr p01;


-- abnormal test: sys create duplicate pitr for sys (object: account)
drop pitr if exists p10;
create pitr p10 for account acc01 range 1 'd';
create pitr p11 for account acc01 range 10 'h';
drop pitr p10;


-- abnormal test: sys create duplicate pitr for sys (object: database)
drop database if exists test;
create database test;
drop pitr if exists p10;
create pitr p10 for database test range 10 'y';
create pitr p11 for database test range 11 'd';
drop pitr p10;
drop database test;


-- abnormal test: sys create duplicate pitr for sys (object: table01)
drop database if exists test;
create database test;
use test;
create table t1(col int, col2 decimal);
insert into t1 values(1,1);
drop pitr if exists p10;
create pitr p10 for table test  t1 range 10 'y';
create pitr p11 for table test  t1 range 11 'd';
drop pitr p10;
drop database test;


-- abnormal test: sys create duplicate pitr for nonsys (object: account)
drop pitr if exists pitr01;
create pitr pitr01 for account acc01 range 11 'mo';
create pitr pitr02 for account acc01 range 11 'mo';


-- abnormal test: nonsys create duplicate pitr for sys (object: account)
-- @session:id=1&user=acc01:test_account&password=111
drop pitr if exists p10;
create pitr p10 for account range 1 'd';
create pitr p11 for account range 10 'h';
drop pitr p10;


-- abnormal test: nonsys create duplicate pitr for sys (object: database)
drop database if exists test;
create database test;
drop pitr if exists p10;
create pitr p10 for database test range 10 'y';
create pitr p11 for database test range 11 'd';
drop pitr p10;
drop database test;


-- abnormal test: nonsys create duplicate pitr for sys (object: table01)
drop database if exists test;
create database test;
use test;
create table t1(col int, col2 decimal);
insert into t1 values(1,1);
drop pitr if exists p10;
create pitr p10 for table test t1 range 10 'y';
create pitr p11 for table test t1 range 11 'd';
drop pitr p10;
drop database test;
-- @session


-- authority: sys creates pitr, verify sys only can view and manage their own pitr
-- @session:id=1&user=acc01:test_account&password=111
drop pitr if exists p11;
create pitr p11 for account range 1 'd';
-- @session
drop pitr if exists p12;
create pitr p12 for account range 1 'd';
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10
select `pitr_id`, `pitr_name`, `create_account`, `create_time`, `modified_time`, `level`, `account_id`, `account_name`, `database_name`, `table_name`, `obj_id`, `pitr_length`, `pitr_unit` from mo_catalog.mo_pitr Where pitr_name != 'sys_mo_catalog_pitr';
alter pitr p11 range 11 'mo';
drop pitr p10;
-- @session:id=1&user=acc01:test_account&password=111
drop pitr p11;
-- @session


-- authority: nonsys creates pitr, verify nonsys only can view and manage own pitr
-- @session:id=1&user=acc01:test_account&password=111
drop pitr if exists p11;
create pitr p11 for account range 1 'd';
-- @session
drop pitr if exists p12;
create pitr p12 for account range 1 'd';
-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:1,2
show pitr;
alter pitr p12 range 10 'mo';
drop pitr p11;
-- @session
drop pitr p12;


-- user can not create/alter/drop pitr
-- @session:id=1&user=acc01:test_account&password=111
drop user if exists user01;
create user user01 identified by '111';
-- @session
-- @session:id=2&user=acc01:user01&password=111
create pitr p20 for account range 10 'd';
-- @session
-- @session:id=1&user=acc01:test_account&password=111
drop user user01;
-- @session


-- nonsys creates pitr for sys
-- @session:id=1&user=acc01:test_account&password=111
drop pitr if exists p20;
create pitr for account sys range 1 'y';
-- @session

drop account acc01;