drop database if exists db1;
create database db1;
-- table b
create table db1.b(b int);
insert into db1.b values (0),(1),(2),(3);

drop account if exists account_test;
create account account_test admin_name = 'root' identified by '111' open comment 'account_test';

use mo_catalog;
drop table if exists a;

--------------------
-- one attribute
--------------------

-- cluster table a
create cluster table a(a int);

-- insert into cluster table
insert into a values(0,0),(1,0),(2,0),(3,0);
insert into a values(0,1),(1,1),(2,1),(3,1);
update a set account_id=(select account_id from mo_account where account_name="account_test") where account_id=1;
select a from a;

-- check it in the non-sys account
-- @session:id=2&user=account_test:root&password=111
use mo_catalog;
select * from a;
-- @session

-- delete data from a
delete from a;

-- check it in the non-sys account
-- @session:id=2&user=account_test:root&password=111
use mo_catalog;
select * from a;
-- @session

-- insert into cluster table
insert into a select b,0 from db1.b;
insert into a select b,1 from db1.b;
update a set account_id=(select account_id from mo_account where account_name="account_test") where account_id=1;
select a from a;

-- check it in the non-sys account
-- @session:id=2&user=account_test:root&password=111
use mo_catalog;
select * from a;
-- @session

delete from a;

-- load into cluster table
load data infile '$resources/load_data/cluster_table1.csv' into table a;
update a set account_id=(select account_id from mo_account where account_name="account_test") where account_id=1;
select a from a;

-- check it in the non-sys account
-- @session:id=2&user=account_test:root&password=111
use mo_catalog;
select * from a;
-- @session

delete from a;

-- check it in the non-sys account
-- @session:id=2&user=account_test:root&password=111
use mo_catalog;
select * from a;
-- @session

delete from a;
truncate table a;

-- non-sys account operate the cluster table
-- @session:id=2&user=account_test:root&password=111
use mo_catalog;
delete from a;
drop table a;
truncate table a;
-- @session

-- drop account
drop account if exists account_test;

select a from a;
drop table if exists a;
drop account if exists account_test;
drop database if exists db1;

use mo_catalog;
CREATE CLUSTER TABLE `mo_instance` (`id` varchar(128) NOT NULL,`name` VARCHAR(255) NOT NULL,`account_name` varchar(128) NOT NULL,`provider` longtext NOT NULL,`provider_id` longtext,`region` longtext NOT NULL,`plan_type` longtext NOT NULL,`version` longtext,`status` longtext,`quota` longtext,`network_policy` longtext,`created_by` longtext,`created_at` datetime(3) NULL,PRIMARY KEY (`id`, `account_id`),UNIQUE INDEX `uniq_acc` (`account_name`));
desc mo_catalog.mo_instance;
create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';
-- @session:id=3&user=acc_idx:root&password=123456
desc mo_catalog.mo_instance;
-- @session
drop table mo_instance;
drop account if exists acc_idx;

use mo_catalog;
CREATE CLUSTER TABLE `mo_instance` (`id` varchar(128) NOT NULL,`name` VARCHAR(255) NOT NULL,`account_name` varchar(128) NOT NULL,`provider` longtext NOT NULL,`provider_id` longtext,`region` longtext NOT NULL,`plan_type` longtext NOT NULL,`version` longtext,`status` longtext,`quota` longtext,`network_policy` longtext,`created_by` longtext,`created_at` datetime(3) NULL,PRIMARY KEY (`id`, `account_id`),UNIQUE INDEX `uniq_acc` (`account_name`));
create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';
-- @session:id=4&user=acc_idx:root&password=123456
SELECT mo_table_rows('mo_catalog', 'mo_instance') as t;
SELECT mo_table_size('mo_catalog', 'mo_instance') as t;
-- @session
drop table mo_instance;
drop account if exists acc_idx;