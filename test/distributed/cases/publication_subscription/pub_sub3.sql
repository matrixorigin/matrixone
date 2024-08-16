drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';
-- publication name test
drop database if exists db01;
create database db01;
use db01;
drop table if exists table01;
create table table01 (col1 int unique key, col2 enum ('a','b','c'));
insert into table01 values(1,'a');
insert into table01 values(2, 'b');
drop table if exists table02;
create table table02 (col1 int primary key , col2 enum ('a','b','c'));
insert into table02 values(1,'a');
insert into table02 values(2, 'b');
drop table if exists table03;
create table table03(col1 int auto_increment, key key1(col1));
insert into table03 values (1);
insert into table03 values (2);
drop publication if exists `add`;
create publication `add` database db01 account all;
drop publication if exists account;
create publication account database db01 table table01,table02 account acc01;
drop publication if exists `$$$`;
create publication `$$$` database db01 account all;
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sub01;
create database sub01 from sys publication `add`;
drop database if exists sub02;
create database sub02 from sys publication account;
drop database if exists sub03;
create database sub03 from sys publication `$$$`;
-- @ignore:5,7
show subscriptions;
-- @session

drop publication `add`;
drop publication account;
drop publication `$$$`;
drop database db01;

-- @session:id=1&user=acc01:test_account&password=111
drop database sub01;
drop database sub02;
drop database sub03;
-- @session




-- test mo-database
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db02;
create database db02;
use db02;
drop table if exists t1;
create table t1 (a tinyint unsigned not null, primary key(a));
insert into t1 values (255), (0xFC), (254), (253);
drop table if exists t2;
create table t2 ( a tinyint not null default 1, tinyint8 tinyint primary key);
insert into t2 (tinyint8) values (-1),(127),(-128);
drop table if exists t3;
create table t3 ( a tinyint not null default 1, tinyint8 tinyint unsigned primary key);
insert into t3 (tinyint8) values (0),(255), (0xFE), (253);
drop publication if exists pub01;
create publication pub01 database db02 account acc02 comment 'publish to acc01';
drop publication if exists pub02;
create publication pub02 database db02 account acc02, acc03 comment 'publish to acc01 and acc03';
drop publication if exists pub03;
create publication pub03 database db02 table t1, t2 account acc02 comment 'publish to acc02';
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub02;
create database sub02 from acc01 publication pub01;
-- @session

-- @ignore:0,2,7
select * from mo_catalog.mo_subs;

-- @session:id=2&user=acc02:test_account&password=111
drop database sub02;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub01;
drop publication pub02;
drop publication pub03;
drop database db02;
-- @session

drop account acc01;
drop account acc02;
drop account acc03;