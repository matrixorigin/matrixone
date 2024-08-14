drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
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

drop account acc01;