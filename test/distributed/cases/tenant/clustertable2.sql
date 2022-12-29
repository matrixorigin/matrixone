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
insert into a accounts(sys,account_test) values(0),(1),(2),(3);
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
insert into a accounts(sys,account_test) select b from db1.b;

select a from a;

-- check it in the non-sys account
-- @session:id=2&user=account_test:root&password=111
use mo_catalog;
select * from a;
-- @session

delete from a;

-- load into cluster table
load data infile '$resources/load_data/cluster_table1.csv' into table a accounts(sys,account_test) (a);
select a from a;

-- check it in the non-sys account
-- @session:id=2&user=account_test:root&password=111
use mo_catalog;
select * from a;
-- @session

delete from a;

-- insert into account_id
insert into a(account_id) values (0),(1),(2),(3);
insert into a(account_id) select b from db1.b;
load data infile '$resources/load_data/cluster_table1.csv' into table a (account_id);

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