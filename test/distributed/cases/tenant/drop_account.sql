drop account if exists account_test;
create account account_test admin_name = 'root' identified by '111' open comment 'account_test';
-- @ignore:2,6,7
show accounts;

-- @session:id=2&user=account_test:root&password=111
create database db1;
use db1;
create table t1 (a int primary key);

create database db2;
use db2;
create table t2 (a int primary key, b int, FOREIGN KEY (b) REFERENCES db1.t1(a));
-- @session

-- drop account with cross database foreign key
drop account account_test;
