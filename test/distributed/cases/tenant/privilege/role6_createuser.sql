drop user if exists u1,u2,u3,u4,u5,u6,u7,u8,u9,u10;
create user u1 identified by '111', u2 identified by '111' default role public;
create user u3 identified by '111', u4 identified by '111';
create user u5 identified by '111', u6 identified by '111' default role moadmin;
create user u7 identified by '111', u8 identified by '111' default role rx;

drop role if exists r1;
create role r1;
create user u9 identified by '111', u10 identified by '111' default role r1;


drop user if exists u1,u2,u3,u4,u5,u6,u7,u8,u9,u10;
drop role r1;

create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';
-- @session:id=1&user=acc_idx:root&password=123456
create database db1;
use db1;
drop table if exists t;
create table t(
                  a int,
                  b int,
                  c int,
                  primary key(a)
);
show tables;
alter user 'root' identified by '111';
-- @session
-- @session:id=2&user=acc_idx:root&password=111
use db1;
show tables;
drop database db1;
-- @session
drop account acc_idx;