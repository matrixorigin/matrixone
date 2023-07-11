drop database if exists test;
create database test;
select `variable_value` from mo_catalog.mo_mysql_compatibility_mode where dat_name ="test";
alter database test set mysql_compatibility_mode = '8.0.30-MatrixOne-v0.7.0';
select `variable_value` from mo_catalog.mo_mysql_compatibility_mode where dat_name ="test";
drop database test;

drop database if exists test;
create database test;
use test;
select version();
alter database test set mysql_compatibility_mode = '8.0.30-MatrixOne-v0.7.0';
select version();
drop database test;
drop account if exists abc;
create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=abc:admin&password=123456
drop database if exists test;
drop database if exists test1;
create database test;
create database test1;
use test;
select version();
alter database test set mysql_compatibility_mode = '8.0.30-MatrixOne-v0.7.0';
select version();
use test1;
select version();
alter account config abc set mysql_compatibility_mode = '8.0.30-MatrixOne-v0.8.0';
select version();
drop database test;
drop database test1;
-- @session
drop account abc;

create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=3&user=default_1:admin&password=111111
create role role1;
grant create database on account * to role1;
create user user1 identified by '123456' default role role1;
create role role2;
grant create database on account * to role2;
create user user2 identified by '123456' default role role2;
-- @session
-- @session:id=4&user=default_1:user1:role1&password=123456
create database db1;
alter database db1 set mysql_compatibility_mode = '0.7.0';
-- @session
-- @session:id=5&user=default_1:user2:role2&password=123456
alter database db1 set mysql_compatibility_mode = '0.8.0';
-- @session
drop account default_1;