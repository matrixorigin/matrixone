
create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=1&user=default_1:admin&password=111111
create role role1;
grant create database on account * to role1;
create user user1 identified by '123456' default role role1;
create role role2;
grant create database on account * to role2;
create user user2 identified by '123456' default role role2;
-- @session
-- @session:id=2&user=default_1:user1:role1&password=123456
create database db1;
alter database db1 set mysql_compatibility_mode = '0.7.0';
-- @session
-- @session:id=3&user=default_1:user2:role2&password=123456
alter database db1 set mysql_compatibility_mode = '0.8.0';
-- @session
drop account default_1;