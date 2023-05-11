create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=1&user=default_1:admin&password=111111
create role role1;
grant create database on account * to role1;
create user user1 identified by '123456' default role role1;
-- @session
-- @session:id=2&user=default_1:user1:role1&password=123456
create database db1;
create table db1.t1(a int);
insert into db1.t1 values(1);
select * from db1.t1;
truncate table db1.t1;
insert into db1.t1 values(2);
select * from db1.t1;
-- @session
drop account default_1;

create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=3&user=default_1:admin&password=111111
create role role1;
grant create database on account * to role1;
create user user1 identified by '123456' default role role1;
-- @session
-- @session:id=4&user=default_1:user1:role1&password=123456
create database db1;
-- @session
-- @session:id=5&user=default_1:admin&password=111111
create role role2;
grant create table on database db1 to role2;
create user user2 identified by '123456';
grant role1,role2 to user2;
-- @session
-- @session:id=6&user=default_1:user2&password=123456
create table db1.t2(a int);
set role role2;
create table db1.t2(a int);
set role public;
create table db1.t3(a int);
set secondary role all;
create table db1.t3(a int);
-- @session
drop account default_1;