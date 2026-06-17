set global enable_privilege_cache = off;
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
-- @bvt:issue#10126
insert into db1.t1 values(2);
select * from db1.t1;
-- @bvt:issue
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
insert into db1.t2 values(1);
insert into db1.t2 values(2);
insert into db1.t2 values(3);
set role public;
create table db1.t3(a int);
set secondary role all;
create table db1.t3(a int);
-- @session
-- @session:id=7&user=default_1:user2:role1&password=123456
select * from db1.t2;
-- @session
-- @session:id=8&user=default_1:user2:role2&password=123456
grant all on table db1.t2 to role1;
-- @session
-- @session:id=9&user=default_1:user2:role1&password=123456
select * from db1.t2;
-- @session
drop account default_1;

create account ddl_orphan_owner ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=10&user=ddl_orphan_owner:admin&password=111111
create role owner_r;
grant connect,create database on account * to owner_r;
create user owner_u identified by '123456' default role owner_r;
create role dropper_r;
grant connect,drop database on account * to dropper_r;
create user dropper_u identified by '123456' default role dropper_r;
-- @session
-- @session:id=11&user=ddl_orphan_owner:owner_u:owner_r&password=123456
create database orphan_db;
create table orphan_db.orphan_t(a int);
-- @session
-- @session:id=10&user=ddl_orphan_owner:admin&password=111111
grant drop table on database orphan_db to dropper_r;
drop role owner_r;
-- @session
-- @session:id=12&user=ddl_orphan_owner:dropper_u:dropper_r&password=123456
drop table orphan_db.orphan_t;
drop database orphan_db;
-- @session
drop account ddl_orphan_owner;
set global enable_privilege_cache = on;
