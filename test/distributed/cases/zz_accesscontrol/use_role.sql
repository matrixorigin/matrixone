--env prepare statement
drop role if exists use_role_1,use_role_2,use_role_3,use_role_4,use_role_5;
drop user if exists use_user_1,use_user_2;
drop database if exists use_db_1;
create role use_role_1,use_role_2,use_role_3,use_role_4,use_role_5;
create database use_db_1;

--default指定主要角色，切换角色set secondary role all/none,use role来回切换，set role public
create user use_user_1 identified by '123456' default role use_role_1;
grant select ,insert ,update on table *.* to use_role_1;
grant all on database * to use_role_2;
grant use_role_2 to use_user_1;
-- @session:id=2&user=sys:use_user_1&password=123456
create table use_db_1.use_table_1(a int,b varchar(20),c double );
set secondary role all;
create table use_db_1.use_table_1(a int,b varchar(20),c double );
select * from use_db_1.use_table_1;
insert into use_db_1.use_table_1 values(34,'kelly',90.3);
set role use_role_2;
create table use_db_1.use_table_2(a int,b varchar(20),c double );
insert into use_db_1.use_table_2 values(34,'kelly',90.3);
select * from use_db_1.use_table_2;
set secondary role none;
create table use_db_1.use_table_3(a int,b varchar(20),c double );
insert into use_db_1.use_table_3 values(34,'kelly',90.3);
select * from use_db_1.use_table_3;
drop table use_db_1.use_table_3;
set role use_role_1;
create table use_db_1.use_table_4(a int,b varchar(20),c double );
insert into use_db_1.use_table_2 values(10,'yellow',99.99);
select * from use_db_1.use_table_2;
drop table use_db_1.use_table_2;
set role public;
create table use_db_1.use_table_4(a int,b varchar(20),c double );
insert into use_db_1.use_table_2 values(10,'yellow',99.99);
select * from use_db_1.use_table_2;
drop table use_db_1.use_table_2;
set secondary role all;
create table use_db_1.use_table_5(a int,b varchar(20),c double );
insert into use_db_1.use_table_5 values(10,'yellow',99.99);
select * from use_db_1.use_table_5;
drop table use_db_1.use_table_5;
set role moadmin;
-- @session

--缺省default指定主要角色，grant/alter指定主要角色，切换角色set secondary role all/none,use role来回切换，切换角色/不存在角色，set 用户没有被授权的role
create user use_user_2 identified by '123456';
grant create user, drop user, alter user, create role, drop role, create database,drop database,show databases on account *  to use_role_3;
grant all on table *.* to use_role_4;
grant create table,drop table on database * to use_role_5;
grant use_role_3,use_role_4,use_role_5 to use_user_2;
-- @session:id=2&user=sys:use_user_2&password=123456
set role use_not_exists;
set role use_role_3;
create role use_role_test;
set role use_role_test;
drop role  use_role_test;
set secondary role all;
create table use_db_1.use_table_6(a int,b varchar(20),c double);
insert into use_db_1.use_table_6 values(10,'yellow',99.99);
create database use_db_test;
drop database use_db_test;
set secondary role none;
insert into use_db_1.use_table_6 values (10, 'yellow', 99.99);
drop table use_db_1.use_table_6;
create role if not exists use_role_test;
drop role use_role_test;
set role use_role_5;
drop table use_db_1.use_table_6;
create database if not exists use_db_test;
-- @session

drop role if exists use_role_1,use_role_2,use_role_3,use_role_4,use_role_5;
drop user if exists use_user_1,use_user_2;
drop database if exists use_db_1;
