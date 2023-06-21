set global enable_privilege_cache = off;
--env prepare statement
drop database if exists revoke_db_01;
drop role if exists revoke_role_1,revoke_role_2,revoke_role_3,revoke_role_4,revoke_role_5,revoke_role_6,revoke_role_7,revoke_role_8;
drop user if exists revoke_user_1,revoke_user_2,revoke_user_3,revoke_user_4,revoke_user_5,revoke_user_6,revoke_user_7;
create role if not exists revoke_role_1,revoke_role_2,revoke_role_3,revoke_role_4,revoke_role_5,revoke_role_6,revoke_role_7,revoke_role_8;
create user if not exists revoke_user_1 identified by '12345678',revoke_user_2 identified by '12345678',revoke_user_3 identified by '12345678',revoke_user_4 identified by '12345678',revoke_user_5 identified by '12345678',revoke_user_6 identified by '12345678',revoke_user_7 identified by '12345678';
create database revoke_db_01;

--revoke多个权限from一个role，覆盖部分/全部，异常情况：无该权限，role不存在
grant create user, drop user, alter user, create role, drop role, create database,drop database on account * to revoke_role_1 with grant option;
grant revoke_role_1 to revoke_user_1;
revoke create user, drop user, alter user on account * from revoke_role_1;
select role_name, privilege_name, privilege_level from mo_catalog.mo_role_privs where role_name='revoke_role_1';
-- @session:id=2&user=sys:revoke_user_1:revoke_role_1&password=12345678
create user re_test_user identified by '12345678';
drop user re_test_user;
create role revoke_role_9;
drop role revoke_role_9;
-- @session
revoke all on account * from revoke_role_3;
revoke create user, drop user, show tables on account * from revoke_role_1;
revoke create user, drop user, show tables on table *.* from revoke_role_1;
revoke create user, drop user on account * from re_not_exists;
revoke all on account * from revoke_role_1;

--revoke多个权限from多个role，覆盖回收部分/全部，异常情况：无该权限，role不存在,if exists
grant create table,drop table,alter table on database *.* to revoke_role_2,revoke_role_3 with grant option;
grant all on account * to revoke_role_2;
grant revoke_role_2 to revoke_user_2;
grant revoke_role_3 to revoke_user_3;
revoke drop table,create table on database *.* from revoke_role_2,revoke_role_3;
select role_name, privilege_name, privilege_level from mo_catalog.mo_role_privs where role_name in ('revoke_role_2','revoke_role_3');
-- @session:id=2&user=sys:revoke_user_2:revoke_role_2&password=12345678
create table revoke_db_01.revoke_table_1(a int,b varchar(20),c double);
drop table revoke_db_01.revoke_table_1;
create database revoke_db_02;
drop database revoke_db_02;
-- @session
-- @session:id=2&user=sys:revoke_user_3:revoke_role_3&password=12345678
create table revoke_db_01.revoke_table_1(a int,b varchar(20),c double);
drop table revoke_db_01.revoke_table_1;
-- @session
revoke create table,select,insert on database * from revoke_role_2,revoke_role_3;
revoke if exists create table,select,insert on database * from revoke_role_2,revoke_role_3;
revoke all on account * from revoke_role_2,revoke_role_3;
revoke if exists all on account * from revoke_role_2,revoke_role_3;

--revoke一个权限from多个role
grant all on table *.* to revoke_role_4,revoke_role_5 with grant option;
grant create table,drop table,alter table on database *.* to revoke_role_5;
grant revoke_role_4 to revoke_user_4 with grant option;
grant revoke_role_5 to revoke_user_5;
revoke all on table *.* from revoke_role_4,revoke_role_5;
select role_name, privilege_name, privilege_level from mo_catalog.mo_role_privs where role_name in ('revoke_role_4','revoke_role_5');
-- @session:id=2&user=sys:revoke_user_4:revoke_role_4&password=12345678
select * from mo_catalog.mo_user;
-- @session
-- @session:id=2&user=sys:revoke_user_5:revoke_role_5&password=12345678
select * from mo_catalog.mo_user;
create table revoke_db_01.revoke_table_2(a int,b varchar(20),c double);
drop table revoke_db_01.revoke_table_2;
-- @session

--revoke ownership
grant ownership on database revoke_db_01 to revoke_role_6;
grant all on table *.* to revoke_role_6;
grant revoke_role_6 to revoke_user_6;
revoke ownership on database revoke_db_01 from revoke_role_6;
-- @session:id=2&user=sys:revoke_user_6:revoke_role_6&password=12345678
create table revoke_test_table_1(a int);
drop table revoke_test_table_1;
grant ownership on database revoke_db_01 to revoke_role_7;
select * from revoke_db_01.revoke_table_1;
-- @session

drop database if exists revoke_db_01;
drop role if exists revoke_role_1,revoke_role_2,revoke_role_3,revoke_role_4,revoke_role_5,revoke_role_6,revoke_role_7,revoke_role_8;
drop user if exists revoke_user_1,revoke_user_2,revoke_user_3,revoke_user_4,revoke_user_5,revoke_user_6,revoke_user_7;
set global enable_privilege_cache = on;