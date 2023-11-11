set global enable_privilege_cache = off;
--env prepare statement
drop role if exists newrole, role_1234,12role,`role@hhhh123`,`role.123`,_newrole,role222;
drop role if exists role1,role2,role3,role4,role5,role6,role_7,user_role,u_role;
drop role if exists role_1,role_2,role_3,role_4,role_5,role_6,role_7,'中文','12345','default';
drop role if exists `abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff`;
drop user if exists role_user,user_role;
drop role if exists role_role1,role_role2,role_role3,role_role4,role_role5,role_role6,role_role7,role_role8,u_role;
drop database  if exists p_db;
--1.rolename支持字符，数字，特殊字符混合,中文,大小写不敏感,覆盖"",'',``,及去除头尾空格
create role newrole, role_1234,12role,`role@hhhh123`,`role.123`,_newrole,RoLe222;
create role NewRole;
select role_name from mo_catalog.mo_role where role_name in ('newrole', 'role_1234','12role','role@hhhh123','role.123','_newrole','role222');
create role "role_1",'role_2',`role_3`;
select role_name from mo_catalog.mo_role where role_name in ('role_1','role_2','role_3');
create role " role_4 ",' role_5 ',` role_6 `;
select role_name from mo_catalog.mo_role where role_name in ('role_4','role_5','role_6');
create role '中文','12345';
select role_name from mo_catalog.mo_role where role_name in ('中文','12345');
create role 'abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff';
select role_name from mo_catalog.mo_role where role_name in ('abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff');
create role 'default';
select role_name from mo_catalog.mo_role where role_name in ('default');

--2.异常测试:语法错误,rolename包含冒号，关键字，rolename和rolename同名,rolename为空
create if not exists role select;
create role a b c;
create role 'test:abc';
create role "test:abc";
create role `test:abc`;
create role default;
create user user_role identified by '12345678';
create role user_role;
create role '';

--3.rolename已存在/不存在，包含初始化moadmin，public,if not exist存在/不存在
create role moadmin,public;
create role if not exists moadmin,public;
select role_name from mo_catalog.mo_role where role_name in ('moadmin','public');
create role if not exists role_7;
select role_name from mo_catalog.mo_role where role_name = 'role_7';

--4.一次性创建多个role都不存在，部分存在，全部存在，名字非法，管理员角色，if not exist
use mo_catalog;
create role if not exists role_role1,role_role2,role_role3,role_role4,role_role5,role_role6;
select role_name from mo_role where role_name like 'role_role%' order by role_name;
create role role_role1,role_role2,role_role3,role_role7,role_role8;
create role if not exists role_role1,role_role2,role_role3,role_role7,role_role8;
select role_name from mo_role where role_name like 'role_role%' order by role_name;
create role role_role9,role_role:10,role_role11;
select role_name from mo_role where role_name like 'role_role%' order by role_name;

--6.覆盖CREATE-grant-DROP-CREATE,CREATE-grant-grant场景
create role if not exists role2;
create user role_user identified by '111111';
grant insert,select on table *.* to role2;
use mo_catalog;
select role_name,obj_type,privilege_name,privilege_level from mo_role_privs where role_name='role2';
grant role2 to role_user;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_user_grant,mo_user,mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role2';
drop role role2;
select count(*) from mo_role_privs where role_name='role2';
select count(*) from mo_user_grant,mo_user,mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role2';
create role role2;
grant insert,select on table *.* to role2;
select role_name,obj_type,privilege_name,privilege_level from mo_role_privs where role_name='role2';
create role if not exists role3;
grant create database on account * to role3;
select role_name,obj_type,privilege_name,privilege_level from mo_role_privs where role_name='role3';
grant role3 to role_user;
grant ownership on table *.* to role3;
grant role3 to role_user;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_user_grant,mo_user,mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role3';

--prepare,set,execute权限验证
drop user if exists user_prepare_01;
drop role if exists role_prepare_1;
create user user_prepare_01 identified by '123456';
create role role_prepare_1;
create database if not exists p_db;
grant create table ,drop table on database *.*  to role_prepare_1;
grant connect on account * to role_prepare_1;
grant insert,select on table *.* to role_prepare_1;
grant role_prepare_1 to user_prepare_01;
-- @session:id=17&user=sys:user_prepare_01:role_prepare_1&password=123456
use p_db;
prepare stmtt from 'drop table if exists  grant_table_30';
execute stmtt;
prepare stmt from 'create table grant_table_30(a int)';
execute stmt;
prepare stmt1 from 'insert into grant_table_30 values(?)';
set @a=55;
execute stmt1 using @a;
prepare stmt2 from 'select * from grant_table_30';
execute stmt2;
prepare stmt2 from 'update grant_table_30 set a=60';
execute stmt2;
prepare stmt2 from 'insert into grant_table_30 select 89';
execute stmt2;
prepare stmt3 from 'insert into grant_table_30 select * from grant_table_30';
execute stmt3;
prepare stmt4 from 'show databases';
execute stmt4;
prepare stmt5 from 'select "abc"';
execute stmt5;
-- @session

drop role if exists newrole, role_1234,12role,`role@hhhh123`,`role.123`,_newrole,role222;
drop role if exists role1,role2,role3,role4,role5,role6,role_7,user_role,u_role;
drop role if exists role_1,role_2,role_3,role_4,role_5,role_6,role_7,'中文','12345','default';
drop role if exists `abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff`;
drop user if exists role_user,user_role;
drop role if exists role_role1,role_role2,role_role3,role_role4,role_role5,role_role6,role_role7,role_role8,u_role;
drop database  if exists p_db;
set global enable_privilege_cache = on;
deallocate prepare stmt1;
deallocate prepare stmt2;
deallocate prepare stmt3;
deallocate prepare stmt4;
deallocate prepare stmt5;