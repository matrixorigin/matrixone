--env prepare statement
drop user if exists user1,user2,user3,user4,user5,user11,user12,testuser,user_grant_1,user_grant_3,user_grant_4,user_grant_5,user_grant_6,user_grant_7,user_grant_8,user_grant_9,user_grant_10,user_prepare_01;
drop role if exists u_role,test_role,grant_role_1,role_sys_priv,role_account_priv_2,role_account_priv_3,role_account_priv_4,role_account_priv_5,role_account_priv_6,role_account_priv_7,role_account_priv_8,role_account_priv_9,role_account_priv_10,role_prepare_1;
drop database if exists grant_db;
drop database if exists testdb;
drop database if exists testdb4;
drop database if exists testdb5;
drop database if exists grant_db4;
drop database if exists grant_db5;
drop account if exists grant_account01;
drop table if exists table_4;
drop table if exists grant_table_10;
drop table if exists grant_table_30;
create account grant_account01 admin_name='admin' identified by '123456';
create database grant_db;

--覆盖语法语义:部分授权，all，allowership，with grant option
create role test_role;
create database testdb;
create user testuser IDENTIFIED BY '123456';
grant select,insert,update on table testdb.* to test_role with grant option;
select privilege_name,obj_type,privilege_level,with_grant_option from mo_catalog.mo_role_privs where role_name='test_role';
grant all on account * to test_role;
select privilege_name,obj_type,privilege_level,with_grant_option from mo_catalog.mo_role_privs where role_name='test_role';
grant OWNERSHIP on database *.* to test_role;
grant OWNERSHIP on table *.* to test_role;
select privilege_name,obj_type,privilege_level,with_grant_option from mo_catalog.mo_role_privs where role_name='test_role';

--grant privs异常测试：object_type和priv_level不对应，语法错误：缺省object_type，role不存在，权限直接赋予用户,grant相同role
grant select,insert,update on testdb.* to test_role;
grant select,insert,update on account * to 'test_role';
grant show tables,create,drop,alter on  testdb.* to 'test_role';
grant show tables,create,drop,alter on  table testdb.* to 'test_role';
grant select,insert,create database on table testdb.* to test_role;
grant select,insert,update on table to 'test_role';
grant select,insert,update on table testdb.* to 'trole';
grant select,insert,create database on account * to test_role;
grant select,insert,create database on account *.* to testuser;

--grant role异常测试：role不存在,grant相同role，重新授权给moadmin
grant role_not_exists to dump;
grant moadmin to user_not_exists;
create role grant_role_1;
grant  create account, drop account, alter account on *  to 'grant_role_1' with grant option;
grant grant_role_1 to grant_role_1;
grant show tables,create table,drop table on database * to moadmin;

-- sys下 role赋予account privs
create user 'user_grant_1' identified by '123456';
create role 'role_sys_priv';
grant  create account, drop account, alter account on account *  to role_sys_priv;
grant role_sys_priv to user_grant_1;
-- @session:id=2&user=sys:user_grant_1:role_sys_priv&password=123456
create account account01 admin_name='admin' identified by '123456';
drop account account01 ;
-- @session

-- priv_type ：account level 覆盖所有权限，验证所有授权生效;object_type: account缺省/不缺省，priv_level：*
-- @session:id=3&user=grant_account01:admin&password=123456
create user if not exists user_grant_2 identified by '123456';
create role if not exists 'role_account_priv_1';
grant  create user, drop user, alter user, create role, drop role, create database,drop database,show databases,connect,manage grants on account *  to role_account_priv_1 with grant option;
grant select on table *.* to role_account_priv_1;
grant role_account_priv_1 to user_grant_2;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_account_priv_1';
-- @session
-- @session:id=4&user=grant_account01:user_grant_2:role_account_priv_1&password=123456
create user user_test_2 identified by '123456';
select user_name,authentication_string from mo_catalog.mo_user where user_name='user_test_2';
create role if not exists role_test_01;
select role_name from mo_catalog.mo_role where role_name='role_test_01';
grant create user, drop user on account * to role_test_01;
grant insert,select on table *.* to role_test_01;
grant role_test_01 to user_test_2;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_test_01';
drop role role_test_01;
select role_name from mo_catalog.mo_role where role_name='role_test_01';
drop user user_test_2;
select user_name,authentication_string from mo_catalog.mo_user where user_name='user_test_2';
create database db_test_01;
use db_test_01;
drop database db_test_01;
-- @session

--priv_type ：account level 覆盖all权限,ownership权限0.6不支持;object_type: account，priv_level：*
create user 'user_grant_3' identified by '123456';
create role 'role_account_priv_2';
grant  all on account *  to 'role_account_priv_2';
grant role_account_priv_2 to user_grant_3;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_account_priv_2' with grant option ;
-- @session:id=5&user=sys:user_grant_3:role_account_priv_2&password=123456
create user 'user_test_3' identified by '123456';
create role if not exists role_test_02;
grant all on account * to role_test_02;
grant role_test_02 to user_test_3;
drop role role_test_02;
create database db_test_01;
use db_test_01;
drop database db_test_01;
drop user user_test_3;
-- @session

--priv_type ：database level 覆盖所有权限;object_type: database；priv_level：*
create user 'user_grant_4' identified by '123456';
create role 'role_account_priv_3';
grant show tables,create table ,drop table,alter table on database grant_db to role_account_priv_3;
grant connect on account * to role_account_priv_3;
grant role_account_priv_3 to user_grant_4;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_account_priv_3';
-- @session:id=6&user=sys:user_grant_4:role_account_priv_3&password=123456
use grant_db;
show tables;
create table grant_table_03 (id int,name varchar(50),num double)PARTITION BY KEY(id) PARTITIONS 4;
-- @bvt:issue#8320
show create table grant_table_03;
-- @bvt:issue
create view grant_v_1 as select * from grant_table_03;
drop table grant_table_03;
drop view  grant_v_1;
-- @session

--priv_type ：database level all权限;object_type: database；priv_level：*
create user 'user_grant_5' identified by '123456';
create role 'role_account_priv_4';
create database grant_db4;
create database grant_db5;
grant all on database grant_db4 to role_account_priv_4;
grant connect on account * to role_account_priv_4;
grant role_account_priv_4 to user_grant_5;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_account_priv_4';
-- @session:id=7&user=sys:user_grant_5:role_account_priv_4&password=123456
use grant_db4;
show tables;
create table grant_table_04 (id int,name varchar(50),num double)PARTITION BY KEY(id) PARTITIONS 4;
-- @bvt:issue#8320
show create table grant_table_04;
-- @bvt:issue
create view grant_v_2 as select * from grant_table_04;
drop table grant_table_04;
drop view grant_v_2;
use grant_db5;
show tables;
create table grant_table_04 (id int,name varchar(50),num double)PARTITION BY KEY(id) PARTITIONS 4;
-- @session

--priv_type ：database level ownership权限;object_type: database；priv_level：*，*.*,db_name(bug)
create user 'user_grant_6' identified by '123456';
create role 'role_account_priv_5',role_account_priv_6;
grant ownership on database * to role_account_priv_5;
grant create role,connect on account * to role_account_priv_5;
grant role_account_priv_5 to user_grant_6;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_account_priv_5';
-- @session:id=8&user=sys:user_grant_6:role_account_priv_5&password=123456
use grant_db;
show tables;
create table grant_table_05 (id int,name varchar(50),num double)PARTITION BY KEY(id) PARTITIONS 4;
create role 'role_account_priv_10';
grant create table,drop table on database * to role_account_priv_10;
-- @session
drop role role_account_priv_6;

--object_type: table level；priv_type ：select ,insert ,update权限;priv_level：*.*
create database testdb4;
create database testdb5;
create table testdb4.table_1(id int,name varchar(50),num double);
insert into testdb4.table_1 values (1,'banana',83.98),(2,'apple',0.003);
create table testdb5.table_2(id int,name varchar(50),num double);
insert into testdb5.table_2 values (3,'pear',3.8),(4,'orange',5.03);
create table testdb5.table_3(id int,name varchar(50),num double);
insert into testdb5.table_3 values (5,'aaa',3.8),(6,'bbb',5.03);
create table table_4(id int,name varchar(50),num double);
insert into table_4 values (7,'ccc',1.8),(8,'ddd',5.3);
create user 'user_grant_7' identified by '123456';
create role 'role_account_priv_6';
grant select ,insert ,update on table *.* to role_account_priv_6;
grant role_account_priv_6 to user_grant_7;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_account_priv_6';
-- @session:id=9&user=sys:user_grant_7:role_account_priv_6&password=123456
select * from testdb5.table_2;
insert into testdb5.table_2 values (9,'',8.00);
select * from testdb5.table_2;
update testdb4.table_1 set name='uuu' where id=2;
select * from testdb4.table_1;
delete from testdb4.table_1;
-- @session

--object_type: table level；priv_type ：truncate ,delete ,references ,index权限;priv_level：daname.*
create user 'user_grant_8' identified by '123456';
create role 'role_account_priv_7';
grant truncate ,delete ,reference ,index on table testdb4.* to role_account_priv_7;
grant role_account_priv_7 to user_grant_8;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_account_priv_7';
-- @session:id=10&user=sys:user_grant_8:role_account_priv_7&password=123456
--truncate table testdb4.table_1;
delete from testdb4.table_1;
select * from testdb4.table_1;
--create max index index_1 on testdb4.table_1(id);
--drop index index_1;
truncate table testdb5.table_2;
-- @session

--object_type: table level；priv_type ：ownership;priv_level：db_name.tblname
create user 'user_grant_9' identified by '123456';
create role 'role_account_priv_8';
grant ownership on table testdb5.table_2 to role_account_priv_8;
grant role_account_priv_8 to user_grant_9;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_account_priv_8';
-- @session:id=11&user=sys:user_grant_9:role_account_priv_8&password=123456
select * from testdb4.table_1;
delete from testdb4.table_1;
insert into testdb5.table_2 values(20,'yeah',10.20);
update testdb5.table_2 set name='bread' where id=20;
select * from testdb5.table_2;
-- @session

--object_type: table level；priv_type ：all;priv_level：daname.*
create user 'user_grant_10' identified by '123456';
create role 'role_account_priv_9';
create table grant_table_10(a int);
grant all on table testdb4.* to role_account_priv_9;
grant role_account_priv_9 to user_grant_10;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name='role_account_priv_9';
-- @session:id=12&user=sys:user_grant_10:role_account_priv_9&password=123456
insert into testdb4.table_1 values (10,'ccc',1.8),(11,'ddd',5.3);
select * from testdb4.table_1;
update testdb4.table_1 set name='oppo' where id=10;
delete from testdb4.table_1;
select * from testdb4.table_1;
delete from testdb5.table_2;
show tables;
create database ttt;
create account `test@123456` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
drop table testdb4.table_1;
-- @session

--多个权限授权给多个role
drop role if exists r1,r2,r3,r4,r5,r6,r7,r8,r9,r10;
create role r1,r2,r3,r4,r5,r6,r7,r8,r9,r10;
grant select,insert ,update on table *.* to r1,r2,r3,r4,r5;
select role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_role_privs where role_name in ('r1','r2','r3','r4','r5');

--一个权限授权给多个role,异常情况：权限存在不合法，role不存在情况
grant create table on database *.* to r1,r2,r3,r4,r5;
select role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_role_privs where role_name in ('r1','r2','r3','r4','r5') and obj_type="database";
grant create table on database *.* to r1,r2,r15,r4,r5;
grant select on database *.* to r1,r2,r3,r4,r5;

--多个role授权给多个用户
create user user1 identified by '12345678',user2 identified by '12345678',user3 identified by '12345678',user4 identified by '12345678',user5 identified by '12345678';
grant r1,r2,r3,r4,r5 to user1,user2,user3,user4,user5;
select count(*) from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name in ('r1','r2','r3','r4','r5');
-- @session:id=13&user=sys:user1:r1&password=12345678
create table grant_table_10(a int);
-- @session

--一个role授权给多个用户
grant create role on account * to r5;
grant r5 to user1,user2,user3,user4,user5;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name in ('r5');
-- @session:id=14&user=sys:user3:r5&password=12345678
create role test_role;
select count(*) from mo_catalog.mo_role where role_name='test_role';
-- @session

--多个role授权给多个role
create user user11 identified by '12345678';
grant select ,insert ,update on table *.* to r1,r2 with grant option;
grant r1,r2 to r6,r7;
select mr.role_name,mp.role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_role_grant mg,mo_catalog.mo_role mr ,mo_catalog.mo_role_privs mp where  mg.grantee_id=mr.role_id and mg.granted_id = mp.role_id and mr.role_name in ('r6','r7');

--一个role授权给多个role
create user user12 identified by '12345678';
grant r2 to r8,r9,r10;
select mr.role_name,mp.role_name,obj_type,privilege_name,privilege_level from mo_role_grant mg,mo_role mr ,mo_role_privs mp where  mg.grantee_id=mr.role_id and mg.granted_id = mp.role_id and mr.role_name in ('r8','r9','10');

--权限重复授权给role，role重复授权user，role重复授权给role
grant select ,insert ,update on table *.* to r1,r2 with grant option;
grant select ,insert ,update on table *.* to r1,r2 with grant option;
select role_name,obj_type,privilege_name,privilege_level,with_grant_option from mo_catalog.mo_role_privs where role_name in ('r1','r2');
grant r1,r2 to user1,user2;
grant r1,r2 to user1,user2;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and user_name in ('user1','user2') and role_name in ('r1','r2');
grant r1,r2 to r6,r7;
grant r1,r2 to r6,r7;
select mr.role_name,mp.role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_role_grant mg,mo_catalog.mo_role mr ,mo_catalog.mo_role_privs mp where  mg.grantee_id=mr.role_id and mg.granted_id = mp.role_id and mr.role_name in ('r6','r7');

drop user if exists user1,user2,user3,user4,user5,user11,user12,testuser,user_grant_1,user_grant_3,user_grant_4,user_grant_5,user_grant_6,user_grant_7,user_grant_8,user_grant_9,user_grant_10,user_prepare_01;
drop role if exists u_role,test_role,grant_role_1,role_sys_priv,role_account_priv_2,role_account_priv_3,role_account_priv_4,role_account_priv_5,role_account_priv_6,role_account_priv_7,role_account_priv_8,role_account_priv_9,role_account_priv_10,role_prepare_1;
drop database if exists grant_db;
drop database if exists testdb;
drop database if exists testdb4;
drop database if exists testdb5;
drop database if exists grant_db4;
drop database if exists grant_db5;
drop account if exists grant_account01;
drop table if exists table_4;
drop table if exists grant_table_10;
drop table if exists grant_table_30;
drop role if exists r1,r2,r3,r4,r5,r6,r7,r8,r9,r10;