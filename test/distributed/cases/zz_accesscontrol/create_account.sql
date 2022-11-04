--env prepare statement
drop account if exists `test@123456`;
drop account if exists testaccount;
drop account if exists 123_acc;
drop account if exists _acc;
drop account if exists a12;
drop account if exists _acc1;
drop account if exists FaSt;
drop account if exists `123`;
drop account if exists a123;
drop account if exists `abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff`;
drop account if exists abc;
drop account if exists abcd;
drop account if exists cm1;
drop account if exists cm2;
drop account if exists cm3;
drop account if exists accout_Xingming_insert;
drop account if exists `ab.cd`;
drop account if exists `test/123`;
drop account if exists `test%`;
drop account if exists `非常`;
drop account if exists user_strip_01;
drop account if exists account_1;
drop account if exists aaa;
drop account if exists account;
--2.account name字符，数字，特殊字符混合 ,admin_name/auth_string数字英文中文特殊符号组合，特殊字符打头，大小写不敏感，auth_string大小写敏感,覆盖"",'',``
create account `test@123456` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
select account_name from mo_catalog.mo_account where account_name='test@123456';
create account testaccount ADMIN_NAME 'admin@123' IDENTIFIED BY 'yyyy_1234@126.com';
select account_name from mo_catalog.mo_account where account_name='testaccount';
create account 123_acc ADMIN_NAME '8888' IDENTIFIED BY 'ffffff';
select account_name from mo_catalog.mo_account where account_name='123_acc';
create account _acc ADMIN_NAME 'AbcDef' IDENTIFIED BY 'NIU_2345';
select account_name from mo_catalog.mo_account where account_name='_acc';
create account a12 ADMIN_NAME 'a12' IDENTIFIED BY 'aaaaa';
select account_name from mo_catalog.mo_account where account_name='a12';
create account _acc1 ADMIN_NAME '_AbcDef' IDENTIFIED BY '_2345';
select account_name from mo_catalog.mo_account where account_name='_acc1';
create account a123 ADMIN_NAME 'a12' IDENTIFIED BY 'aaaaa';
select account_name from mo_catalog.mo_account where account_name='a12';
create account FaSt ADMIN_NAME '账号' IDENTIFIED BY '账号密码';
select account_name from mo_catalog.mo_account where account_name='fast';
create account `ab.cd` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
select account_name from mo_catalog.mo_account where account_name='ab.cd';
create account `test/123` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
select account_name from mo_catalog.mo_account where account_name='test/123';
create account `test%` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
select account_name from mo_catalog.mo_account where account_name='test%';
create account `123` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
select account_name from mo_catalog.mo_account where account_name='123';
create account `非常` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
select account_name from mo_catalog.mo_account where account_name='非常';

--3.account name,admin_name,auth_string长度180字符, "",'',``前后空格清除
create account `abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff` admin_name `abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffffdddddd` IDENTIFIED BY '1111111111111111111111111111111111111111111111111111111';
select account_name from mo_catalog.mo_account where account_name='abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff';
create account ` user_strip_01 ` ADMIN_NAME " admin " IDENTIFIED BY " 123456 ";
select account_name from mo_catalog.mo_account where account_name='user_strip_01';

--4.account name账户已存在,大小写敏感，if exist存在不存在,comment关键字中文字符英文长度
create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
select account_name from mo_catalog.mo_account where account_name='abc';
create account ABC ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account Abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account IF NOT EXISTS Abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account IF NOT EXISTS Abcd ADMIN_NAME 'admin' IDENTIFIED BY '123456' comment '这是comment备注/123456';
select account_name,comments from mo_catalog.mo_account where account_name='abcd';
create account IF NOT EXISTS cm1 ADMIN_NAME 'admin' IDENTIFIED BY '123456' comment 'this is test comment , please check';
select account_name,comments from mo_catalog.mo_account where account_name='cm1';
create account cm2 ADMIN_NAME 'admin' IDENTIFIED BY '123456' comment 'abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff';
select account_name,comments from mo_catalog.mo_account where account_name='cm2';
create account cm3 ADMIN_NAME 'admin' IDENTIFIED BY '123456' comment '';
select account_name,comments from mo_catalog.mo_account where account_name='cm3';

--5.异常测试：语法不合法
create accout a1 ADMIN_NAME ' admin' IDENTIFIED BY '123456';
create account a1;
create accout a1 ADMIN_NAME ' admin';
create accout a1 IDENTIFIED BY '123456';
create accout a1 comment'aaaa';
create account 123 ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account 非常 ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account "acc1" ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account 'acc1' ADMIN_NAME 'admin' IDENTIFIED BY '123456';

--6.异常值:account name/admin_name/auth_string为空值，冒号，关键字
create account bbb ADMIN_NAME '' IDENTIFIED BY '123456';
create account `` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account aaa ADMIN_NAME 'admin' IDENTIFIED BY '';
create account test:account ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account 'test:account' ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account "test:account" ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account `test:account` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
create account default ADMIN_NAME 'root' IDENTIFIED BY '123456';
create account account ADMIN_NAME 'root' IDENTIFIED BY '123456';
select count(*) from mo_catalog.mo_account where account_name in ('test:account','default','account');
-- @bvt:issue
-- 7.account初始accountamdin权限验证：查询系统表;创建db，user，table;sys租户下root看不到account下的系统表数据
create account account_1 admin_name='admin' identified by '123456';
-- @session:id=2&user=account_1:admin:accountadmin&password=123456
show databases;
use mo_catalog;
show tables;
select user_name,authentication_string,owner from mo_user;
select role_name,obj_type,privilege_name,privilege_level from mo_role_privs;
create database account_1_db;
CREATE USER account_1_user IDENTIFIED BY '123456';
create table a(b int);
use account_1_db;
create table a(b int);
-- @session
use mo_catalog;
select user_name,authentication_string,owner from mo_user where user_name ='account_1_user';

--8.CREATE-DROP-CREATE,CREATE-ALTER-CREATE场景
create account accout_Xingming_insert ADMIN_NAME 'root' IDENTIFIED BY '123456789';
select account_name from mo_catalog.mo_account where account_name='accout_xingming_insert';
drop account accout_Xingming_insert;
select account_name from mo_catalog.mo_account where account_name='accout_xingming_insert';
create account if not exists accout_Xingming_insert ADMIN_NAME 'root' IDENTIFIED BY '123456789';
select account_name from mo_catalog.mo_account where account_name='accout_xingming_insert';

drop account if exists `test@123456`;
drop account if exists testaccount;
drop account if exists 123_acc;
drop account if exists _acc;
drop account if exists a12;
drop account if exists _acc1;
drop account if exists FaSt;
drop account if exists `123`;
drop account if exists a123;
drop account if exists `abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff`;
drop account if exists abc;
drop account if exists abcd;
drop account if exists cm1;
drop account if exists cm2;
drop account if exists cm3;
drop account if exists accout_Xingming_insert;
drop account if exists `ab.cd`;
drop account if exists `test/123`;
drop account if exists `test%`;
drop account if exists `非常`;
drop account if exists user_strip_01;
drop account if exists account_1;
drop account if exists aaa;
drop account if exists account;
