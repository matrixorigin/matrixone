set global enable_privilege_cache = off;
-- @setup
--alter account auth_option: password
create account if not exists Abc ADMIN_NAME 'admin' IDENTIFIED BY '123456' comment 'comment test';
alter account abc admin_name='admin'  IDENTIFIED BY '1WERDFT3YG';
-- @session:id=1&user=abc:admin&password=1WERDFT3YG
select user_name,authentication_string from mo_catalog.mo_user;
create database testdb;
drop database testdb;
-- @session
alter account abc admin_name='admin'  IDENTIFIED BY 'yyyy_34lifel';
-- @session:id=1&user=abc:admin&password=yyyy_34lifel
select user_name,authentication_string from mo_catalog.mo_user;
-- @session
alter account abc admin_name='admin'  IDENTIFIED BY 'abcddddddfsfafaffsefsfsefljofiseosfjosissssssssssssssssssssssssssssssssssssssssssssssssssssssssssss';
-- @session:id=1&user=abc:admin&password=abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff` admin_name `abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffffdddddd
select user_name,authentication_string from mo_catalog.mo_user;
show databases;
-- @session
alter account abc admin_name='admin'  IDENTIFIED BY 'Ni7893';
-- @session:id=1&user=abc:admin&password=Ni7893
select user_name,authentication_string from mo_catalog.mo_user;
-- @session
alter account abc admin_name='admin'  IDENTIFIED BY '_1234';
-- @session:id=1&user=abc:admin%&password=_1234
select user_name,authentication_string from mo_catalog.mo_user;
-- @session
alter account abc admin_name='admin'  IDENTIFIED BY 'nnnn@12.fef';
-- @session:id=1&user=abc:admin&password=nnnn@12.fef
select user_name,authentication_string from mo_catalog.mo_user;
-- @session
alter account abc admin_name='admin'  IDENTIFIED BY '密码';
-- @session:id=1&user=abc:admin&password=密码
select user_name,authentication_string from mo_catalog.mo_user;
-- @session
alter account abc admin_name='admin'  IDENTIFIED BY '123 456';
-- @session:id=1&user=abc:admin&password=123 456
select user_name,authentication_string from mo_catalog.mo_user;
-- @session
alter account abc admin_name='admin'  IDENTIFIED BY 'test:aaa';
-- @session:id=1&user=abc:admin&password=test:aaa
select user_name,authentication_string from mo_catalog.mo_user;
-- @session
drop account abc;

--alter admin_name /password is null
create account if not exists test ADMIN_NAME '1WERDFT3YG' IDENTIFIED BY '123456';
alter account test admin_name='1WERDFT3YG'  IDENTIFIED BY '';

--alter not exist account ,alter if exists,admin_name not exist
alter account not_exist_account ADMIN_NAME 'admin' IDENTIFIED BY '123456';
alter account if exists not_exist_account ADMIN_NAME 'admin' IDENTIFIED BY '123456';
alter account test ADMIN_NAME 'testaaa' IDENTIFIED BY '123456';
alter account if exists test ADMIN_NAME 'testaaa' IDENTIFIED BY '123456';
drop account test;

--alter account same admin_name and password
create account if not exists test ADMIN_NAME 'admin' IDENTIFIED BY '123456' comment 'account comment';
alter account test admin_name='admin'  IDENTIFIED BY '123456';
-- @session:id=2&user=test:admin&password=123456
select user_name,authentication_string from mo_catalog.mo_user;
-- @session
drop account test;

--after create new user role ,alter account admin_name
create account if not exists alaccount ADMIN_NAME 'WERTY12ERT' IDENTIFIED BY '123456' comment 'account comment';
-- @session:id=3&user=alaccount:WERTY12ERT&password=123456
create user 'al_user_1' identified by '123456';
create role if not exists al_role;
grant all on account * to al_role;
grant al_role to al_user_1;
create database al_db;
-- @session
alter account alaccount ADMIN_NAME 'WERTY12ERT' IDENTIFIED BY 'abc@123';
-- @session:id=3&user=alaccount:WERTY12ERT&password=abc@123
select user_name,authentication_string from mo_catalog.mo_user where user_name='al_user_1';
select role_name,comments from mo_catalog.mo_role;
-- @session
-- @session:id=3&user=alaccount:WERTY12ERT&password=abc@123
show databases;
drop database al_db;
-- @session
drop account alaccount;

--alter account comment
create account if not exists testcomment ADMIN_NAME 'test_user' IDENTIFIED BY 'Asd1235' comment 'account comment';
alter account testcomment comment 'new account comment';
select account_name,comments from mo_catalog.mo_account where account_name='testcomment';
-- @session:id=8&user=testcomment:test_user&password=Asd1235
show databases;
-- @session
alter account testcomment comment '';
select account_name,comments from mo_catalog.mo_account where account_name='testcomment';
alter account testcomment comment 'abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff';
select account_name,comments from mo_catalog.mo_account where account_name='testcomment';
alter account testcomment comment '周三下午18：00';
select account_name,comments from mo_catalog.mo_account where account_name='testcomment';
alter account if exists testcomment comment '177634853$%^&*!@()';
select account_name,comments from mo_catalog.mo_account where account_name='testcomment';
alter account if exists testcomment1 comment '177634853$%^&*!@()';
alter account testcomment1 comment '177634853$%^&*!@()';
drop account testcomment;

--alter account status_option: OPEN|SUSPEND
create account if not exists testsuspend ADMIN_NAME 'admin' IDENTIFIED BY '123456' comment 'account comment';
-- @session:id=4&user=testsuspend:admin&password=123456
select user_name, authentication_string from mo_catalog.mo_user;
-- @session
alter account testsuspend suspend;
select account_name,status from mo_catalog.mo_account order by account_name;
alter account testsuspend OPEN;
select account_name,status from mo_catalog.mo_account order by account_name;
--suspend status alter ADMIN_NAME/comment,drop account
alter account testsuspend suspend;
select account_name,status from mo_catalog.mo_account order by account_name;
alter account testsuspend ADMIN_NAME 'admin' IDENTIFIED BY '1234567890';
alter account testsuspend comment 'aaaaaaa';
select account_name,status,comments from mo_catalog.mo_account where account_name='testsuspend';
drop account testsuspend;
select account_name,status from mo_catalog.mo_account where account_name='testsuspend';
alter account testsuspend open;

--reopen an already opened account or resuspend an already suspended account, there is no error
create account if not exists testsuspend ADMIN_NAME 'user01' IDENTIFIED BY 'fffff' comment 'account comment';
alter account testsuspend OPEN;
select account_name,status from mo_catalog.mo_account where account_name='testsuspend';
alter account testsuspend OPEN;
select account_name,status from mo_catalog.mo_account where account_name='testsuspend';
alter account testsuspend suspend;
select account_name,status from mo_catalog.mo_account where account_name='testsuspend';
alter account testsuspend suspend;
select account_name,status from mo_catalog.mo_account where account_name='testsuspend';
drop account testsuspend;
select account_name,status from mo_catalog.mo_account where account_name='testsuspend';

--Illegal syntax
create account if not exists test ADMIN_NAME 'adminuser' IDENTIFIED BY '123456' comment 'account comment';
alter account test admin_name='adminuser'  IDENTIFIED BY '123456' comment 'new comment ' ;
alter account test admin_name='adminuser'  IDENTIFIED BY '123456' suspend comment 'new comment';
alter account test suspend comment 'new comment';
alter account test admin_name='adminuser';
drop account test;

--Executed in a non moadmin role
drop user if exists al_user_2;
create user 'al_user_2' identified by '123456';
create role if not exists al_role2;
grant all on account * to al_role2;
grant al_role2 to al_user_2;
create account if not exists test ADMIN_NAME '123ERTYU' IDENTIFIED BY '123ERTYU' comment 'account comment';
-- @session:id=5&user=sys:al_user_2:al_role2&password=123456
alter account test admin_name='adminuser'  IDENTIFIED BY '123456';
alter account test comment 'ccccccc';
alter account test suspend;
-- @session
drop role if exists al_role2;
drop user if exists al_user_2;
drop account test;
set global enable_privilege_cache = on;