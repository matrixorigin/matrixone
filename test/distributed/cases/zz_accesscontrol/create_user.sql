set global enable_privilege_cache = off;
--env prepare statement
drop user if exists yellowcar,test_user,user123456,123user,`1234@fff`,`1234`,`user1234.aaaa`,`123user中文`,user_role,user123456,c_user_01,c_user_02,c_user_03,customername,customer,custom,finley,jason,`neil@192.168.1.10`;
drop user if exists test_user,test_user0,test_user1,test_user2,user1,user2,user3,user4,user5,user6,user7,user8,user9,user10,tester1,tester2,tester001,tester002,tester003,`daisy@192.168.1.10`,daisy;
drop user if exists user1,tester1,tester2,jason,finley,custom,customer,customname,c_user_01,c_user_02,c_user_03;
drop account if exists test5555;
drop role if exists low_level,mid_level,high_level,u_role;
drop table if exists testdb.aaa;
create role if not exists u_role;

--1.username,auth_string字符数字中文特殊字符组合,大小写不敏感,覆盖''，""，``；去除头尾空格
create user yellowcar identified by 'oldmaster';
select user_name,authentication_string from mo_catalog.mo_user where user_name='yellowcar';
create user  user123456 identified by 'TYUJI_123@126.cn' comment '这是创建用户测试aaaa11111';
select user_name,authentication_string from mo_catalog.mo_user where user_name='user123456';
create user  123user  identified by 'eeeeee' ;
select user_name, authentication_string from mo_catalog.mo_user where user_name='123user';
create user Test_user identified by '12345678';
create user Test_User identified by '12345678';
create user `1234@fff` identified by '#¥%……&' comment 'this is test@fefffff' ;
select user_name, authentication_string from mo_catalog.mo_user where user_name='1234@fff';
create user  `1234`  identified by '#¥%……&' comment 'this is test@fefffff' ;
select user_name, authentication_string from mo_catalog.mo_user where user_name='1234';
create user `user1234.aaaa`  identified by '#¥%……&' comment 'this is test@fefffff' ;
select user_name, authentication_string from mo_catalog.mo_user where user_name='user1234.aaaa';
create user  `123user中文`  identified by 'eee中文' ;
select user_name, authentication_string from mo_catalog.mo_user where user_name='123user中文';
create user  "test_user0" identified by '12345678';
create user  ` c_user_01 `  identified by ' 1111 ' ;
create user  " c_user_02 "  identified by '1111' ;
create user  ' c_user_03 '  identified by '1111' ;
select user_name, authentication_string from mo_catalog.mo_user where user_name in('c_user_01','c_user_02','c_user_03');

--2.支持一次性创建一个/多个用户都不存在，部分存在，全部存在，名字非法，if not exits
create user if not exists test_user1 identified by '12345678';
create user  test_user1 identified by '12345678';
create user if not exists test_user1 identified by '12345678';
create user user1 identified by '12345678',user2 identified by '12345678',user3 identified by '12345678',user4 identified by '12345678',user5 identified by '12345678',user6 identified by '12345678',user7 identified by '12345678',user8 identified by '12345678',user9 identified by '12345678',user10 identified by '12345678' default role u_role;
select count(*) from mo_catalog.mo_user where user_name like 'user%';
create user tester1 identified by '12345678',tester2 identified by '12345678',user3 identified by '12345678',user4 identified by '12345678';
select count(*) from mo_catalog.mo_user where user_name like 'tester%';
create user if not exists tester1 identified by '12345678',tester2 identified by '12345678',user3 identified by '12345678',user4 identified by '12345678';
select count(*) from mo_catalog.mo_user where user_name like 'tester%';
create user tester001 identified by '12345678',tester002  identified by '12345678',tester:003 identified by '12345678';
create user if not exists tester001 identified by '12345678',tester002  identified by '12345678',tester:003 identified by '12345678';
select count(*) from mo_catalog.mo_user where user_name like 'tester00%';

--4.user包含@hostname，表将user与hostname存入对应列
CREATE USER daisy@192.168.1.10 IDENTIFIED BY '123456';
select user_name, user_host from mo_catalog.mo_user where user_name='daisy';
CREATE USER 'neil@192.168.1.10' IDENTIFIED BY '123456';
select user_name, user_host from mo_catalog.mo_user where user_name='neil';
CREATE USER 'jason'@'192.168.1.10' IDENTIFIED BY '123456';
select user_name, user_host from mo_catalog.mo_user where user_name='jason';
create user 'finley'@'%.example.com' IDENTIFIED BY '123456';
select user_name, user_host from mo_catalog.mo_user where user_name='finley';
CREATE USER 'custom'@'localhost' IDENTIFIED BY '11111';
select user_name, user_host from mo_catalog.mo_user where user_name='custom';
CREATE USER 'customer'@'host47.example.com' IDENTIFIED BY '1111';
select user_name, user_host from mo_catalog.mo_user where user_name='customer';
CREATE USER 'customername'@'%' IDENTIFIED BY '1111';
select user_name, user_host from mo_catalog.mo_user where user_name='customername';
CREATE USER ''@'localhost' IDENTIFIED BY '1111';

--5.异常情况：语法语义错误,username包含冒号，default role不存在,username空，密码空
create user if not exists user1 ,user2 ,user3 identified by '12345678';
create user if not exists all identified by '111' ;
create user user:1 identified by '111' ;
create user 'user:1' identified by '111' ;
create user "user:1" identified by '111' ;
create user `user:1` identified by '111' ;
create user user_aaa identified by '12345678' default role aaa ;
create user '' identified by '111' ;
create user c_user_4 identified by '' ;

--6.CREATE-DROP-CREATE,CREATE-ALTER-CREATE场景
create user if not exists test_user2 identified by '12345678';
select user_name,authentication_string from mo_catalog.mo_user where user_name='test_user2';
drop user test_user2;
select user_name,authentication_string from mo_catalog.mo_user where user_name='test_user2';
create user if not exists test_user2 identified by '12345678';
select user_name,authentication_string from mo_catalog.mo_user where user_name='test_user2';
--alter account test_user2 default role moadmin identified by 'apple_987' comment'alter comment 正确';

--7.异常：创建用户赋予moadmin角色
create user if not exists user1 identified by '12345678' default role moadmin;

drop user if exists yellowcar,test_user,user123456,123user,`1234@fff`,`1234`,`user1234.aaaa`,`123user中文`,user_role,user123456,c_user_01,c_user_02,c_user_03,customername,customer,custom,finley,jason,`neil@192.168.1.10`;
drop user if exists test_user,test_user0,test_user1,test_user2,user1,user2,user3,user4,user5,user6,user7,user8,user9,user10,tester1,tester2,tester001,tester002,tester003,`daisy@192.168.1.10`,daisy;
drop user if exists user1,tester1,tester2,jason,finley,custom,customer,customname,c_user_01,c_user_02,c_user_03;
drop account if exists test5555;
drop role if exists low_level,mid_level,high_level,u_role;
drop table if exists testdb.aaa;
set global enable_privilege_cache = on;
