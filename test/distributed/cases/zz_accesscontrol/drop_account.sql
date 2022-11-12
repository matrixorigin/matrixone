--env prepare statement
drop account if exists drop_account_01;
drop account if exists drop_account_02;
drop account if exists drop_account_03;
drop account if exists drop_account_04;
drop account if exists drop_account_05;
drop database if exists drop_acc_db_1;

--drop account 存在/不存在,if exists不存在/存在,覆盖``
create account drop_account_01 ADMIN_NAME 'root' IDENTIFIED BY '1234567890';
select account_name from mo_catalog.mo_account where account_name='drop_account_01';
drop account drop_account_01;
drop account drop_account_02;
select account_name from mo_catalog.mo_account where account_name='drop_account_01';
create account if not exists drop_account_03 ADMIN_NAME 'root' IDENTIFIED BY '1234567890';
drop account drop_account_03;
drop account if exists drop_account_03;
select account_name from mo_catalog.mo_account where account_name='drop_account_03';
create account drop_account_04 ADMIN_NAME 'root' IDENTIFIED BY '1234567890';
drop account `drop_account_04`;

--异常：drop sys，语法错误
-- @bvt:issue#5705
drop account sys;
-- @bvt
drop accout abc;
drop account if not exists abc;
drop account exists abc;
drop account '';

--account下创建用户，role，db，table。。后drop account
create account drop_account_05 ADMIN_NAME 'admin' IDENTIFIED BY '12345';
-- @session:id=2&user=drop_account_05:admin&password=12345
create role drop_acc_role_1;
create database drop_acc_db_1;
create table drop_acc_db_1.drop_acc_tb_1(a int);
insert into drop_acc_db_1.drop_acc_tb_1 values (20);
create user drop_acc_user_1 identified by '1111' default role drop_acc_role_1;
grant select ,insert ,update on table drop_acc_db_1.* to drop_acc_role_1 with grant option;
-- @session

drop account if exists drop_account_01;
drop account if exists drop_account_02;
drop account if exists drop_account_03;
drop account if exists drop_account_04;
drop account if exists drop_account_05;
drop database if exists drop_acc_db_1;