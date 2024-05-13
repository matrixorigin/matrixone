set global enable_privilege_cache = off;
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

--prepared
create account if not exists drop_account_01 ADMIN_NAME 'root' IDENTIFIED BY '1234567890';
PREPARE s1 FROM "drop account ?";
set @a_var = 'drop_account_01';
EXECUTE s1 USING @a_var;
DEALLOCATE PREPARE s1;
select account_name from mo_catalog.mo_account where account_name='drop_account_01';

--异常：drop sys，语法错误
drop account sys;
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

create account drop_account_test admin_name = 'root' identified by '111';
-- @session:id=3&user=drop_account_test:root&password=111
CREATE DATABASE IF NOT EXISTS `sctc-ls`; 
CREATE DATABASE `emis_etao_006`;
CREATE DATABASE `iot_etao_006`;
CREATE DATABASE `xxl_job`;
CREATE DATABASE `ucl360_bi`;
CREATE DATABASE `ucl360_v3_20220823_unre_003`;
CREATE DATABASE `ucl360_v210`;
create database `111`;
create database if not exists ucl360_v3_20220823;
-- @session
DROP ACCOUNT IF EXISTS drop_account_test;

create account `ce46ba96_6c2f_4344_9b80_a1e9f03c600b` admin_name = 'root' identified by '111';
-- @session:id=4&user=ce46ba96_6c2f_4344_9b80_a1e9f03c600b:root&password=111
CREATE DATABASE IF NOT EXISTS `sctc-ls`; 
CREATE DATABASE `emis_etao_006`;
CREATE DATABASE `iot_etao_006`;
CREATE DATABASE `xxl_job`;
CREATE DATABASE `ucl360_bi`;
CREATE DATABASE `ucl360_v3_20220823_unre_003`;
CREATE DATABASE `ucl360_v210`;
create database `111`;
create database if not exists ucl360_v3_20220823;
-- @session
DROP ACCOUNT IF EXISTS `ce46ba96_6c2f_4344_9b80_a1e9f03c600b`;

set global enable_privilege_cache = on;