-- @suite
-- @setup
drop database if exists test01;
drop database if exists test03;
drop database if exists test04;
drop database if exists test05;
drop database if exists `测试数据库`;
drop database if exists t01234567890123456789012345678901234567890123456789012345678901234567890123456789;

-- @case
-- @desc:test for create database
-- @label:bvt
create database test01;
create database IF NOT EXISTS test01;
-- @case
-- @desc:test for create database with chinese name
-- @label:bvt
create database `测试数据库`;
-- @case
-- @desc:test for create database with utf-8 charset
-- @label:bvt
#create database test03 default character set utf8 collate utf8_general_ci encryption 'Y';
#create database test04 character set=utf8 collate=utf8_general_ci encryption='N';
-- @case
-- @desc:test for create database with too long name
-- @label:bvt
create database t01234567890123456789012345678901234567890123456789012345678901234567890123456789;

-- @case
-- @desc:test for show database
-- @label:bvt
show databases;
drop database if exists test01;
drop database if exists test03;
drop database if exists test04;
drop database if exists test05;
drop database if exists `测试数据库`;
drop database if exists t01234567890123456789012345678901234567890123456789012345678901234567890123456789;
drop database if exists `TPCH_0.01G`;
create database if not exists `TPCH_0.01G`;
use `TPCH_0.01G`;
drop database if exists `TPCH_0.01G`;
drop database if exists db1;
create database if not exists db1;
show schemas;
drop database if exists db1;