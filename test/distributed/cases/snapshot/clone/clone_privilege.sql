drop account if exists acc1;
create account acc1 admin_name "root1" identified by "111";

-- @session:id=1&user=acc1:root1&password=111
create database db1 clone mo_catalog;
create database db3 clone system;
create database db4 clone information_schema;

create database db6;
create table db6.t1 clone mo_catalog.mo_tables;
create table db6.t2 clone system.statement_info;

create database db7;
create table db7.t1 (a int primary key);

create table mo_catalog.t1 clone db7.t1;
create table system.t1 clone db7.t1;
-- @session

create database db1 clone mo_catalog;
create database db2 clone system;

create database db3;
create table db3.t1 clone mo_catalog.mo_tables;
create table db3.t2 clone system.statement_info;

create database db4;
create table db4.t1 (a int primary key);
create table mo_catalog.t1 clone db4.t1;
create table system.t1 clone db4.t1;


use mo_catalog;
create database db5 clone db4;

drop account if exists acc1;
drop database if exists db1;
drop database if exists db2;
drop database if exists db3;
drop database if exists db4;
drop database if exists db5;