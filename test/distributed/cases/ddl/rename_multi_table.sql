drop database if exists abc;
create database abc;
use abc;
create table rename_table_01(a int primary key auto_increment,b varchar(10));
create table rename_table_02(a int primary key auto_increment,b varchar(10));
create table rename_table_03(a int primary key auto_increment,b varchar(10));
create table rename_table_04(a int primary key auto_increment,b varchar(10));
create table rename_table_05(a int primary key auto_increment,b varchar(10));

insert into rename_table_01(b) values ('key');
insert into rename_table_02(b) values ('key');
insert into rename_table_03(b) values ('key');
insert into rename_table_04(b) values ('key');
insert into rename_table_05(b) values ('key');
show tables;

rename table rename_table_01 to rename01,rename_table_02 to rename02,rename_table_03 to rename03,rename_table_04 to rename04,rename_table_05 to rename05;
show tables;
desc rename01;
desc rename02;
desc rename03;
desc rename04;
desc rename05;

show create table rename01;
show create table rename02;
show create table rename03;
show create table rename04;
show create table rename05;

drop database if exists abc;
