set time_zone='SYSTEM';
--env prepare statement
drop table if exists trun_table_01;
drop table if exists trun_table_02;
drop table if exists trun_table_03;

create table trun_table_01(clo1 tinyint AUTO_INCREMENT,clo2 smallint not null,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255) default 'style',col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 json,primary key(clo3,col12),unique key uk1 (col16),key k1 (clo1));
insert into trun_table_01 values (1,-2,3,56,9,8,10,50,99.0,82.99,'yellllow','1999-11-11','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,23.98430943,'tttext','{"a": "3","b": [0,1,2]}');
insert into trun_table_01 values (2,-2,90,56,9,8,10,50,99.0,82.99,'yellllow','2011-01-21','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,98.23,'tttext','{"a": "3","b": [0,1,2]}');
insert into trun_table_01 values (3,-2,100,56,9,8,10,50,99.0,82.99,'yellllow','2021-01-21','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,77.3,'tttext','{"a": "3","b": [0,1,2]}');
insert into trun_table_01 values (4,-2,102,56,9,8,10,50,99.0,82.99,'yellllow','2022-10-11','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,209.43,'tttext','{"a": "3","b": [0,1,2]}');
select * from trun_table_01;
truncate table trun_table_01;
select * from trun_table_01;
truncate table trun_table_01;
select * from trun_table_01;
insert into trun_table_01(clo2 ,clo3 ,clo4,clo5,clo6,clo7,clo8 ,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18) values (-2,3,56,9,8,10,50,99.0,82.99,'yellllow','1999-11-11','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,54.3,'tttext','{"a": "3","b": [0,1,2]}');
insert into trun_table_01(clo2 ,clo3 ,clo4,clo5,clo6,clo7,clo8 ,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18) values (-2,90,56,9,8,10,50,99.0,82.99,'yellllow','2011-01-21','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,11.43,'tttext','{"a": "3","b": [0,1,2]}');
select * from trun_table_01;
-- @bvt:issue#7133
update trun_table_01 set clo3=90 ,col12='2011-01-21' where clo1=1;
update trun_table_01 set clo3=66 ,col12='2011-01-21' where clo1=1;
select * from trun_table_01;
-- @bvt:issue
truncate  table trun_table_01;
select * from trun_table_01;
delete from  trun_table_01 where clo1=2;
select * from trun_table_01;
insert into trun_table_01(clo2,clo3,col12) select 36,118,'2002-12-03';
insert into trun_table_01(clo2,clo3,col12) select 45,108,'2012-02-23';
select * from trun_table_01;
truncate table trun_table_01;
select * from trun_table_01;
drop table trun_table_01;
create table trun_table_01(clo1 tinyint AUTO_INCREMENT,clo2 smallint not null,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255) default 'style',col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 json,primary key(clo3,col12),unique key uk1 (col16),key k1 (clo1));
insert into trun_table_01 values (1,-2,3,56,9,8,10,50,99.0,82.99,'yellllow','1999-11-11','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,23.98430943,'tttext','{"a": "3","b": [0,1,2]}');
select * from trun_table_01;

-- @bvt:issue#9124
create temporary table trun_table_02(clo1 tinyint AUTO_INCREMENT,clo2 smallint not null,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255) default 'style',col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 json,primary key(clo3,col12),unique key uk1 (col16),key k1 (clo1));
insert into trun_table_02 values (1,-2,3,56,9,8,10,50,99.0,82.99,'yellllow','1999-11-11','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,23.98430943,'tttext','{"a": "3","b": [0,1,2]}');
insert into trun_table_02 values (2,-2,90,56,9,8,10,50,99.0,82.99,'yellllow','2011-01-21','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,98.23,'tttext','{"a": "3","b": [0,1,2]}');
insert into trun_table_02 values (3,-2,100,56,9,8,10,50,99.0,82.99,'yellllow','2021-01-21','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,77.3,'tttext','{"a": "3","b": [0,1,2]}');
insert into trun_table_02 values (4,-2,102,56,9,8,10,50,99.0,82.99,'yellllow','2022-10-11','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,209.43,'tttext','{"a": "3","b": [0,1,2]}');
select * from trun_table_02;
truncate table trun_table_02;
select * from trun_table_02;
delete from trun_table_02 where clo1=1;
insert into trun_table_02 values (3,-2,100,56,9,8,10,50,99.0,82.99,'yellllow','2021-01-21','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,23.98430943,'tttext','{"a": "3","b": [0,1,2]}');
insert into trun_table_02 values (4,-2,102,56,9,8,10,50,99.0,82.99,'yellllow','2022-10-11','1999-11-11 12:00:00','2010-11-11 11:00:00.00',false,2.43,'tttext','{"a": "3","b": [0,1,2]}');
select * from trun_table_02;

delete from trun_table_02 where clo1=3;
select * from trun_table_02;

truncate table trun_table_02;
select * from trun_table_02;
insert into trun_table_02 select * from trun_table_01;
select * from trun_table_02;

update trun_table_02 set clo3=90 where clo1=1;
select * from trun_table_02;
update trun_table_02 set clo3=90, col12='1992-11-01' where clo1=1;
select * from trun_table_02;

truncate table trun_table_02;
select * from trun_table_02;
drop table trun_table_02;
-- @bvt:issue

create external table trun_table_03(clo1 tinyint AUTO_INCREMENT,clo2 smallint not null,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255) default 'style',col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 json)infile{"filepath"='$resources/external_table_file/trun_table.csv'} fields terminated by '|' lines terminated by '\n';
select * from  trun_table_03;
truncate table trun_table_03;
select * from  trun_table_03;
truncate table trun_table_03;
drop table trun_table_03;
create external table trun_table_03(clo1 tinyint AUTO_INCREMENT,clo2 smallint not null,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255) default 'style',col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 json)infile{"filepath"='$resources/external_table_file/trun_table.csv'} fields terminated by '|'  lines terminated by '\n';
drop table trun_table_03;

-- accesscontrol
use mo_catalog;
truncate table mo_database;
truncate table mo_tables;
truncate table mo_account;
truncate table mo_role;
truncate table mo_user_grant;
truncate table mo_role_grant;
truncate table mo_role_privs;
use system ;
truncate table statement_info;
truncate table rawlog;
truncate table log_info;
truncate table error_info;
truncate table span_info;
use information_schema;
truncate table key_column_usage ;
truncate table columns;
truncate table profiling;
truncate table `PROCESSLIST`;
truncate table user_privileges;
truncate table schemata;
truncate table character_sets;
truncate table triggers;
truncate table tables;
truncate table engines;
use truncate_table_2;
drop table if exists trun_table_01;
drop table if exists trun_table_02;
drop table if exists trun_table_03;






