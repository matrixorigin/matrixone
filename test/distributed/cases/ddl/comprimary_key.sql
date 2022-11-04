-- env statement prepare
drop table if exists ex_table_cpk;
drop table if exists cpk_table_1;
drop table if exists cpk_table_1_pk;
drop table if exists cpk_table_2;
drop table if exists cpk_table_3;
drop table if exists cpk_table_3_pk;
drop table if exists cpk_table_4;
drop table if exists cpk_table_5;
drop table if exists cpk_table_6;
drop table if exists cpk_table_7;
drop table if exists cpk_table_8;
drop table if exists cpk_table_9;
drop table if exists cpk_table_10;
drop table if exists cpk_table_11;
drop table if exists cpk_table_42;
drop table if exists cpk_table_43;
create external table ex_table_cpk(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/cpk_table_1.csv'} ;

-- 复合主键int+varchar
create table cpk_table_1(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255),primary key(col3, col18));
insert into cpk_table_1 select * from ex_table_cpk;
select col3,col18 from cpk_table_1;
-- 唯一性验证
insert into cpk_table_1 select * from ex_table_cpk;

-- 复合主键tinyint+datetime+int
create  table  cpk_table_2(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255),primary key(col1,col12,col3));
insert into cpk_table_2 select * from ex_table_cpk;
select col1,col12,col3 from cpk_table_2;
-- 唯一性验证
insert into cpk_table_2 select * from ex_table_cpk;

-- 复合主键smallint+float+timestamp+varchar
create  table  cpk_table_3(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255),primary key(col2,col9,col14,col20));
insert into cpk_table_3 select * from ex_table_cpk;
select col2,col9,col14,col20 from cpk_table_3;
-- 唯一性验证
create  table  cpk_table_3_pk(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255),primary key(col2,col9,col14,col19));
-- @bvt:issue#6261
insert into cpk_table_3_pk select * from ex_table_cpk;
-- @bvt:issue
-- 复合主键bigint+tinyint unsigned+DateTime+ decimal+double
create  table  cpk_table_4(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255),primary key(col4,col5,col13,col10,col6));
insert into cpk_table_4 select * from ex_table_cpk;
select col4,col5,col13,col10,col16 from cpk_table_4;
-- 唯一性验证
-- @bvt:issue#6261
insert into cpk_table_4 select * from ex_table_cpk;
-- @bvt:issue
-- 复合主键smallint unsigned+int unsigned+bigint unsigned+char+bool+varchar
create  table  cpk_table_5(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 char(255),col19 varchar(255),col20 varchar(255),primary key(col6,col7,col8,col18,col16,col19));
insert into cpk_table_5 select * from ex_table_cpk;
select col6,col7,col8,col18,col16,col19 from cpk_table_5;
-- 唯一性验证
-- @bvt:issue#6261
insert into cpk_table_5 select * from ex_table_cpk;
-- @bvt:issue
show create table cpk_table_5;

-- 复合主键19个
create  table  cpk_table_6(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 char(255),col19 varchar(255),col20 varchar(255),primary key(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col18,col19,col20));
insert into cpk_table_6 select * from ex_table_cpk;
select * from cpk_table_6;
-- 唯一性验证
-- @bvt:issue#6261
insert into cpk_table_6 select * from ex_table_cpk;
-- @bvt:issue
-- 异常：复合主键列部分不存在
create table cpk_table_7(a int,b float,c char(20),primary key(a,d));
create table cpk_table_8(a int,b float,c char(20),primary key(e,f));

-- 部分复合主键有空值
create table cpk_table_9(col1 int,col2 varchar(255),col3 timestamp, col4 double,col5 date,primary key(col1, col2,col5));
insert into cpk_table_9 values (3,'','2019-02-10 00:00:00',78.90,'2001-07-10');
insert into cpk_table_9 values (4,'beijing','2019-02-10 00:00:00',78.90,NULL);

-- 异常：全部有空值
insert into cpk_table_9 values (NULL,NULL,'2019-02-10 00:00:00',78.90,NULL);

-- 异常：复合主键出现重复字段名
create table cpk_table_10(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255),primary key(col3,col11,col12,col3));
create table cpk_table_10(col1 int,col2 text,col3 double,primary key(col1,col2));
-- load data
create table cpk_table_11(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255),primary key(clo3, col19));
load data infile '$resources/external_table_file/ex_table_sep_1.csv' into table cpk_table_11 fields terminated by '|' enclosed by '\"';

-- insert into select xxx
create table cpk_table_42(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255));
insert into cpk_table_42  select * from ex_table_cpk;
create table cpk_table_43(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255),primary key(clo4, col14,col20));
insert into cpk_table_43 select * from cpk_table_42;
select clo4, col14,col20 from cpk_table_43;
-- insert into values
insert into cpk_table_43 values(8,11,1,9,15,600,700,56,3.4365,5.5590,"math","2020-04-30","1999-08-07 00:00:00","1975-09-09 23:59:59",true,602.53,"abcdefg","message","s@126.com","balabalabalabalabala");
select clo4, col14,col20 from cpk_table_43;
-- insert into 无复合主键表 select 复合主键表
insert into cpk_table_42 select * from cpk_table_43;
select clo4, col14,col20 from cpk_table_43;

-- test cases for external table with null values
drop table if exists rawlog_withnull;
CREATE external TABLE rawlog_withnull (
`raw_item` VARCHAR(1024),
`node_uuid` VARCHAR(36),
`node_type` VARCHAR(64),
`span_id` VARCHAR(16),
`statement_id` VARCHAR(36),
`logger_name` VARCHAR(1024),
`timestamp` DATETIME,
`level` VARCHAR(1024),
`caller` VARCHAR(1024),
`message` TEXT,
`extra` JSON,
`err_code` VARCHAR(1024),
`error` TEXT,
`stack` VARCHAR(4096),
`span_name` VARCHAR(1024),
`parent_span_id` VARCHAR(16),
`start_time` DATETIME,
`end_time` DATETIME,
`duration` BIGINT UNSIGNED,
`resource` JSON)
infile{"filepath"='$resources/external_table_file/rawlog_withnull.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select raw_item,node_uuid,node_type,span_id,statement_id,logger_name,timestamp from rawlog_withnull order by 1 limit 1;
drop table if exists rawlog_withnull;