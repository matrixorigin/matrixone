drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- stage test
drop database if exists test;
create database test;
use test;


-- create table and write data to stage and then load data from stage and and upload to table
-- datatype: date, datetime, timestamp, bool
drop table if exists t1;
create table t1(
col1 date not null,
col2 datetime,
col3 timestamp,
col4 bool
);
set time_zone = 'SYSTEM';
load data infile '$resources/load_data/time_date_1.csv' into table t1 fields terminated by ',';
select * from t1;
drop stage if exists stage01;
create stage stage01 url = 'file:///$resources/into_outfile/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1, col2 from t1 into outfile 'stage://stage01/local_stage_table001.csv';
drop table t1;
create table t1(
col1 date not null,
col2 datetime
);
load data infile '$resources/into_outfile/stage/local_stage_table001.csv' into table t1 fields terminated by ',' ignore 1 lines;
select * from t1;
show create table t1;
drop table t1;
drop stage stage01;



-- create table and write data to stage and then load data from stage and and upload to table
-- datatype: float, double, decimal
drop table if exists t2;
create table t2(
col1 float,
col2 double,
col3 decimal(5,2),
col4 decimal(20,5)
);
load data infile '$resources/load_data/float_1.csv' into table t2 fields terminated by ',';
select * from t2;
drop stage if exists stage02;
create stage stage02 url = 'file:///$resources/into_outfile/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col3, col4 from t2 into outfile 'stage://stage02/local_stage_table002.csv';
drop table t2;
create table t2(
col3 decimal(5,2),
col4 decimal(20,5)
);
load data infile '$resources/into_outfile/stage/local_stage_table002.csv' into table t2 fields terminated by ',' ignore 1 lines;
select * from t2;
show create table t2;
drop table t2;
drop stage stage02;



-- create table and write data to stage and then load data from stage and and upload to table
-- datatype: char, varchar, text, varchar
drop table if exists t3;
create table t3(
col1 char(225) default 'a',
col2 varchar(225),
col3 text,
col4 varchar(225)
);
load data infile '$resources/load_data/char_varchar_1.csv' into table t3 fields terminated by ',';
select * from t3;
drop stage if exists stage03;
create stage stage03 url = 'file:///$resources/into_outfile/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1, col3 from t3 into outfile 'stage://stage03/local_stage_table003.csv';
drop table t3;
create table t3(
col1 char(225) default 'a',
col3 text
);
load data infile '$resources/into_outfile/stage/local_stage_table003.csv' into table t3 fields terminated by ',' ignore 1 lines;
select * from t3;
show create table t3;
drop table t3;
drop stage stage03;



-- create table and write data to stage and then load data from stage and and upload to table
-- datatype: tinyint, smallint, int, bigint, tinyint unsigned, smallint unsigned, int unsigned, bigint unsigned
drop table if exists t4;
create table t4(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned
);
load data infile '$resources/load_data/integer_numbers_1.csv' into table t4 fields terminated by ',';
select * from t4;
drop stage if exists stage04;
create stage stage04 url = 'file:///$resources/into_outfile/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1, col3, col5, col7 from t4 into outfile 'stage://stage04/local_stage_table004.csv';
drop table if exists t4;
create table t4(
col1 tinyint,
col3 int,
col5 tinyint unsigned,
col7 int unsigned
);
load data infile '$resources/into_outfile/stage/local_stage_table004.csv' into table t4 fields terminated by ',' ignore 1 lines;
select * from t4;
show create table t4;
drop table t4;
drop stage stage04;



-- create table and write data to stage and then load data from stage and upload to table
-- test load data, auto_increment
-- load duplicate data into table report error
drop table if exists t5;
create table t5(
col1 int auto_increment primary key,
col2 int,
col3 int
);
load data infile '$resources/load_data/auto_increment_1.csv' into table t5 fields terminated by ',';
select * from t5;
drop stage if exists stage05;
create stage stage05 url = 'file:///$resources/into_outfile/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1,col2,col3 from t5 into outfile 'stage://stage05/local_stage_table005.csv';
truncate t5;
insert into t5 values (1,1,1);
-- echo duplicate
load data infile '$resources/into_outfile/stage/local_stage_table005.csv' into table t5 fields terminated by ',' ignore 1 lines;
delete from t5;
load data infile '$resources/into_outfile/stage/local_stage_table005.csv' into table t5 fields terminated by ',' ignore 1 lines;
select * from t5;
show create table t5;
drop table t5;
drop stage stage05;



-- load data with fields terminated by ',' enclosed by '`' lines terminated by '\n' from stage and upload to table
-- @bvt:issue#18712
drop table if exists t6;
create table t6 (col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_enclosed_by01.csv' into table t6 fields terminated by ',' enclosed by '`' lines terminated by '\n';
select * from t6;
drop stage if exists stage06;
create stage stage06 url = 'file:///$resources/into_outfile/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col2 from t6 into outfile 'stage://stage06/local_stage_table006.csv';
drop table if exists t6;
create table t6 (col2 varchar(20));
load data infile '$resources/into_outfile/stage/local_stage_table006.csv' into table t6 fields terminated by ',' enclosed by '`' lines terminated by '\n' ignore 1 lines;
select * from t6;
show create table t6;
drop table t6;
drop stage stage06;
-- @bvt:issue



-- load compressed file(tar.gz) into stage and upload to table
drop table if exists t7;
create table t7(col1 text, col2 text);
load data infile {'filepath'='$resources/load_data/text.csv.tar.gz', 'compression'='tar.gz'} into table t7 FIELDS  ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' parallel 'true';
select * from t7;
drop stage if exists stage07;
create stage stage07 url = 'file:///$resources/into_outfile/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col2 from t7 into outfile 'stage://stage07/local_stage_table007.csv';
drop table if exists t7;
create table t7(col1 text);
load data infile '$resources/into_outfile/stage/local_stage_table007.csv' into table t7 FIELDS ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' ignore 1 lines parallel 'true';
select * from t7;
show create table t7;
drop table t7;
drop stage stage07;



-- normal account administrators have permission to create stages
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db01;
create database db01;
use db01;
drop table if exists t11;
create table t11 (col1 int);
load data infile '$resources/load_data/test_character.csv' into table t11 CHARACTER SET utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t11;
drop stage if exists stage11;
create stage stage11 url = 'file:///$resources/into_outfile/stage' comment = 'this is a stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1 from t11 into outfile 'stage://stage11/local_stage_table011.csv';
drop table if exists t11;
create table t11 (col1 int);
load data infile '$resources/into_outfile/stage/local_stage_table011.csv' into table t11 CHARACTER SET utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' ignore 1 lines;
select * from t11;
drop table t11;
drop stage stage11;
drop database db01;
-- @session



-- external table
drop table if exists ex_table_01;
create table ex_table_01(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned,
col9 float,
col10 double,
col11 varchar(255),
col12 Date,
col13 DateTime,
col14 timestamp,
col15 bool,
col16 decimal(5,2),
col17 text,
col18 varchar(255),
col19 varchar(255),
col20 varchar(255));
load data infile '$resources/external_table_file/ex_table_sep_10.csv' into table ex_table_01 fields terminated by ',' enclosed by '\"' lines terminated by '\r\n';
select col1, col2, col3, col4, col5, col6, col7, col8, col9, col10 from ex_table_01;
drop stage if exists ex_stage;
create stage ex_stage url = 'file:///$resources/into_outfile/stage' comment = 'file stage';
select col1, col2, col3, col4, col5, col6, col7, col8, col9, col10 from ex_table_01 into outfile 'stage://ex_stage/ttttable01.csv';

-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
drop table ex_table_01;
create external table ex_table_01(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned,
col9 float,
col10 double)
infile 'stage://ex_stage/ttttable01.csv' fields terminated by ',' enclosed by '\"' lines terminated by '\r\n' ignore 1 lines;
select * from ex_table_01;
drop table ex_table_01;
drop stage ex_stage;

drop database test;
drop account acc01;