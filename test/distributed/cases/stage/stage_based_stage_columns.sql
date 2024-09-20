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
create stage stage01 url = 'file:///$resources/into_outfile';
drop stage if exists substage01;
create stage substage01 url = 'stage://stage01/stage/';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1, col3 from t1 into outfile 'stage://substage01/local_stage_t001.csv';
drop table t1;
create table t1(
col1 date not null,
col3 timestamp
);
load data infile 'stage://substage01/local_stage_t001.csv' into table t1 fields terminated by ',' ignore 1 lines;
select * from t1;
show create table t1;
drop table t1;
drop stage stage01;
drop stage substage01;



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
create stage stage02 url = 'file:///$resources/into_outfile';
drop stage if exists substage02;
create stage substage02 url = 'stage://stage02/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col3, col4 from t2 into outfile 'stage://substage02/local_stage_t002.csv';
drop table t2;
create table t2(
col3 decimal(5,2),
col4 decimal(20,5)
);
load data infile 'stage://substage02/local_stage_t002.csv' into table t2 fields terminated by ',' ignore 1 lines;
select * from t2;
show create table t2;
drop table t2;
drop stage stage02;
drop stage substage02;



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
create stage stage03 url = 'file:///$resources/into_outfile';
drop stage if exists substage03;
create stage substage03 url = 'stage://stage03/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col2, col3 from t3 into outfile 'stage://substage03/local_stage_t003.csv';
drop table t3;
create table t3(
col2 varchar(225),
col3 text
);
load data infile 'stage://substage03/local_stage_t003.csv' into table t3 fields terminated by ',' ignore 1 lines;
select * from t3;
show create table t3;
drop table t3;
drop stage stage03;
drop stage substage03;



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
create stage stage04 url = 'file:///$resources/into_outfile';
drop stage if exists substage04;
create stage substage04 url = 'stage://stage04/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col2, col4, col6, col8 from t4 into outfile 'stage://substage04/local_stage_t004.csv';
drop table t4;
create table t4(
col2 smallint,
col4 bigint,
col6 smallint unsigned,
col8 bigint unsigned
);
load data infile 'stage://substage04/local_stage_t004.csv' into table t4 fields terminated by ',' ignore 1 lines;
select * from t4;
show create table t4;
drop table t4;
drop stage stage04;
drop stage substage04;



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
create stage stage05 url = 'file:///$resources/into_outfile';
drop stage if exists substage05;
create stage substage05 url = 'stage://stage05/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1 from t5 into outfile 'stage://substage05/local_stage_t005.csv';
drop table t5;
create table t5 (col1 int auto_increment primary key);
insert into t5 values (1);
-- echo duplicate
load data infile 'stage://substage05/local_stage_t005.csv' into table t5 fields terminated by ',' ignore 1 lines;
delete from t5;
load data infile 'stage://substage05/local_stage_t005.csv' into table t5 fields terminated by ',' ignore 1 lines;
select * from t5;
show create table t5;
drop table t5;
drop stage stage05;
drop stage substage05;



-- load data with fields terminated by ',' enclosed by '`' lines terminated by '\n' from stage and upload to table
-- @bvt:issue#18712
drop table if exists t6;
create table t6 (col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_enclosed_by01.csv' into table t6 fields terminated by ',' enclosed by '`' lines terminated by '\n';
select * from t6;
drop stage if exists stage06;
create stage stage06 url = 'file:///$resources/into_outfile';
drop stage if exists substage06;
create stage substage06 url = 'stage://stage06/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col2 from t6 into outfile 'stage://substage06/local_stage_t006.csv';
drop table t6;
create table t6 (col2 varchar(20));
load data infile 'stage://substage06/local_stage_t006.csv' into table t6 fields terminated by ',' enclosed by '`' lines terminated by '\n' ignore 1 lines;
select * from t6;
show create table t6;
drop table t6;
drop stage stage06;
drop stage substage06;
-- @bvt:issue



-- load compressed file(tar.gz) into stage and upload to table
drop table if exists t7;
create table t7(col1 text, col2 text);
load data infile {'filepath'='$resources/load_data/text.csv.tar.gz', 'compression'='tar.gz'} into table t7 FIELDS  ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' parallel 'true';
select * from t7;
drop stage if exists stage07;
create stage stage07 url = 'file:///$resources/into_outfile';
drop stage if exists substage07;
create stage substage07 url = 'stage://stage07/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1 from t7 into outfile 'stage://substage07/local_stage_t007.csv';
drop table t7;
create table t7(col1 text);
load data infile 'stage://substage07/local_stage_t007.csv' into table t7 FIELDS ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' ignore 1 lines parallel 'true';
select * from t7;
show create table t7;
drop table t7;
drop stage stage07;
drop stage substage07;



-- load data with starting by from stage and upload to table
drop table if exists t9;
create table t9(col1 int unique key, col2 bigint, col3 varchar(30));
load data infile '$resources/load_data/test_starting_by03.csv' into table t9 fields terminated by '|' lines terminated by '\n';
select * from t9;
drop stage if exists stage09;
create stage stage09 url = 'file:///$resources/into_outfile' comment = '这是一个基于file system创建的stage';
drop stage if exists substage09;
create stage substage09 url = 'stage://stage09/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1, col3 from t9 into outfile 'stage://substage09/local_stage_t009.csv';
drop table t9;
create table t9(col1 int unique key, col3 varchar(30));
load data infile 'stage://substage09/local_stage_t009.csv' into table t9 fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select * from t9;
show create table t9;
drop table t9;
drop stage stage09;
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
drop stage substage09;



-- through creating external table, load data from stage-based stage
drop table if exists ex_table_1;
create table ex_table_1(
col1 tinyint default 8,
col2 smallint null,
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
col20 varchar(255),
primary key(col1));
load data infile '$resources/external_table_file/ex_table_3_6.csv'into table ex_table_1 fields terminated by ',';
select * from ex_table_1;
drop stage if exists ex_stage01;
create stage ex_stage01 url = 'file:///$resources/into_outfile' comment = '这是一个基于file system创建的stage';
drop stage if exists substage01;
create stage substage01 url = 'stage://ex_stage01/stage';
select col1, col3, col5, col7, col9, col11, col13, col15 from ex_table_1 into outfile 'stage://substage01/sub_stage001.csv';
drop table ex_table_1;
create external table ex_table_1(
col1 tinyint default 8,
col3 int,
col5 tinyint unsigned,
col7 int unsigned,
col9 float,
col11 varchar(255),
col13 DateTime,
col15 bool,
primary key(col1)
)infile 'stage://substage01/sub_stage001.csv' fields terminated by ',' ignore 1 lines;;
select * from ex_table_1;
drop table ex_table_1;
drop stage ex_stage01;
drop stage substage01;
drop database test;