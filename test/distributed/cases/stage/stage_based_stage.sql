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
create stage stage01 url = 'file:///$resources/into_outfile';
drop stage if exists substage01;
create stage substage01 url = 'stage://stage01/stage/';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select * from t1 into outfile 'stage://substage01/local_stage_t01.csv';
truncate t1;
load data infile '$resources/into_outfile/stage/local_stage_t01.csv' into table t1 fields terminated by ',' ignore 1 lines;
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
select * from t2 into outfile 'stage://substage02/local_stage_t02.csv';
truncate t2;
load data infile '$resources/into_outfile/stage/local_stage_t02.csv' into table t2 fields terminated by ',' ignore 1 lines;
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
select * from t3 into outfile 'stage://substage03/local_stage_t03.csv';
delete from t3;
load data infile '$resources/into_outfile/stage/local_stage_t03.csv' into table t3 fields terminated by ',' ignore 1 lines;
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
select * from t4 into outfile 'stage://substage04/local_stage_t04.csv';
delete from t4;
load data infile '$resources/into_outfile/stage/local_stage_t04.csv' into table t4 fields terminated by ',' ignore 1 lines;
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
select * from t5 into outfile 'stage://substage05/local_stage_t05.csv';
truncate t5;
insert into t5 values (1,1,1);
-- echo duplicate
load data infile '$resources/into_outfile/stage/local_stage_t05.csv' into table t5 fields terminated by ',' ignore 1 lines;
delete from t5;
load data infile '$resources/into_outfile/stage/local_stage_t05.csv' into table t5 fields terminated by ',' ignore 1 lines;
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
select * from t6 into outfile 'stage://substage06/local_stage_t06.csv';
truncate t6;
load data infile '$resources/into_outfile/stage/local_stage_t06.csv' into table t6 fields terminated by ',' enclosed by '`' lines terminated by '\n' ignore 1 lines;
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
select * from t7 into outfile 'stage://substage07/local_stage_t07.csv';
truncate t7;
load data infile '$resources/into_outfile/stage/local_stage_t07.csv' into table t7 FIELDS ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' ignore 1 lines parallel 'true';
select * from t7;
show create table t7;
drop table t7;
drop stage stage07;
drop stage substage07;



-- load compressed file(tar.bz2) into stage and upload to table
drop table if exists t8;
create table t8(col1 text, col2 text);
load data infile {'filepath'='$resources/load_data/text.csv.tar.gz', 'compression'='tar.gz'} into table t8 FIELDS  ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' parallel 'true';
select * from t8;
drop stage if exists stage08;
create stage stage08 url = 'file:///$resources/into_outfile' comment = 'this is a stage';
drop stage if exists substage08;
create stage substage08 url = 'stage://stage08/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select * from t8 into outfile 'stage://substage08/local_stage_t08.csv';
truncate t8;
load data infile '$resources/into_outfile/stage/local_stage_t08.csv' into table t8 FIELDS ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' ignore 1 lines parallel 'true';
select * from t8;
show create table t8;
drop table t8;
drop stage stage08;
drop stage substage08;



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
select * from t9 into outfile 'stage://substage09/local_stage_t09.csv';
truncate t9;
load data infile '$resources/into_outfile/stage/local_stage_t09.csv' into table t9 fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select * from t9;
show create table t9;
drop table t9;
drop stage stage09;
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
drop stage substage09;



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
create stage stage11 url = 'file:///$resources/into_outfile' comment = 'stage11';
drop stage if exists substage11;
create stage substage11 url = 'stage://stage11/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select * from t11 into outfile 'stage://substage11/local_stage_t11.csv';
truncate t11;
load data infile '$resources/into_outfile/stage/local_stage_t11.csv' into table t11 CHARACTER SET utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' ignore 1 lines;
select * from t11;
drop table t11;
drop stage stage11;
drop stage substage11;
drop database db01;
-- @session



-- abnormal test, create duplicate sub-stage
drop stage if exists stage01;
create stage ab_stage url = 'file:///$resources/into_outfile';
create stage substage12 url = 'stage://ab_stage/stage';
create stage substage12 url = 'stage://ab_stage/stage';
drop stage ab_stage;
drop stage substage12;



-- create stage, if origin url is invalid, upload to stage, report error
drop table if exists ab_table01;
create table ab_table01(col1 text, col2 text);
load data infile {'filepath'='$resources/load_data/text.csv.tar.gz', 'compression'='tar.gz'} into table ab_table01 FIELDS  ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' parallel 'true';
select * from ab_table01;
drop stage if exists ab_stage;
create stage ab_stage url = 'file:///$resources/into_outfile';
drop stage if exists sub_stage;
create stage sub_stage url = 'stage://ab_stage/stage';
select * from ab_table01 into outfile 'stage://local_stage_t12.csv';
drop table ab_table01;
drop stage ab_stage;
drop stage sub_stage;




-- create stage, if url is invalid, load from stage, report error
drop table if exists ab_table02;
create table ab_table02(col1 text, col2 text);
load data infile {'filepath'='$resources/load_data/text.csv.tar.gz', 'compression'='tar.gz'} into table ab_table02 FIELDS  ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' parallel 'true';
select * from ab_table02;
drop stage if exists ab_stage;
create stage ab_stage url = 'file:///$resources/into_outfile';
drop stage if exists sub_stage;
create stage sub_stage url = 'stage://ab_stage/stage';
select * from ab_table02 into outfile 'stage://sub_stage/ttt.csv';
truncate ab_table02;
-- load from stage report error
load data infile '$resources/into_outfile/stage/abc.csv' into table ab_table02 CHARACTER SET "utf_8" fields terminated by ',' lines starting by 'cha' ignore 1 lines;
select * from ab_table02;
drop table ab_table02;
drop stage ab_stage;
drop stage sub_stage;



-- create stage, load data into table, alter stage url, comment
drop table if exists t15;
create table t15(col1 int unique key, col2 bigint, col3 varchar(30));
load data infile '$resources/load_data/test_starting_by03.csv' into table t15 fields terminated by '|' lines terminated by '\n';
select * from t15;
drop stage if exists alter_stage01;
create stage alter_stage01 url = 'file:///$resources/into_outfile' comment = '这是一个基于file system创建的stage';
drop stage if exists alter_substage01;
create stage alter_substage01 url = 'stage://alter_stage01/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
alter stage alter_stage01 set url = 'file:/into_outfile';
select * from t15 into outfile 'stage://alter_stage01/local_stage_t09.csv';
alter stage alter_stage01 set comment = 'this is a modified stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
truncate t15;
load data infile 'file:/into_outfile/into_outfile/stage/local_stage_t09.csv' into table t15 fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select * from t15;
show create table t15;
drop table t15;
drop stage alter_stage01;
drop stage alter_substage01;



-- external table
-- fields terminated by ',' enclosed by '\"' lines terminated by '\n'
drop table if exists ex_table_1;
create table ex_table_1
(char_1 char(20),
char_2 varchar(10),
date_1 date,
date_2 datetime,
date_3 timestamp
);
load data infile '$resources/external_table_file/ex_table_char.csv' into table ex_table_3 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_1;
drop stage if exists ex_stage01;
create stage ex_stage01 url = 'file:///$resources/into_outfile' comment = '基于external table加载数据';
drop stage if exists sub_stage01;
create stage sub_stage01 url = 'stage://ex_stage01/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select * from ex_table_1 into outfile 'stage://sub_stage01/external_t01.csv';
drop table if exists ex_table_1;
create external table ex_table_1
(char_1 char(20),
char_2 varchar(10),
date_1 date,
date_2 datetime,
date_3 timestamp)
infile 'stage://sub_stage01/external_t01.csv' fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 lines;
select * from ex_table_1;
show create table ex_table_1;
drop table ex_table_1;
drop stage ex_stage01;
drop stage sub_stage01;



-- external table
-- fields terminated by '|' enclosed by '' lines terminated by '\n' ignore 1 lines
drop table if exists ex_table_2;
create table ex_table_2(
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
col20 varchar(255)
);
load data infile '$resources/external_table_file/ex_table_sep_1.csv' into table ex_table_2 fields terminated by '|' enclosed by '' lines terminated by '\n';
select col1, col2, col5, col6, col7, col8, col9, col10, col12, col13 from ex_table_2;
drop stage if exists ex_stage02;
create stage ex_stage02 url = 'file:///$resources/into_outfile' comment = 'ex_stage02';
drop stage if exists sub_stage02;
create stage sub_stage02 url = 'stage://ex_stage02/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select col1, col2, col5, col6, col7, col8, col9, col10, col12, col13 from ex_table_2 into outfile 'stage://sub_stage02/external_t02.csv';
drop table ex_table_2;
create external table ex_table_2(
col1 tinyint,
col2 smallint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned,
col9 float,
col10 double,
col11 varchar(255),
col12 Date) infile 'stage://sub_stage02/external_t02.csv' fields terminated by ',' enclosed by '' lines terminated by '\n' ignore 1 lines;
select * from ex_table_2;
show create table ex_table_2;
drop table ex_table_2;
drop stage ex_stage02;
drop stage sub_stage02;



-- create external table and load data from stage in non-sys account
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists acc_test;
create database acc_test;
use acc_test;
drop table if exists jsonline_t1;
create table jsonline_t1(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned
);
load data infile{'filepath'='$resources/load_data/integer_numbers_1.jl','format'='jsonline','jsondata'='object'}into table jsonline_t1;
select * from jsonline_t1;
drop stage if exists ex_stage03;
create stage ex_stage03 url = 'file:///$resources/into_outfile' comment = 'ex_stage03';
drop stage if exists sub_stage03;
create stage sub_stage03 url = 'stage://ex_stage03/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select * from jsonline_t1 into outfile 'stage://sub_stage03/external_t03.csv';
drop table jsonline_t1;
use acc_test;
create external table jsonline_t1 (
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned
) infile 'stage://sub_stage03/external_t03.csv' fields terminated by ',' ignore 1 lines;
select * from jsonline_t1;
drop table jsonline_t1;
drop stage ex_stage03;
drop stage sub_stage03;
drop database acc_test;
-- @session
drop database test;
drop account acc01;