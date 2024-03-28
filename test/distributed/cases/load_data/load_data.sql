
-- test load data, integer numbers
drop table if exists t1;
create table t1(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned
);

-- load data
load data infile '$resources/load_data/integer_numbers_1.csv' into table t1 fields terminated by ',';
select * from t1;

-- into outfile
select * from t1 into outfile '$resources/into_outfile/outfile_integer_numbers_1.csv';
delete from t1;

-- load data
load data infile '$resources/into_outfile/outfile_integer_numbers_1.csv' into table t1 fields terminated by ',' ignore 1 lines;
select * from t1;
delete from t1;

load data infile '$resources/load_data/integer_numbers_2.csv' into table t1 fields terminated by'*';
select * from t1;
delete from t1;

drop table t1;


-- test load data, char varchar type
drop table if exists t2;
create table t2(
col1 char(225),
col2 varchar(225),
col3 text,
col4 varchar(225)
);

-- load data
load data infile '$resources/load_data/char_varchar_1.csv' into table t2 fields terminated by ',';
select * from t2;

-- into outfile
select * from t2 into outfile '$resources/into_outfile/outfile_char_varchar_1.csv';
delete from t2;

-- load data
load data infile '$resources/into_outfile/outfile_char_varchar_1.csv' into table t2 fields terminated by ',' ignore 1 lines;
select * from t2;
delete from t2;

load data infile '$resources/load_data/char_varchar_2.csv' into table t2 fields terminated by ',';
select * from t2;
delete from t2;


load data infile '$resources/load_data/char_varchar_3.csv' into table t2 fields terminated by ',';
select * from t2;
delete from t2;

load data infile '$resources/load_data/char_varchar_4.csv' into table t2 fields terminated by'|';
select * from t2;
delete from t2;

load data infile '$resources/load_data/char_varchar_5.csv' into table t2 fields terminated by'?';
select * from t2;
delete from t2;

drop table t2;


-- test load data, float type double type
drop table if exists t3;
create table t3(
col1 float,
col2 double,
col3 decimal(5,2),
col4 decimal(20,5)
);

insert into t3 values (1.3,1.3,1.3,1.3);
select * from t3;
load data infile '$resources/load_data/float_1.csv' into table t3 fields terminated by ',';
select * from t3;
delete from t3;

-- load data
load data infile '$resources/load_data/float_2.csv' into table t3 fields terminated by ',';
select * from t3;

-- into outfile
select * from t3 into outfile '$resources/into_outfile/outfile_float_2.csv';
delete from t3;

-- load data
load data infile '$resources/into_outfile/outfile_float_2.csv' into table t3 fields terminated by ',' ignore 1 lines;
select * from t3;
delete from t3;

load data infile '$resources/load_data/float_3.csv' into table t3 fields terminated by ',';

drop table t3;

-- test load data, Time and Date type
drop table if exists t4;
create table t4(
col1 date,
col2 datetime,
col3 timestamp,
col4 bool
);
set time_zone = 'SYSTEM';
load data infile '$resources/load_data/time_date_1.csv' into table t4 fields terminated by ',';
select * from t4;
delete from t4;

-- load data
load data infile '$resources/load_data/time_date_2.csv' into table t4 fields terminated by ',';
select * from t4;

-- into outfile
select * from t4 into outfile '$resources/into_outfile/outfile_time_date_2.csv';
delete from t4;

-- load data
load data infile '$resources/into_outfile/outfile_time_date_2.csv' into table t4 fields terminated by ',' ignore 1 lines;
select * from t4;
delete from t4;

load data infile '$resources/load_data/time_date_3.csv' into table t4 fields terminated by ',';
delete from t4;

load data infile '$resources/load_data/time_date_4.csv' into table t4 fields terminated by';';
select * from t4;
delete from t4;

load data infile '$resources/load_data/time_date_5.csv' into table t4 fields terminated by ',';
select * from t4;
delete from t4;

drop table t4;

-- test load data, auto_increment
drop table if exists t5;
create table t5(
col1 int auto_increment primary key,
col2 int,
col3 int
);

insert into t5 values (1,1,1);
-- echo duplicate
load data infile '$resources/load_data/auto_increment_1.csv' into table t5 fields terminated by ',';
select * from t5;

drop table t5;

drop table if exists t6;
create table t6(
col1 int auto_increment primary key,
col2 int,
col3 int
);

-- echo duplicate
-- @bvt:issue#3433
load data infile '$resources/load_data/auto_increment_2.csv' into table t6 fields terminated by ',';
select * from t6;
-- @bvt:issue
load data infile '$resources/load_data/auto_increment_2.csv' into table t6 FIELDS ESCAPED BY '\\' TERMINATED BY ',';
load data infile '$resources/load_data/auto_increment_2.csv' into table t6 FIELDS TERMINATED BY ',' LINES STARTING BY 'aaa';
drop table t6;

create table t7(
col1 int,
col2 int,
col3 int
);
load data infile '$resources/load_data/auto_increment_2.csv' into table t7 fields terminated by ',' parallel 'true';
select * from t7 order by col1;

drop table t7;

create table t8(a int, b int);
load data infile '$resources/load_data/auto_increment_20.csv' into table t7 fields terminated by ',' set col2=nullif(col2, '1');

create table t9(a varchar, b varchar, c varchar, d varchar);
load data infile {"filepath"="$resources/load_data/parallel.txt.gz", "compression"="gzip"} into table t9 FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t9;
load data infile {"filepath"="$resources/load_data/parallel.txt.gz", "compression"="gzip"} into table t9 FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' parallel 'true';
select * from t9;

create account if not exists `abc2` admin_name 'user' identified by '111';
-- @session:id=1&user=abc2:user:accountadmin&password=111
create database if not exists ssb;
use ssb;
create table test_table(
col1 int AUTO_INCREMENT,
col2 float,
col3 bool,
col4 Date,
col5 varchar(255),
col6 text,
PRIMARY KEY (`col1`)
);
load data infile '$resources/load_data/test_1.csv' into table test_table fields terminated by ',' parallel 'true';
select * from test_table;
drop table test_table;
drop database ssb;
-- @session
drop account `abc2`;

drop table if exists t1;
create table t1(
col1 char(225),
col2 varchar(225),
col3 text,
col4 varchar(225)
);

load data infile '$resources/load_data/char_varchar_5.csv' into table t1 fields terminated by'?';
delete from t1;
load data infile '$resources/load_data/char_varchar_5.csv' into table t1 fields terminated by'?';
delete from t1;
load data infile '$resources/load_data/char_varchar_5.csv' into table t1 fields terminated by'?';
delete from t1;
drop table t1;

drop table if exists t1;
create table t1 (col1 int);
load data infile '$resources/load_data/test_character.csv' into table t1 CHARACTER SET utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
load data infile '$resources/load_data/test_character.csv' into table t1 CHARACTER SET utf_8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
load data infile '$resources/load_data/test_character.csv' into table t1 CHARACTER SET gbk FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
load data infile '$resources/load_data/test_character.csv' into table t1 CHARACTER SET utf_16 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
load data infile '$resources/load_data/test_character.csv' into table t1 CHARACTER SET utf_xx FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
load data infile '$resources/load_data/test_character.csv' into table t1 CHARACTER SET "utf-xx" FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
load data infile '$resources/load_data/test_character.csv' into table t1 CHARACTER SET "utf-16" FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
drop table t1;

drop table if exists t1;
create table t1(col1 int, col2 varchar(10));
load data infile '$resources/load_data/test_character01.csv' into table t1 CHARACTER SET abcd FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
load data infile '$resources/load_data/test_character01.csv' into table t1 CHARACTER SET utf_8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
load data infile '$resources/load_data/test_character01.csv' into table t1 CHARACTER SET "utf-16" FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
load data infile '$resources/load_data/test_character01.csv' into table t1 CHARACTER SET "utf_xx" FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
select * from t1;
drop table t1;

drop table if exists test01;
create table test01(col1 int, col2 varchar(20));
load data infile '$resources/load_data/test_starting_by02.csv' into table test01 CHARACTER SET "utf_8" fields terminated by ',' lines starting by 'cha';
select * from test01;
drop table test01;

drop table if exists test02;
create table test02(col1 int, col2 bigint, col3 varchar(30));
load data infile '$resources/load_data/test_starting_by03.csv' into table test02 fields terminated by '|' lines starting by '1' terminated by '\n';
select * from test02;
drop table test02;

-- default starting by string ''
drop table if exists test03;
create table test03(col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_starting_by01.csv' into table test03 CHARACTER SET "utf_8" fields terminated by ',' lines terminated by '\n';
select * from test03;
drop table test03;

-- @bvt:issue#15084
drop table if exists test04;
create table test04 (col1 varchar(20), col2 varchar(60));
load data infile '$resources/load_data/test_escaped_by01.csv' into table test04 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from test04;
drop table test04;
-- @bvt:issue

drop table if exists test05;
create table test05 (col1 varchar(20), col2 varchar(60));
load data infile '$resources/load_data/test_escaped_by02.csv' into table test05 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from test05;
drop table test05;

-- @bvt:issue#15110
drop table if exists test06;
create table test06(col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_enclosed_by01.csv' into table test06 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from test06;
drop table test06;
-- @bvt:issue

drop table if exists test07;
create table test07(col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_enclosed_by02.csv' into table test07 fields terminated by ',' enclosed by '`' lines terminated by '\n';
select * from test07;
drop table test07;

-- @bvt:issue#15110
drop table if exists test08;
create table test08 (col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_enclosed_by01.csv' into table test08 fields terminated by ',' enclosed by '`' lines terminated by '\n';
select * from test08;
drop table test08;
-- @bvt:issue

drop table if exists test09;
create table test09(col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_starting_by04.csv' into table test09 CHARACTER SET "utf_8" fields terminated by ',' lines starting by ' ';
select * from test09;
drop table test09;