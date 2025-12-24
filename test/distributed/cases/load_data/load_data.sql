
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
load data infile '$resources/load_data/integer_numbers_4.csv' into table t1 fields terminated by ',';
select * from t1;
delete from t1;

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

drop table if exists test04;
create table test04 (col1 varchar(20), col2 varchar(60));
load data infile '$resources/load_data/test_escaped_by01.csv' into table test04 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from test04;
drop table test04;

drop table if exists test05;
create table test05 (col1 varchar(20), col2 varchar(60));
load data infile '$resources/load_data/test_escaped_by02.csv' into table test05 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from test05;
drop table test05;


drop table if exists test06;
create table test06(col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_enclosed_by01.csv' into table test06 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from test06;
load data local infile '$resources/load_data/test_enclosed_by01.csv' into table test06 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from test06;
delete from test06;
load data local infile {"filepath"="$resources/load_data/test_enclosed_by01.csv", "compression"='', "format"='csv'}  into table test06 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from test06;
drop table test06;

drop table if exists test07;
create table test07(col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_enclosed_by02.csv' into table test07 fields terminated by ',' enclosed by '`' lines terminated by '\n';
select * from test07;
drop table test07;

drop table if exists test08;
create table test08 (col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_enclosed_by01.csv' into table test08 fields terminated by ',' enclosed by '`' lines terminated by '\n';
select * from test08;
drop table test08;


drop table if exists test09;
create table test09(col1 varchar(20), col2 varchar(20));
load data infile '$resources/load_data/test_starting_by04.csv' into table test09 CHARACTER SET "utf_8" fields terminated by ',' lines starting by ' ';
select * from test09;
drop table test09;

drop table if exists test10;
create table test10(col1 text, col2 text);
load data infile {'filepath'='$resources/load_data/text.csv.tar.gz', 'compression'='tar.gz'} into table test10 FIELDS  ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' parallel 'true';
select * from test10;

load data infile {'filepath'='$resources/load_data/text.csv.tar.bz2', 'compression'='tar.bz2'} into table test10 FIELDS  ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' parallel 'true';
select * from test10;
drop table test10;

drop table if exists load_data_t1;
CREATE TABLE load_data_t1 (
`name` VARCHAR(255) DEFAULT null,
`age` INT DEFAULT null,
`city` VARCHAR(255) DEFAULT null
);
load data inline format='csv', data=$XXX$ zhangsan,26,XiAn $XXX$ into table load_data_t1 fields terminated by ',' lines terminated by '\r\n' (city,age,name);
select * from load_data_t1;
delete from load_data_t1;
load data inline format='csv', data=$XXX$ XiAn,26 $XXX$ into table load_data_t1 fields terminated by ',' lines terminated by '\r\n' (city,age);
select * from load_data_t1;
delete from load_data_t1;
load data inline format='csv', data=$XXX$ zhangsan, XiAn,26 $XXX$ into table load_data_t1 fields terminated by ',' lines terminated by '\r\n' (@name, city,age);
select * from load_data_t1;
delete from load_data_t1;
load data inline format='csv', data=$XXX$ zhangsan, XiAn,26 $XXX$ into table load_data_t1 fields terminated by ',' lines terminated by '\r\n' (@aa, city,age);
select * from load_data_t1;
delete from load_data_t1;
load data inline format='csv', data=$XXX$ zhangsan, XiAn,26 $XXX$ into table load_data_t1 fields terminated by ',' lines terminated by '\r\n' (city,age);
load data inline format='csv', data=$XXX$ zhangsan $XXX$ into table load_data_t1 fields terminated by ',' lines terminated by '\r\n' (city,age);
drop table load_data_t1;

drop table if exists load_data_t2;
create table load_data_t2(id int, name varchar(20), age int);
LOAD DATA infile '$resources/load_data/test_columnlist_01.csv' into table load_data_t2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (id,   name , age);
select * from load_data_t2 order by id;
delete from load_data_t2;

LOAD DATA infile '$resources/load_data/test_columnlist_01.csv' into table load_data_t2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (name, id, age);


LOAD DATA infile '$resources/load_data/test_columnlist_01.csv' into table load_data_t2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (id,   name );
select * from load_data_t2 order by id;
delete from load_data_t2;

LOAD DATA infile '$resources/load_data/test_columnlist_01.csv' into table load_data_t2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (id,   name , age, address);


LOAD DATA infile '$resources/load_data/test_columnlist_02.csv' into table load_data_t2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (id, name, age);
select * from load_data_t2;
delete from load_data_t2;

LOAD DATA infile '$resources/load_data/test_columnlist_02.csv' into table load_data_t2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (id, name);
select * from load_data_t2 order by id;
delete from load_data_t2;

LOAD DATA infile '$resources/load_data/test_columnlist_02.csv' into table load_data_t2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (age, name);
select * from load_data_t2 order by id;
drop table load_data_t2;

drop table if exists load_data_t3;
create table load_data_t3(col1 int not null, col2 varchar(20) default "jane", col3 int);
LOAD DATA infile '$resources/load_data/test_columnlist_02.csv' into table load_data_t3 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (col3, col2);

LOAD DATA infile '$resources/load_data/test_columnlist_03.csv' into table load_data_t3 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (col1, col3);
select * from load_data_t3 order by id;
drop table load_data_t3;

drop table if exists load_data_t4;
create table load_data_t4(col1 int default 22, col2 int);
LOAD DATA infile '$resources/load_data/test_columnlist_03.csv' into table load_data_t4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (col1, @col2);
select * from load_data_t4;
delete from load_data_t4;
LOAD DATA infile '$resources/load_data/test_columnlist_03.csv' into table load_data_t4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (@col1, col2);
select * from load_data_t4;
delete from load_data_t4;

LOAD DATA infile '$resources/load_data/test_columnlist_04.csv' into table load_data_t4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (@col1, col2);
drop table load_data_t4;

drop table if exists load_data_t5;
create table load_data_t5(col1 int, col2 int, col3 int);
LOAD DATA local infile '$resources/load_data/test_columnlist_04.csv' into table load_data_t5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (@col1, col2);
select * from load_data_t5;
delete from load_data_t5;
LOAD DATA local infile '$resources/load_data/test_columnlist_04.csv' into table load_data_t5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (col1, col2);
select * from load_data_t5;
delete from load_data_t5;
LOAD DATA local infile '$resources/load_data/test_columnlist_04.csv' into table load_data_t5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
select * from load_data_t5;
delete from load_data_t5;
LOAD DATA local infile '$resources/load_data/test_columnlist_04.csv' into table load_data_t5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'(col1, @col2);
select * from load_data_t5;
delete from load_data_t5;
LOAD DATA local infile '$resources/load_data/test_columnlist_03.csv' into table load_data_t5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
select * from load_data_t5;
delete from load_data_t5;
LOAD DATA local infile '$resources/load_data/test_columnlist_03.csv' into table load_data_t5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'(@col1, col2);
select * from load_data_t5;
drop table load_data_t5;

drop table if exists load_data_t6;
create table load_data_t6(col1 int);
LOAD DATA local infile '$resources/load_data/test_columnlist_03.csv' into table load_data_t6 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
select * from load_data_t6;
drop table load_data_t6;

drop table if exists load_data_t7;
create table load_data_t7 (col1 varchar(20), col2 varchar(20), col3 varchar(20));
load data local infile '$resources/load_data/test_escaped_by03.csv' into table load_data_t7 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from load_data_t7;
delete from load_data_t7;
load data infile '$resources/load_data/test_escaped_by03.csv' into table load_data_t7 fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select * from load_data_t7;
drop table load_data_t7;

drop account if exists test_load;
create account test_load ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=5&user=test_load:admin&password=123456
show session variables like 'sql_mode';
set session sql_mode = "NO_ENGINE_SUBSTITUTION";
show session variables like 'sql_mode';
create database test_load_db;
use test_load_db;
drop table if exists load_data_t8;
create table load_data_t8(col1 int);
LOAD DATA infile '$resources/load_data/test_columnlist_03.csv' into table load_data_t8 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
select * from load_data_t8;
set session sql_mode = "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES";
LOAD DATA infile '$resources/load_data/test_columnlist_03.csv' into table load_data_t8 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
drop table load_data_t8;
drop database test_load_db;

-- @session
drop account test_load;

drop table if exists load_data_t9;
create table load_data_t9(col1 int, col2 varchar(100), col3 varchar(100));
load data infile '$resources/load_data/test_parallel.csv' into table load_data_t9 fields terminated by ',' parallel 'true';
select count(*) from load_data_t9;
delete from load_data_t9;
load data infile '$resources/load_data/test_parallel.csv' into table load_data_t9 fields terminated by ',' ignore 99999 lines parallel 'true';
select count(*) from load_data_t9;
delete from load_data_t9;
load data infile '$resources/load_data/test_parallel.csv' into table load_data_t9 fields terminated by ',' ignore 9999 lines parallel 'true';
select count(*) from load_data_t9;
delete from load_data_t9;
load data infile '$resources/load_data/test_parallel.csv' into table load_data_t9 fields terminated by ',' enclosed by '"' LINES TERMINATED BY '\n' parallel 'true';
select count(*) from load_data_t9;
delete from load_data_t9;
load data infile '$resources/load_data/test_parallel.csv' into table load_data_t9 fields terminated by ',' parallel 'true' strict 'true';
select count(*) from load_data_t9;
delete from load_data_t9;
load data infile '$resources/load_data/test_parallel.csv' into table load_data_t9 fields terminated by ',' parallel 'true' strict 'false';
select count(*) from load_data_t9;
delete from load_data_t9;

load data infile {'filepath'='$resources/load_data/test_parallel_gz.csv.gz'} into table load_data_t9 FIELDS  ENCLOSED BY '"' TERMINATED BY "," LINES TERMINATED BY '\n' parallel 'true';
select count(*) from load_data_t9;

drop table load_data_t9;

drop table if exists load_data_type;
create table load_data_type(col1 int);

load data infile '$resources/load_data/test_int.csv' into table load_data_type fields terminated by ',';
load data infile '$resources/load_data/test_int.csv' into table load_data_type fields terminated by ',' ignore 1 lines;
select * from load_data_type;
drop table load_data_type;

drop table if exists load_data_0303;
CREATE TABLE load_data_0303 (
  mandt VARCHAR(3) NOT NULL COMMENT '客户端',
  vbeln VARCHAR(10) NOT NULL COMMENT '销售凭证号',
  zfkb1 DECIMAL(5,3) COMMENT '折扣比例1',
  zfkb2 DECIMAL(5,3) COMMENT '折扣比例2',
  zfkb3 DECIMAL(5,3) COMMENT '折扣比例3',
  zfkb4 DECIMAL(5,3) COMMENT '折扣比例4',
  zfkb5 DECIMAL(5,3) COMMENT '折扣比例5',
  zfkb6 DECIMAL(5,3) COMMENT '折扣比例6',
  zfkb7 DECIMAL(5,3) COMMENT '折扣比例7',
  zfkb8 DECIMAL(5,3) COMMENT '折扣比例8',
  zfkb9 DECIMAL(5,3) COMMENT '折扣比例9',
  zfkd1 VARCHAR(10) COMMENT '折扣代码1',
  zfkd2 VARCHAR(10) COMMENT '折扣代码2',
  zfkd3 VARCHAR(10) COMMENT '折扣代码3',
  zfkd4 VARCHAR(10) COMMENT '折扣代码4',
  zfkd5 VARCHAR(10) COMMENT '折扣代码5',
  zfkd6 VARCHAR(10) COMMENT '折扣代码6',
  zfkd7 VARCHAR(10) COMMENT '折扣代码7',
  zfkd8 VARCHAR(10) COMMENT '折扣代码8',
  zfkd9 VARCHAR(10) COMMENT '折扣代码9',
  zfkp1 VARCHAR(1) COMMENT '折扣标识1',
  zfkp2 VARCHAR(1) COMMENT '折扣标识2',
  zfkp3 VARCHAR(1) COMMENT '折扣标识3',
  zfkp4 VARCHAR(1) COMMENT '折扣标识4',
  zfkp5 VARCHAR(1) COMMENT '折扣标识5',
  zfkp6 VARCHAR(1) COMMENT '折扣标识6',
  zfkp7 VARCHAR(1) COMMENT '折扣标识7',
  zfkp8 VARCHAR(1) COMMENT '折扣标识8',
  zfkp9 VARCHAR(1) COMMENT '折扣标识9',
  zterm VARCHAR(4) COMMENT '付款条件',
  xsplt VARCHAR(1) COMMENT '销售拆分标识',
  xt000 VARCHAR(1) COMMENT '特殊标识',
  htqrs VARCHAR(1) COMMENT '合同确认标识',
  qrszje DECIMAL(15,2) COMMENT '确认总金额',
  kxfje DECIMAL(15,2) COMMENT '可销金额',
  bkxfje DECIMAL(15,2) COMMENT '不可销金额',
  jsjz DECIMAL(15,2) COMMENT '结算价值',
  xszk DECIMAL(5,2) COMMENT '销售折扣',
  bzf DECIMAL(15,2) COMMENT '保证费',
  gsbjf DECIMAL(15,2) COMMENT '公司拨金额',
  xhsh DECIMAL(15,2) COMMENT '销货赊回',
  xhjy DECIMAL(15,2) COMMENT '销货交易',
  xhpcb DECIMAL(15,2) COMMENT '销货赔偿拨',
  ckfjf DECIMAL(15,2) COMMENT '出口附加费',
  ysf1 DECIMAL(15,2) COMMENT '运输费1',
  ysf2 DECIMAL(15,2) COMMENT '运输费2',
  zbfwf DECIMAL(15,2) COMMENT '资本服务费',
  sjzzf DECIMAL(15,2) COMMENT '实际支付总费',
  wwjgf DECIMAL(15,2) COMMENT '未完工交付费',
  azf1 DECIMAL(15,2) COMMENT '安装费1',
  xjkcf DECIMAL(15,2) COMMENT '现金扣除费',
  khkcf DECIMAL(15,2) COMMENT '客户扣除费',
  syjcf DECIMAL(15,2) COMMENT '商业检查费',
  jsfwb DECIMAL(5,2) COMMENT '技术服务比例',
  jsfwf DECIMAL(15,2) COMMENT '技术服务费',
  shfy DECIMAL(15,2) COMMENT '审核费用',
  crmxm VARCHAR(10) COMMENT 'CRM项目',
  crmht VARCHAR(10) COMMENT 'CRM合同',
  zftdb VARCHAR(1) COMMENT '支付通道',
  zftbl DECIMAL(5,2) COMMENT '支付比例',
  zdkdb VARCHAR(1) COMMENT '折扣通道',
  zftr1 VARCHAR(8) COMMENT '支付期限1 (YYYYMMDD)',
  zftr2 VARCHAR(8) COMMENT '支付期限2 (YYYYMMDD)',
  zftr3 VARCHAR(8) COMMENT '支付期限3 (YYYYMMDD)',
  zftb1 DECIMAL(5,2) COMMENT '支付比例1',
  zftb2 DECIMAL(5,2) COMMENT '支付比例2',
  zftb3 DECIMAL(5,2) COMMENT '支付比例3',
  zvkgrp VARCHAR(3) COMMENT '销售组',
  jssyr VARCHAR(12) COMMENT '结算所有人',
  xclear VARCHAR(1) COMMENT '清算标识',
  uname VARCHAR(12) COMMENT '用户名',
  udate VARCHAR(8) COMMENT '更新日期 (YYYYMMDD)',
  utime VARCHAR(6) COMMENT '更新时间 (HHMMSS)',
  zwwglf DECIMAL(5,2) COMMENT '未完工利率',
  zazglf DECIMAL(5,2) COMMENT '安装管理利率',
  zfksh VARCHAR(1) COMMENT '付款审核标识',
  zfkt1 VARCHAR(4) COMMENT '付款条件1',
  zfkt2 VARCHAR(4) COMMENT '付款条件2',
  zfkt3 VARCHAR(4) COMMENT '付款条件3',
  zfkt4 VARCHAR(4) COMMENT '付款条件4',
  zfkt5 VARCHAR(4) COMMENT '付款条件5',
  zfkt6 VARCHAR(4) COMMENT '付款条件6',
  zfkt7 VARCHAR(4) COMMENT '付款条件7',
  zfkt8 VARCHAR(4) COMMENT '付款条件8',
  zfkt9 VARCHAR(4) COMMENT '付款条件9',
  qrsdat VARCHAR(8) COMMENT '确认日期 (YYYYMMDD)',
  dlaz DECIMAL(15,2) COMMENT '代理安装费',
  zzcbl DECIMAL(5,2) COMMENT '总成本比例',
  zcbbl DECIMAL(5,2) COMMENT '成本比例',
  zydjf VARCHAR(1) COMMENT '重要计费标识',
  zscbjd VARCHAR(8) COMMENT '生产报价单 (YYYYMMDD)',
  jssbhj DECIMAL(15,2) COMMENT '结算拨回金额',
  xszhf DECIMAL(15,2) COMMENT '销售折扣费',
  xsyjj DECIMAL(15,2) COMMENT '销售佣金价',
  xszhbgf DECIMAL(15,2) COMMENT '销售折扣保管费',
  jtxsyj DECIMAL(15,2) COMMENT '具体销售佣金',
  jtxsdj DECIMAL(15,2) COMMENT '具体销售单价',
  zxsfwf DECIMAL(15,2) COMMENT '最新服务费',
  zydbm VARCHAR(10) COMMENT '重要部门',
  zznrl VARCHAR(40) COMMENT '总内容量',
  zgfrl VARCHAR(40) COMMENT '供方容量',
  zgfrldw VARCHAR(3) COMMENT '供方容量单位',
  zbkbl VARCHAR(10) COMMENT '折扣比例备份',
  PRIMARY KEY (mandt, vbeln)
);

LOAD DATA INFILE '$resources/load_data/test_parse_newline.csv' INTO TABLE load_data_0303 FIELDS TERMINATED BY ','  lines terminated by '\r\n';
select count(*) from load_data_0303;
delete from load_data_0303;
LOAD DATA INFILE '$resources/load_data/test_parse_newline.csv' INTO TABLE load_data_0303 FIELDS TERMINATED BY ','  lines terminated by '\n';
select count(*) from load_data_0303;
delete from load_data_0303;
LOAD DATA INFILE '$resources/load_data/test_parse_newline.csv' INTO TABLE load_data_0303 FIELDS TERMINATED BY ',';
select count(*) from load_data_0303;
drop table load_data_0303;

drop database if exists test;
create database test;
use test;

create table t1(a bigint primary key, b vecf32(3), c varchar(10), d double);

insert into t1 values(1, "[-1,-1,-1]", "aaaa", 1.213);
insert into t1 values(2, "[-1, -1, 0]", "bbbb", 11.213);
insert into t1 values(3, "[1,1,1]", "cccc", 111.213);

select count(*) from t1;

select * from t1 into outfile '$resources/into_outfile/load_data/t1.csv';
truncate table t1;
load data infile '$resources/into_outfile/load_data/t1.csv' into table t1 ignore 1 lines;
select count(*) from t1;


create table t2 (a int primary key, b json);

insert into t2 values(1, '{"key1":"你好\\t不\\r好\\f呀\\n\\\\"}');
insert into t2 values(2, '{"key2":"谢谢\\t你，\\r我非常\\f好\\n\\\\"}');
select * from t2 order by a asc;

select * from t2 into outfile '$resources/into_outfile/load_data/t2.csv';
truncate table t2;

load data infile '$resources/into_outfile/load_data/t2.csv' into table t2 ignore 1 lines;
select * from t2 order by a asc;

drop database test;