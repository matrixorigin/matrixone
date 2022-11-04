--env prepare statement
drop table if exists ex_table_1;
drop table if exists ex_table_2_1;
drop table if exists ex_table_2_2;
drop table if exists ex_table_2_3;
drop table if exists ex_table_2_4;
drop table if exists ex_table_2_5;
drop table if exists ex_table_2_6;
drop table if exists ex_table_2_7;
drop table if exists ex_table_2_8;
drop table if exists ex_table_2_9;
drop table if exists ex_table_2_10;
drop table if exists ex_table_2_11;
drop table if exists ex_table_2_12;
drop table if exists ex_table_2_13;
drop table if exists ex_table_2_14;
drop table if exists ex_table_2_15;
drop table if exists ex_table_2_16;
drop table if exists ex_table_2_17;
drop table if exists ex_table_2_18;
drop table if exists ex_table_2_19;
drop table if exists ex_table_3;
drop table if exists ex_table_3_1;
drop table if exists ex_table_3_2;
drop table if exists ex_table_3_3;
drop table if exists ex_table_3_4;
drop table if exists ex_table_3_5;
drop table if exists ex_table_31;
drop table if exists ex_table_4;
drop table if exists ex_table_5;
drop table if exists ex_table_6;
drop table if exists ex_table_6a;
drop table if exists ex_table_7;
drop table if exists ex_table_8;
drop table if exists ex_table_9;
drop table if exists ex_table_10;
drop table if exists ex_table_10a;
drop table if exists ex_table_11;
drop table if exists ex_table_12;
drop table if exists ex_table_13;
drop table if exists ex_table_14;
drop table if exists ex_table_text;
drop table if exists ex_table_log;
drop table if exists ex_table_gzip;
drop table if exists ex_table_bzip2;
drop table if exists ex_table_lz4;
drop table if exists ex_table_auto;
drop table if exists ex_table_none;
drop table if exists ex_table_nocomp;
drop table if exists ex_table_cp1;
drop table if exists ex_table_cp2;
drop table if exists ex_table_15;
drop table if exists ex_table_drop;
drop table if exists table_15;
drop table if exists ex_table_yccs;

--覆盖各数值类型正常值,极值，空值
create external table ex_table_1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_number.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from  ex_table_1;

--覆盖各数值类型异常值：非法值中文字符特殊字符，超出范围的值
create external table ex_table_2_1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_1.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_1;
create external table ex_table_2_2(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_2.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_2;
create external table ex_table_2_3(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_3.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_3;
create external table ex_table_2_4(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_4.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_4;
create external table ex_table_2_5(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_5.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_5;
create external table ex_table_2_6(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_6.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_6;
create external table ex_table_2_7(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_7.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_7;
create external table ex_table_2_8(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_8.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_8;
create external table ex_table_2_9(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_9.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_9;
create external table ex_table_2_10(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_10.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_10;
create external table ex_table_2_11(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_11.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_11;
create external table ex_table_2_12(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_12.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_12;
create external table ex_table_2_13(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_13.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_13;
create external table ex_table_2_14(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_14.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_14;
create external table ex_table_2_15(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_15.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_15;
create external table ex_table_2_16(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_16.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_16;
create external table ex_table_2_17(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_17.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_17;
create external table ex_table_2_18(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_18.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_18;
create external table ex_table_2_19(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_19.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_2_19;

-- @bvt:issue#5442
--覆盖字符数字中文特殊字符
create external table ex_table_3(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_char.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_3;
--增加text类型

--覆盖非法值，超出范围值
create external table ex_table_3_1(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_3_1.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_3_1;
create external table ex_table_3_2(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_3_2.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_3_2;
create external table ex_table_3_3(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_3_3.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_3_3;
create external table ex_table_3_4(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_3_4.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_3_4;
create external table ex_table_3_5(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_3_5.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_3_5;
-- @bvt:issue

create external table ex_table_31(clo1 tinyint default 8,clo2 smallint null,clo3 int not null,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255),primary key(clo1))infile{"filepath"='$resources/external_table_file/ex_table_3_6.csv'};
select * from ex_table_31;

create external table ex_table_4(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_1.csv'} fields terminated by '|' enclosed by '\"' lines terminated by '\n';
select clo1,clo5,clo7,col12,col13,col14,col16,col17,col18 from ex_table_4;
create external table ex_table_5(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_2.csv'} fields terminated by '|' lines terminated by '\n';
select * from ex_table_5;
create external table ex_table_6(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_3.csv'} fields terminated by '*' enclosed by '\"' lines terminated by '\n';
select clo1,clo5,clo7,col12,col13,col14,col16,col17,col18  from ex_table_6;
create external table ex_table_6a(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_11.csv'} fields terminated by '\t' enclosed by '\"' lines terminated by '\n';
select clo1,clo5,clo7,col12,col13,col14,col16 from ex_table_6a;

--异常值分隔符封闭符#，\r,\n
create external table ex_table_7(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_4.csv'} fields terminated by '#' enclosed by '\"' lines terminated by '\n';
select * from ex_table_7;
create external table ex_table_8(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_5.csv'} fields terminated by '\n' enclosed by '\"' lines terminated by '\n';
select * from ex_table_8;
create external table ex_table_9(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_6.csv'} fields terminated by '\r' enclosed by '\"' lines terminated by '\n';
select * from ex_table_9;

--异常值字段数据包含封闭符
create external table ex_table_10(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_7.csv'} fields terminated by ''  lines terminated by '\n';
select * from ex_table_10;

--文件中分隔符和封闭符和create指定不匹配
create external table ex_table_10a(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_1.csv'} fields terminated by ',' enclosed by '@' lines terminated by '\n';
select * from ex_table_10a;
create external table ex_table_11(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_8.csv'} fields terminated by '|'  lines terminated by '\n';
select * from ex_table_11;

--缺省换行符
create external table ex_table_12(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_9.csv'} fields terminated by ','  enclosed by '\"' ;
select clo1,clo5,clo7,col12,col13,col14,col16,col17,col18  from ex_table_12;
--换行符为\r
create external table ex_table_13(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_10.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\r\n';
select clo1,clo5,clo7,col12,col13,col14,col16,col17,col18  from ex_table_13;
--缺省fields terminated ，terminated，ENCLOSED(默认分隔符，封闭符"")
create external table ex_table_14(clo1 tinyint primary key,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_10.csv'};
select clo1,clo5,clo7,col12,col13,col14,col16,col17,col18  from ex_table_14;
--text
-- @bvt:issue#5442
create external table ex_table_text(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_char.text'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_text;
--log
create external table ex_table_log(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_char.log'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_log;
-- @bvt:issue
--gzip
create external table ex_table_gzip(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_gzip.gz',"compression"='gzip'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_gzip;

--bzip2
-- @bvt:issue#5442
create external table ex_table_bzip2(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_bzip.bz2',"compression"='bz2'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_bzip2;
-- @bvt:issue
--lz4
create external table ex_table_lz4(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_lz4.lz4',"compression"='lz4'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_lz4;

--zib

--flate

--auto
create external table ex_table_auto(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_lz4.lz4',"compression"='auto'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_auto;

--none
create external table ex_table_none(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,
col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255)) infile{"filepath"='$resources/external_table_file/ex_table_sep_8.csv',"compression"='none'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select clo1,clo5,clo7,col12,col13,col14,col16,col17,col18  from ex_table_none;

--缺省compression
-- @bvt:issue#5442
create external table ex_table_nocomp(char_1 char(20),char_2 varchar(10),date_1 timestamp,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_bzip.bz2'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_nocomp;
-- @bvt:issue
--异常：压缩格式不对应，未压缩文件
create external table ex_table_cp1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_gzip.gz',"compression"='lz4'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_cp1;
create external table ex_table_cp2(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_number.csv',"compression"='lz4'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_cp2;

--insert into internal table select from external table
create external table ex_table_15(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_1.csv'} fields terminated by '|' enclosed by '' lines terminated by '\n';
--create table table_15 as select * from ex_table_15;
create table table_15(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255));
insert into table_15 select * from  ex_table_15;
select clo1,clo5,clo7,col12,col13,col14,col16,col17,col18  from table_15;

--drop外部表后再创建
create external table ex_table_drop(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_1.csv'} fields terminated by '|' enclosed by '' lines terminated by '\n';
drop table ex_table_drop;
create external table ex_table_drop(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/ex_table_sep_1.csv'} fields terminated by '|' enclosed by '' lines terminated by '\n';
select clo1,clo5,clo7,col12,col13,col14,col16,col17,col18  from ex_table_drop;

--内部表和外部表关联
-- @bvt:issue#6171
select count(*) from ex_table_15,table_15 where ex_table_15.clo1=table_15.clo1;
-- @bvt:issue
--异常测试:insert/update/delete
create external table ex_table_yccs(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp)infile{"filepath"='$resources/external_table_file/ex_table_char.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
insert into ex_table_yccs select 'yellow','apple','2020-09-30','2020-09-30 10:20:08','2020-09-30 10:20:08.09834';
update ex_table_yccs set char_1="cat123";
delete from ex_table_yccs;

drop table if exists ex_table_1;
drop table if exists ex_table_2_1;
drop table if exists ex_table_2_2;
drop table if exists ex_table_2_3;
drop table if exists ex_table_2_4;
drop table if exists ex_table_2_5;
drop table if exists ex_table_2_6;
drop table if exists ex_table_2_7;
drop table if exists ex_table_2_8;
drop table if exists ex_table_2_9;
drop table if exists ex_table_2_10;
drop table if exists ex_table_2_11;
drop table if exists ex_table_2_12;
drop table if exists ex_table_2_13;
drop table if exists ex_table_2_14;
drop table if exists ex_table_2_15;
drop table if exists ex_table_2_16;
drop table if exists ex_table_2_17;
drop table if exists ex_table_2_18;
drop table if exists ex_table_2_19;
drop table if exists ex_table_3;
drop table if exists ex_table_3_1;
drop table if exists ex_table_3_2;
drop table if exists ex_table_3_3;
drop table if exists ex_table_3_4;
drop table if exists ex_table_3_5;
drop table if exists ex_table_31;
drop table if exists ex_table_4;
drop table if exists ex_table_5;
drop table if exists ex_table_6;
drop table if exists ex_table_6a;
drop table if exists ex_table_7;
drop table if exists ex_table_8;
drop table if exists ex_table_9;
drop table if exists ex_table_10;
drop table if exists ex_table_10a;
drop table if exists ex_table_11;
drop table if exists ex_table_12;
drop table if exists ex_table_13;
drop table if exists ex_table_14;
drop table if exists ex_table_text;
drop table if exists ex_table_log;
drop table if exists ex_table_gzip;
drop table if exists ex_table_bzip2;
drop table if exists ex_table_lz4;
drop table if exists ex_table_auto;
drop table if exists ex_table_none;
drop table if exists ex_table_nocomp;
drop table if exists ex_table_cp1;
drop table if exists ex_table_cp2;
drop table if exists ex_table_15;
drop table if exists ex_table_drop;
drop table if exists table_15;
drop table if exists ex_table_yccs;
