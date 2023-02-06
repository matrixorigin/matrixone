
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
load data infile '$resources/load_data/integer_numbers_1.csv' into table t1;
select * from t1;

-- into outfile
select * from t1 into outfile '$resources/into_outfile/outfile_integer_numbers_1.csv';
delete from t1;

-- load data
load data infile '$resources/into_outfile/outfile_integer_numbers_1.csv' into table t1 ignore 1 lines;
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
load data infile '$resources/load_data/char_varchar_1.csv' into table t2;
select * from t2;

-- into outfile
select * from t2 into outfile '$resources/into_outfile/outfile_char_varchar_1.csv';
delete from t2;

-- load data
load data infile '$resources/into_outfile/outfile_char_varchar_1.csv' into table t2 ignore 1 lines;
select * from t2;
delete from t2;

load data infile '$resources/load_data/char_varchar_2.csv' into table t2;
select * from t2;
delete from t2;


load data infile '$resources/load_data/char_varchar_3.csv' into table t2;
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
load data infile '$resources/load_data/float_1.csv' into table t3;
select * from t3;
delete from t3;

-- load data
load data infile '$resources/load_data/float_2.csv' into table t3;
select * from t3;

-- into outfile
select * from t3 into outfile '$resources/into_outfile/outfile_float_2.csv';
delete from t3;

-- load data
load data infile '$resources/into_outfile/outfile_float_2.csv' into table t3 ignore 1 lines;
select * from t3;
delete from t3;

load data infile '$resources/load_data/float_3.csv' into table t3;

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
load data infile '$resources/load_data/time_date_1.csv' into table t4;
select * from t4;
delete from t4;

-- load data
load data infile '$resources/load_data/time_date_2.csv' into table t4;
select * from t4;

-- into outfile
select * from t4 into outfile '$resources/into_outfile/outfile_time_date_2.csv';
delete from t4;

-- load data
load data infile '$resources/into_outfile/outfile_time_date_2.csv' into table t4 ignore 1 lines;
select * from t4;
delete from t4;

load data infile '$resources/load_data/time_date_3.csv' into table t4;
delete from t4;

load data infile '$resources/load_data/time_date_4.csv' into table t4 fields terminated by';';
select * from t4;
delete from t4;

load data infile '$resources/load_data/time_date_5.csv' into table t4;
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
load data infile '$resources/load_data/auto_increment_1.csv' into table t5;
select * from t5;

drop table t5;

drop table if exists t6;
create table t6(
col1 int auto_increment primary key,
col2 int,
col3 int
);

-- echo duplicate
load data infile '$resources/load_data/auto_increment_2.csv' into table t6;
select * from t6;

drop table t6;

create table t7(
col1 int,
col2 int,
col3 int
);
load data infile '$resources/load_data/auto_increment_2.csv' into table t7 parallel 'true';
select * from t7 order by col1;

drop table t7;