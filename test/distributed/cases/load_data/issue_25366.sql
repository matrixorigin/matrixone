drop database if exists issue_25366_load_data_local_adjusted;
create database issue_25366_load_data_local_adjusted;
use issue_25366_load_data_local_adjusted;

set sql_mode = '';
create table t_load(
  id int primary key,
  i int,
  d decimal(5,2),
  dt date,
  vc varchar(4),
  nullable_i int
);

load data local infile '$resources/load_data/issue_25366_load_data_local_adjusted.csv'
into table t_load
fields terminated by ',' lines terminated by '\n';

select id, i, d, dt, vc, hex(vc), nullable_i from t_load order by id;

set @old_time_zone = @@time_zone;
set time_zone = '+00:00';
create table t_load_temporal(
  id int primary key,
  d date,
  dt datetime,
  ts timestamp
);

load data local infile '$resources/load_data/issue_25366_load_data_local_temporal.csv'
into table t_load_temporal
fields terminated by ',' lines terminated by '\n';
select id, d, dt, ts from t_load_temporal order by id;

set sql_mode = 'STRICT_TRANS_TABLES';
truncate table t_load_temporal;
load data local infile '$resources/load_data/issue_25366_load_data_local_temporal.csv'
into table t_load_temporal
fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*) as strict_invalid_date_rows from t_load_temporal;

truncate table t_load_temporal;
load data local infile '$resources/load_data/issue_25366_load_data_local_temporal.csv'
into table t_load_temporal
fields terminated by ',' lines terminated by '\n' ignore 2 lines;
select count(*) as strict_invalid_datetime_rows from t_load_temporal;

truncate table t_load_temporal;
load data local infile '$resources/load_data/issue_25366_load_data_local_temporal.csv'
into table t_load_temporal
fields terminated by ',' lines terminated by '\n' ignore 3 lines;
select count(*) as strict_invalid_timestamp_rows from t_load_temporal;

set sql_mode = '';
set time_zone = @old_time_zone;

set sql_mode = 'STRICT_TRANS_TABLES';
truncate table t_load;
load data local infile '$resources/load_data/issue_25366_load_data_local_adjusted.csv'
into table t_load
fields terminated by ',' lines terminated by '\n';
select count(*) from t_load;

set sql_mode = '';
truncate table t_load;
load data infile '$resources/load_data/issue_25366_load_data_local_adjusted.csv'
into table t_load
fields terminated by ',' lines terminated by '\n';
select count(*) from t_load;

create table t_quoted_int(id int primary key, i int);
load data local infile '$resources/load_data/issue_25366_quoted_invalid_int.csv'
into table t_quoted_int
fields terminated by ',' enclosed by '"' lines terminated by '\n';
select count(*) from t_quoted_int;

set sql_mode = 'STRICT_TRANS_TABLES';
truncate table t_quoted_int;
load data local infile '$resources/load_data/issue_25366_quoted_invalid_int.csv'
into table t_quoted_int
fields terminated by ',' enclosed by '"' lines terminated by '\n';
select count(*) from t_quoted_int;

set sql_mode = '';
truncate table t_quoted_int;
load data infile '$resources/load_data/issue_25366_quoted_invalid_int.csv'
into table t_quoted_int
fields terminated by ',' enclosed by '"' lines terminated by '\n';
select count(*) from t_quoted_int;

create table t_quoted_decimal(id int primary key, d decimal(5,2));
load data local infile '$resources/load_data/issue_25366_quoted_invalid_decimal.csv'
into table t_quoted_decimal
fields terminated by ',' enclosed by '"' lines terminated by '\n';
select count(*) from t_quoted_decimal;

set sql_mode = default;
drop table t_quoted_decimal;
drop table t_quoted_int;
drop table t_load_temporal;
drop table t_load;
drop database issue_25366_load_data_local_adjusted;
