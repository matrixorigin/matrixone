-- CSV and Parquet ENUM input must use the declared collation on original bytes.
drop database if exists enum_external_collation;
create database enum_external_collation;
use enum_external_collation;
set @saved_sql_mode=@@sql_mode;
set session sql_mode='STRICT_TRANS_TABLES';

create table bin_serial(e enum('a','A') collate utf8mb4_bin);
load data infile '$resources/load_data/enum_collation_valid.csv' into table bin_serial fields terminated by ',' parallel 'false' strict 'true';
select hex(cast(e as char)),e+0 from bin_serial order by e+0,cast(e as char);
create table bin_parallel(e enum('a','A') collate utf8mb4_bin);
load data infile '$resources/load_data/enum_collation_valid.csv' into table bin_parallel fields terminated by ',' parallel 'true' strict 'true';
select hex(cast(e as char)),e+0 from bin_parallel order by e+0,cast(e as char);
load data infile '$resources/load_data/enum_collation_invalid_nbsp.csv' into table bin_serial fields terminated by ',' parallel 'false' strict 'true';
select count(*) from bin_serial;
load data infile '$resources/load_data/enum_collation_invalid_nbsp.csv' into table bin_parallel fields terminated by ',' parallel 'true' strict 'true';
select count(*) from bin_parallel;
create table general_ci(e enum('a','b') collate utf8mb4_general_ci);
load data infile '$resources/load_data/enum_collation_ci.csv' into table general_ci fields terminated by ',' parallel 'false' strict 'true';
select hex(cast(e as char)),e+0 from general_ci;
load data infile '$resources/load_data/enum_collation_invalid_nbsp.csv' into table general_ci fields terminated by ',' parallel 'false' strict 'true';
select count(*) from general_ci;

create table bin_parquet(e enum('a','A') collate utf8mb4_bin);
load data infile {'filepath'='$resources/load_data/enum_collation_valid.parquet','format'='parquet'} into table bin_parquet;
select hex(cast(e as char)),e+0 from bin_parquet order by e+0,cast(e as char);
load data infile {'filepath'='$resources/load_data/enum_collation_invalid_nbsp.parquet','format'='parquet'} into table bin_parquet;
select count(*) from bin_parquet;

-- UCA zero-weight members can match an empty field without becoming NULL.
create table unicode_empty(e enum('́') collate utf8mb4_unicode_ci);
load data infile '$resources/load_data/enum_collation_empty_quoted.csv' into table unicode_empty fields terminated by ',' optionally enclosed by '"' parallel 'false' strict 'true';
select hex(cast(e as char)),e+0,e is null from unicode_empty;

set session sql_mode=@saved_sql_mode;
drop database enum_external_collation;
