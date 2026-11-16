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

-- The invalid second row must reject the complete load, before narrowing.
create table bin_overflow(e enum('a','b') collate utf8mb4_bin);
load data infile '$resources/load_data/enum_collation_numeric_overflow.csv' into table bin_overflow fields terminated by ',' optionally enclosed by '"' parallel 'false' strict 'true';
select count(*) from bin_overflow;
create table general_overflow(e enum('a','b') collate utf8mb4_general_ci);
load data infile '$resources/load_data/enum_collation_numeric_overflow.csv' into table general_overflow fields terminated by ',' optionally enclosed by '"' parallel 'true' strict 'true';
select count(*) from general_overflow;
create table bin_zero(e enum('a','b') collate utf8mb4_bin);
load data infile '$resources/load_data/enum_collation_numeric_zero.csv' into table bin_zero fields terminated by ',' optionally enclosed by '"' parallel 'false' strict 'true';
select e+0,count(*) from bin_zero group by e+0 order by e+0;
load data infile '$resources/load_data/enum_collation_numeric_zero_padded.csv' into table bin_zero fields terminated by ',' optionally enclosed by '"' parallel 'true' strict 'true';
select e+0,count(*) from bin_zero group by e+0 order by e+0;
create table general_zero(e enum('a','b') collate utf8mb4_general_ci);
load data infile '$resources/load_data/enum_collation_numeric_zero_padded.csv' into table general_zero fields terminated by ',' optionally enclosed by '"' parallel 'false' strict 'true';
select e+0,count(*) from general_zero group by e+0 order by e+0;

-- Numeric text follows the same input-only ASCII space rule as SQL assignment.
create table bin_numeric_serial(e enum('a','b') collate utf8mb4_bin);
load data infile '$resources/load_data/enum_collation_numeric_padded.csv' into table bin_numeric_serial fields terminated by ',' optionally enclosed by '"' parallel 'false' strict 'true';
select e+0,count(*) from bin_numeric_serial group by e+0;
create table bin_numeric_parallel(e enum('a','b') collate utf8mb4_bin);
load data infile '$resources/load_data/enum_collation_numeric_padded.csv' into table bin_numeric_parallel fields terminated by ',' optionally enclosed by '"' parallel 'true' strict 'true';
select e+0,count(*) from bin_numeric_parallel group by e+0;
create table general_numeric(e enum('a','b') collate utf8mb4_general_ci);
load data infile '$resources/load_data/enum_collation_numeric_padded.csv' into table general_numeric fields terminated by ',' optionally enclosed by '"' parallel 'true' strict 'true';
select e+0,count(*) from general_numeric group by e+0;
create table numeric_label(e enum('2','x') collate utf8mb4_bin);
load data infile '$resources/load_data/enum_collation_numeric_padded.csv' into table numeric_label fields terminated by ',' optionally enclosed by '"' parallel 'false' strict 'true';
select e+0,count(*) from numeric_label group by e+0;
create table large_numeric_label(e enum('a','65537','x') collate utf8mb4_bin);
load data infile '$resources/load_data/enum_collation_numeric_overflow.csv' into table large_numeric_label fields terminated by ',' optionally enclosed by '"' parallel 'false' strict 'true';
select e+0 from large_numeric_label order by e+0;
load data infile '$resources/load_data/enum_collation_numeric_nbsp.csv' into table bin_numeric_serial fields terminated by ',' optionally enclosed by '"' parallel 'false' strict 'true';
select count(*) from bin_numeric_serial;

create table bin_parquet(e enum('a','A') collate utf8mb4_bin);
load data infile {'filepath'='$resources/load_data/enum_collation_valid.parquet','format'='parquet'} into table bin_parquet;
select hex(cast(e as char)),e+0 from bin_parquet order by e+0,cast(e as char);
load data infile {'filepath'='$resources/load_data/enum_collation_invalid_nbsp.parquet','format'='parquet'} into table bin_parquet;
select count(*) from bin_parquet;

set session sql_mode=@saved_sql_mode;
drop database enum_external_collation;
