-- Parquet DATE/TIMESTAMP values beyond year 9999 are rejected like SQL
-- assignment, and the whole file is not loaded (issue #31519).
drop database if exists parquet_temporal_range;
create database parquet_temporal_range;
use parquet_temporal_range;
set time_zone = '+00:00';
create table ts(id bigint primary key, v timestamp(6));
create table dt(id bigint primary key, v datetime(6));
create table d(id bigint primary key, v date);
insert into ts values (99, '10000-01-01 00:00:00');
load data infile {'filepath'='$resources/parquet_temporal_range/ts_year10000.parquet', 'format'='parquet'} into table ts;
load data infile {'filepath'='$resources/parquet_temporal_range/ts_year10000.parquet', 'format'='parquet'} into table dt;
load data infile {'filepath'='$resources/parquet_temporal_range/date_year10000.parquet', 'format'='parquet'} into table d;
load data infile {'filepath'='$resources/parquet_temporal_range/date_year10000.parquet', 'format'='parquet'} into table dt;
select count(*) from ts;
select count(*) from dt;
select count(*) from d;
load data infile {'filepath'='$resources/parquet_temporal_range/ts_max.parquet', 'format'='parquet'} into table ts;
load data infile {'filepath'='$resources/parquet_temporal_range/date_max.parquet', 'format'='parquet'} into table d;
select id, cast(v as char) from ts order by id;
select id, v from d order by id;
set time_zone = 'SYSTEM';
drop database parquet_temporal_range;
