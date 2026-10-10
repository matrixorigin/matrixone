-- Regression coverage for Parquet SELECT INTO OUTFILE.
drop database if exists parquet_outfile_regression;
create database parquet_outfile_regression;
use parquet_outfile_regression;

drop stage if exists parquet_outfile_stage;
create stage parquet_outfile_stage URL='file://$resources/into_outfile/parquet_outfile_regression';

-- A single executor batch must still honor a small split target and preserve every row.
create table split_src (id int, payload varchar(600));
insert into split_src
select result, repeat('x', 512) from generate_series(1, 32) g;

-- A failed CSV export must not poison the next Parquet export on this connection.
select id, payload from split_src
into outfile 'stage://parquet_outfile_stage/failed.csv'
format 'csv' splitsize '1' header 'true';

select id, payload from split_src order by id
into outfile 'stage://parquet_outfile_stage/split_%d.parquet'
format 'parquet' splitsize '8k';
select count(*) > 1 as split_ok
from stage_list('stage://parquet_outfile_stage/split_*.parquet');
create table split_copy (id int, payload varchar(600));
load data infile {'filepath'='stage://parquet_outfile_stage/split_*.parquet', 'format'='parquet'} into table split_copy;
select count(*), min(id), max(id), min(length(payload)), max(length(payload)) from split_copy;

-- An empty result still creates a readable Parquet file.
create table empty_src (id int, payload varchar(20));
select * from empty_src into outfile 'stage://parquet_outfile_stage/empty.parquet' format 'parquet';
create table empty_copy (id int, payload varchar(20));
load data infile {'filepath'='stage://parquet_outfile_stage/empty.parquet', 'format'='parquet'} into table empty_copy;
select count(*) from empty_copy;

-- Documented scalar and vector types must survive Parquet export and LOAD DATA.
create table typed_src (
    id bigint unsigned,
    bits bit(8),
    tags set('a', 'b', 'c'),
    v32 vecf32(2),
    v64 vecf64(2),
    year_val year,
    ts timestamp
);
set @parquet_old_time_zone = @@time_zone;
set time_zone = '+08:00';
insert into typed_src values (18446744073709551615, b'10101010', 'a,b', '[1.25,2.5]', '[3.5,4.75]', 2024, '2024-01-01 00:00:00');
select * from typed_src into outfile 'stage://parquet_outfile_stage/typed.parquet' format 'parquet';
set time_zone = '-05:00';
create table typed_copy (
    id bigint unsigned,
    bits bit(8),
    tags set('a', 'b', 'c'),
    v32 vecf32(2),
    v64 vecf64(2),
    year_val year,
    ts timestamp
);
load data infile {'filepath'='stage://parquet_outfile_stage/typed.parquet', 'format'='parquet'} into table typed_copy;
select id, cast(bits as unsigned), tags, vector_dims(v32), vector_dims(v64), year_val, unix_timestamp(ts) from typed_copy;
set time_zone = @parquet_old_time_zone;

-- Duplicate aliases must fail instead of silently dropping a Parquet column.
select id as duplicate_name, id as duplicate_name from split_src
into outfile 'stage://parquet_outfile_stage/duplicate.parquet' format 'parquet';

remove files from stage if exists 'stage://parquet_outfile_stage/*';
drop table split_copy;
drop table split_src;
drop table empty_copy;
drop table empty_src;
drop table typed_copy;
drop table typed_src;
drop stage parquet_outfile_stage;
drop database parquet_outfile_regression;
