-- Parquet LIST -> vector loads must reject NaN/Inf elements the same way
-- the vector SQL input path does (issue #31508).
drop database if exists parquet_vec_nonfinite;
create database parquet_vec_nonfinite;
use parquet_vec_nonfinite;
create table f16(id bigint primary key, v vecf16(2));
create table bf16(id bigint primary key, v vecbf16(2));
create table f32(id bigint primary key, v vecf32(2));
create table f64(id bigint primary key, v vecf64(2));

-- SQL input path rejects these values
insert into f16 values(9, '[100000,100000]');
insert into bf16 values(9, '[3.4e38,3.4e38]');
insert into f32 values(9, '[NaN,1]');
insert into f64 values(9, '[Inf,1]');

-- Parquet input path must reject them too
load data infile {'filepath'='$resources/parquet_vector_nonfinite/f16_overflow.parquet', 'format'='parquet'} into table f16;
load data infile {'filepath'='$resources/parquet_vector_nonfinite/bf16_overflow.parquet', 'format'='parquet'} into table bf16;
load data infile {'filepath'='$resources/parquet_vector_nonfinite/f32_nan.parquet', 'format'='parquet'} into table f32;
load data infile {'filepath'='$resources/parquet_vector_nonfinite/f64_inf.parquet', 'format'='parquet'} into table f64;
select count(*) from f16;
select count(*) from bf16;
select count(*) from f32;
select count(*) from f64;

-- finite values still load
load data infile {'filepath'='$resources/parquet_vector_nonfinite/finite.parquet', 'format'='parquet'} into table f16;
load data infile {'filepath'='$resources/parquet_vector_nonfinite/finite.parquet', 'format'='parquet'} into table f32;
select id, v from f16 order by id;
select id, v from f32 order by id;

drop database parquet_vec_nonfinite;
