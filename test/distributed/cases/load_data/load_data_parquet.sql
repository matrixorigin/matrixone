-- pre
drop database if exists parq;
create database parq;
use parq;

-- read from an already exported file
create table t1(id bigint,name varchar);
load data infile {'filepath'='$resources/load_data/simple.parq', 'format'='parquet'} into table t1;
select * from t1;

create table t2(id bigint not null, name varchar not null, sex bool, f32 float(5,2));
load data infile {'filepath'='$resources/load_data/simple2.parq', 'format'='parquet'} into table t2;
select * from t2;

create table t3(c varchar);
load data infile {'filepath'='$resources/load_data/indexed_str.parq', 'format'='parquet'} into table t3;
select * from t3;


create table t4(id bigint not null, name varchar not null, sex bool, f32 float(5,2));
create stage parqstage URL='file:///$resources/load_data/';
load data infile {'filepath'='stage://parqstage/simple2.parq', 'format'='parquet'} into table t4;
select * from t4;
drop stage parqstage;

create table t5(id bigint, name varchar, int8column tinyint, int16column smallint, binarycolumn binary, varbinarycolumn varbinary(32), blobcolumn blob);
load data infile {'filepath'='$resources/load_data/int8_int16_binary_varbinary_blob.parq', 'format'='parquet'} into table t5;
select * from t5;

-- nested types (List/Struct/Map) to TEXT
create table t6(id int, name varchar(100), scores text, address text, metadata text);
load data infile {'filepath'='$resources/load_data/nested_types.parq', 'format'='parquet'} into table t6;
select * from t6;

-- nested types to JSON
create table t7(id int, name varchar(100), scores json, address json, metadata json);
load data infile {'filepath'='$resources/load_data/nested_types.parq', 'format'='parquet'} into table t7;
select * from t7;

-- nullable columns
drop table if exists pq_nullable_test;
CREATE TABLE pq_nullable_test (
  id BIGINT NOT NULL,
  col_nullable_with_null DOUBLE,
  col_nullable_no_null VARCHAR(10),
  col_not_nullable BIGINT NOT NULL,
  col_all_null DOUBLE,
  PRIMARY KEY (id)
);
load data infile {'filepath'='$resources/parquet/nullable_test.parquet', 'format'='parquet'} into table pq_nullable_test;
SELECT
    COUNT(*) as total_rows,
    SUM(CASE WHEN col_nullable_with_null IS NULL THEN 1 ELSE 0 END) as null_count_col1,
    SUM(CASE WHEN col_all_null IS NULL THEN 1 ELSE 0 END) as null_count_all_null
FROM pq_nullable_test;
SELECT COUNT(*) FROM pq_nullable_test WHERE col_not_nullable IS NULL;
SELECT * FROM pq_nullable_test ORDER BY id;
drop table pq_nullable_test;


-- Map、Struct、List、Decimal、timestamp-no-zone
-- @bvt:issue#23583
drop table if exists parquet_complex_types;
CREATE TABLE `parquet_complex_types` (
     `test_id` bigint NOT NULL,
     `decimal_small` decimal(10,2) NOT NULL DEFAULT 0.0,
     `decimal_medium` decimal(18,4) NOT NULL DEFAULT 0.0,
     `decimal_large` decimal(38,6) NOT NULL DEFAULT 0.0,
     `timestamp_utc` timestamp NOT NULL DEFAULT '1970-01-01 00:00:00',
     `timestamp_micros` timestamp(6) NOT NULL DEFAULT '1970-01-01 00:00:00',
     `timestamp_millis` timestamp(3) NOT NULL DEFAULT '1970-01-01 00:00:00',
     `map_string_int` json DEFAULT NULL,
     `map_string_string` json DEFAULT NULL,
     `struct_simple` json DEFAULT NULL,
     `struct_nested` json DEFAULT NULL,
     `list_int` json DEFAULT NULL,
     `list_string` json DEFAULT NULL,
     `list_struct` json DEFAULT NULL
);
load data infile {'filepath'='$resources/parquet/complex.parquet', 'format'='parquet'} into table parquet_complex_types;
select * from parquet_complex_types order by test_id limit 3;
SELECT COUNT(*) FROM parquet_complex_types;
drop table parquet_complex_types;
-- @bvt:issue

drop table if exists ts_test;
CREATE TABLE ts_test (
     id BIGINT,
     ts_no_tz_micros DATETIME,
     ts_no_tz_millis DATETIME
);
load data infile {'filepath'='$resources/parquet/ts_no_tz_test.parquet', 'format'='parquet'} into table ts_test;
select * from ts_test;
drop table ts_test;

-- compressed file load
drop table if exists none_compression;
CREATE TABLE none_compression (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
);
load data infile {'filepath'='$resources/parquet/test_none.parquet', 'format'='parquet'} into table none_compression;
SELECT 'NONE' as codec, COUNT(*), SUM(value) FROM none_compression;

drop table if exists snappy_compression;
CREATE TABLE snappy_compression (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
);
load data infile {'filepath'='$resources/parquet/test_snappy.parquet', 'format'='parquet'} into table snappy_compression;
SELECT 'NONE' as codec, COUNT(*), SUM(value) FROM snappy_compression;

drop table if exists gzip_compression;
CREATE TABLE gzip_compression (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
);
load data infile {'filepath'='$resources/parquet/test_gzip.parquet', 'format'='parquet'} into table gzip_compression;
SELECT 'NONE' as codec, COUNT(*), SUM(value) FROM gzip_compression;

drop table if exists lz4_compression;
CREATE TABLE lz4_compression (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
);
load data infile {'filepath'='$resources/parquet/test_lz4.parquet', 'format'='parquet'} into table lz4_compression;
SELECT 'NONE' as codec, COUNT(*), SUM(value) FROM lz4_compression;

drop table if exists zstd_compression;
CREATE TABLE zstd_compression (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
);
load data infile {'filepath'='$resources/parquet/test_zstd.parquet', 'format'='parquet'} into table zstd_compression;
SELECT 'NONE' as codec, COUNT(*), SUM(value) FROM zstd_compression;

drop table if exists brotli_compression;
CREATE TABLE brotli_compression (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
);
load data infile {'filepath'='$resources/parquet/test_brotli.parquet', 'format'='parquet'} into table brotli_compression;
SELECT 'NONE' as codec, COUNT(*), SUM(value) FROM brotli_compression;

-- post
drop database parq;
