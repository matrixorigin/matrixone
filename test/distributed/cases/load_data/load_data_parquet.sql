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
select 'NONE' as codec, count(*), sum(value) from brotli_compression;

-- v1
drop table if exists pq_version_compare;
CREATE TABLE `pq_version_compare` (
      `id` bigint DEFAULT NULL,
      `user_name` varchar(50) DEFAULT NULL,
      `login_time` timestamp NULL DEFAULT NULL
);
load data infile {'filepath'='$resources/parquet/test_v1_legacy.parquet', 'format'='parquet'} into table pq_version_compare;
select * from pq_version_compare;
truncate table pq_version_compare;

-- v2
load data infile {'filepath'='$resources/parquet/test_v2_modern.parquet', 'format'='parquet'} into table pq_version_compare;
select * from pq_version_compare;


-- Column statistics (write_statistics=True/False)
drop table if exists pq_stats_check;
CREATE TABLE pq_stats_check (
    id BIGINT,
    score DOUBLE
);
load data infile {'filepath'='$resources/parquet/no_stats.parquet', 'format'='parquet'} into table pq_stats_check;
select * from pq_stats_check;
select 'no_stats' as source, count(*), MIN(id), MAX(score) from pq_stats_check;
truncate pq_stats_check;
load data infile {'filepath'='$resources/parquet/with_stats.parquet', 'format'='parquet'} into table pq_stats_check;
select * from pq_stats_check;
select 'no_stats' as source, count(*), MIN(id), MAX(score) from pq_stats_check;

-- empty file
-- @bvt:issue#23601
-- parquet file column number equal to table column number, and column name is not same
drop table if exists empty_table;
create table empty_table(col1 bigint, col2 varchar);
load data infile {'filepath'='$resources/parquet/empty_test.parquet', 'format'='parquet'} into table empty_table;
select * from empty_table;

-- parquet file column number equal to table column number, and column name is the same
drop table if exists empty_table01;
create table empty_table01(id bigint, name varchar);
load data infile {'filepath'='$resources/parquet/empty_test.parquet', 'format'='parquet'} into table empty_table01;
select * from empty_table;
-- @bvt:issue

-- @bvt:issue#23601
-- parquet file's column number is not equal to table's column number
drop table if exists empty_table02;
create table empty_table02(col1 int);
load data infile {'filepath'='$resources/parquet/empty_test.parquet', 'format'='parquet'} into table empty_table02;
select * from empty_table02;
-- @bvt:issue


-- wide tables
drop table if exists wide_100_columns;
CREATE TABLE wide_100_columns (
      col_0 BIGINT, col_1 DOUBLE, col_2 BIGINT, col_3 DOUBLE, col_4 BIGINT,
      col_5 DOUBLE, col_6 BIGINT, col_7 DOUBLE, col_8 BIGINT, col_9 DOUBLE,
      col_10 BIGINT, col_11 DOUBLE, col_12 BIGINT, col_13 DOUBLE, col_14 BIGINT,
      col_15 DOUBLE, col_16 BIGINT, col_17 DOUBLE, col_18 BIGINT, col_19 DOUBLE,
      col_20 BIGINT, col_21 DOUBLE, col_22 BIGINT, col_23 DOUBLE, col_24 BIGINT,
      col_25 DOUBLE, col_26 BIGINT, col_27 DOUBLE, col_28 BIGINT, col_29 DOUBLE,
      col_30 BIGINT, col_31 DOUBLE, col_32 BIGINT, col_33 DOUBLE, col_34 BIGINT,
      col_35 DOUBLE, col_36 BIGINT, col_37 DOUBLE, col_38 BIGINT, col_39 DOUBLE,
      col_40 BIGINT, col_41 DOUBLE, col_42 BIGINT, col_43 DOUBLE, col_44 BIGINT,
      col_45 DOUBLE, col_46 BIGINT, col_47 DOUBLE, col_48 BIGINT, col_49 DOUBLE,
      col_50 BIGINT, col_51 DOUBLE, col_52 BIGINT, col_53 DOUBLE, col_54 BIGINT,
      col_55 DOUBLE, col_56 BIGINT, col_57 DOUBLE, col_58 BIGINT, col_59 DOUBLE,
      col_60 BIGINT, col_61 DOUBLE, col_62 BIGINT, col_63 DOUBLE, col_64 BIGINT,
      col_65 DOUBLE, col_66 BIGINT, col_67 DOUBLE, col_68 BIGINT, col_69 DOUBLE,
      col_70 BIGINT, col_71 DOUBLE, col_72 BIGINT, col_73 DOUBLE, col_74 BIGINT,
      col_75 DOUBLE, col_76 BIGINT, col_77 DOUBLE, col_78 BIGINT, col_79 DOUBLE,
      col_80 BIGINT, col_81 DOUBLE, col_82 BIGINT, col_83 DOUBLE, col_84 BIGINT,
      col_85 DOUBLE, col_86 BIGINT, col_87 DOUBLE, col_88 BIGINT, col_89 DOUBLE,
      col_90 BIGINT, col_91 DOUBLE, col_92 BIGINT, col_93 DOUBLE, col_94 BIGINT,
      col_95 DOUBLE, col_96 BIGINT, col_97 DOUBLE, col_98 BIGINT, col_99 DOUBLE
);
load data infile {'filepath'='$resources/parquet/wide_table_100.parquet', 'format'='parquet'} into table wide_100_columns;
select count(*) from wide_100_columns;
select max(col_99) from wide_100_columns;
select col_0, col_50, col_99 from wide_100_columns LIMIT 5;


drop table if exists pq_supported_types;
CREATE TABLE pq_supported_types (
    int32_col INT,
    int64_col BIGINT,
    uint32_col INT UNSIGNED,
    float32_col FLOAT,
    float64_col DOUBLE,
    string_col VARCHAR(255),
    bool_col BOOL,
    date_col DATE,
    time_col TIME,
    ts_col TIMESTAMP
);
load data infile {'filepath'='$resources/parquet/supported_types.parquet', 'format'='parquet'} into table pq_supported_types;
select int32_col,int64_col,uint32_col,float32_col,float64_col,string_col,bool_col,date_col,time_col from pq_supported_types;
select count(*) from pq_supported_types;

-- new types
drop table if exists pq_new_types;
create table pq_new_types (
  col_int8 TINYINT,
  col_int16 SMALLINT,
  col_large_string TEXT,
  col_ts_no_tz DATETIME,
  col_binary BLOB,
  col_decimal DECIMAL(18, 3)
);
load data infile {'filepath'='$resources/parquet/new_supported_types.parquet', 'format'='parquet'} into table pq_new_types;
select count(*) from pq_new_types;
select * from pq_new_types;
select length(col_binary) from pq_new_types;
select col_ts_no_tz from pq_new_types;

-- int8->int8,int16->int16,binary,decimal->decimal
drop table if exists subtask_test;
create table subtask_test (
      c_int8 TINYINT,
      c_int16 SMALLINT,
      c_binary BLOB,
      c_decimal DECIMAL(12, 4),
      c_large_str TEXT
);
load data infile {'filepath'='$resources/parquet/subtask_22691_test.parquet', 'format'='parquet'} into table subtask_test;
select * from subtask_test;

-- decimal
drop table if exists pq_decimal_boundary;
create table pq_decimal_boundary (
     dec_p9_s2 DECIMAL(9, 2),
     dec_p18_s9 DECIMAL(18, 9),
     dec_p38_s0 DECIMAL(38, 0),
     dec_p38_s38 DECIMAL(38, 38)
);
load data infile {'filepath'='$resources/parquet/decimal_boundary_test.parquet', 'format'='parquet'} into table pq_decimal_boundary;
select * from pq_decimal_boundary;
select dec_p38_s0 from pq_decimal_boundary WHERE dec_p38_s0 > 0;
select dec_p38_s38 from pq_decimal_boundary WHERE dec_p38_s38 > 0;
select dec_p9_s2 * 1.1 from pq_decimal_boundary LIMIT 1;


-- binary->binary/blob
drop table if exists pq_binary_test;
create table pq_binary_test (
    bin_fixed BINARY(10),
    bin_var VARBINARY(255),
    bin_blob BLOB,
    bin_empty VARBINARY(10)
);
load data infile {'filepath'='$resources/parquet/binary_mapping_test.parquet', 'format'='parquet'} into table pq_binary_test;
select
    hex(bin_fixed) as fixed_hex,
    hex(bin_var) as var_hex,
    hex(bin_blob) as blob_hex
from pq_binary_test;
select length(bin_fixed) from pq_binary_test;
select count(*) from pq_binary_test where bin_blob is null;
select length(bin_empty) from pq_binary_test limit 1;


-- string to int
drop table if exists parquet_01;
CREATE TABLE `parquet_01` (
      `sepal.length` double DEFAULT NULL,
      `sepal.width` double DEFAULT NULL,
      `petal.length` double DEFAULT NULL,
      `petal.width` double DEFAULT NULL,
      `variety` varchar(20) DEFAULT NULL
);
load data infile {'filepath'='$resources/parquet/Iris.parquet', 'format'='parquet'} into table parquet_01;
select count(*) from parquet_01;


-- post
drop database parq;
