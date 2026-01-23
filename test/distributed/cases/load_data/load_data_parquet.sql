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

-- post
drop database parq;
