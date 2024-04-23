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

-- post
drop database parq;