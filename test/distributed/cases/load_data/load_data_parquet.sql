-- pre
drop database if exists parq;
create database parq;
use parq;
create table t1(id bigint,name varchar);

-- read from an already exported file
load data infile {'filepath'='$resources/load_data/simple.parq', 'format'='parquet'} into table t1;
select * from t1;

-- post
drop database parq;