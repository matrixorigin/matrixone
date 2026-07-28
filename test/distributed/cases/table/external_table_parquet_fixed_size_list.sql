drop database if exists parquet_fixed_size_list_db;
create database parquet_fixed_size_list_db;
use parquet_fixed_size_list_db;

drop table if exists ext_vec_plain;
create external table ext_vec_plain (
    id int,
    col_vec_f32 vecf32(3),
    col_vec_f64 vecf64(3)
) infile{'filepath'='$resources/parquet_fixed_size_list/data.parquet', 'format'='parquet'};

select
    id,
    col_vec_f32,
    vector_dims(col_vec_f32) as dims_f32,
    col_vec_f64,
    vector_dims(col_vec_f64) as dims_f64
from ext_vec_plain
order by id;

drop table if exists ext_vec_hive;
create external table ext_vec_hive (
    id int,
    col_vec_f32 vecf32(3),
    col_vec_f64 vecf64(3),
    part_id int
) infile{'filepath'='$resources/hive_partition/fixed_size_list/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};

select
    id,
    col_vec_f32,
    col_vec_f64,
    part_id
from ext_vec_hive
where part_id = 1
order by id;

drop table if exists ext_vec_hive;
drop table if exists ext_vec_plain;
drop database if exists parquet_fixed_size_list_db;
