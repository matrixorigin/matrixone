drop database if exists parquet_cross_type_db;
create database parquet_cross_type_db;
use parquet_cross_type_db;

drop table if exists ext_bool_tinyint;
create external table ext_bool_tinyint (
    col_bool tinyint,
    part_id int
) infile{'filepath'='$resources/hive_partition/cross_type/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};
select col_bool, part_id from ext_bool_tinyint where part_id = 1 order by col_bool, part_id;

drop table if exists ext_bool_varchar;
create external table ext_bool_varchar (
    col_bool varchar(5),
    part_id int
) infile{'filepath'='$resources/hive_partition/cross_type/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};
select col_bool, part_id from ext_bool_varchar where part_id = 1 order by col_bool, part_id;

drop table if exists ext_int64_int;
create external table ext_int64_int (
    col_int64 int,
    part_id int
) infile{'filepath'='$resources/hive_partition/cross_type/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};
select col_int64, part_id from ext_int64_int where part_id = 1 order by col_int64;

drop table if exists ext_int64_varchar;
create external table ext_int64_varchar (
    col_int64 varchar(20),
    part_id int
) infile{'filepath'='$resources/hive_partition/cross_type/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};
select col_int64, part_id from ext_int64_varchar where part_id = 1 order by col_int64;

drop table if exists ext_double_float;
create external table ext_double_float (
    col_double float,
    part_id int
) infile{'filepath'='$resources/hive_partition/cross_type/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};
select col_double, part_id from ext_double_float where part_id = 1 order by col_double;

drop table if exists ext_decimal_double;
create external table ext_decimal_double (
    col_decimal double,
    part_id int
) infile{'filepath'='$resources/hive_partition/cross_type/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};
select col_decimal, part_id from ext_decimal_double where part_id = 1 order by col_decimal;

drop table if exists ext_decimal_varchar;
create external table ext_decimal_varchar (
    col_decimal varchar(20),
    part_id int
) infile{'filepath'='$resources/hive_partition/cross_type/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};
select col_decimal, part_id from ext_decimal_varchar where part_id = 1 order by col_decimal;

drop table if exists ext_date_varchar;
create external table ext_date_varchar (
    col_date varchar(20),
    part_id int
) infile{'filepath'='$resources/hive_partition/cross_type/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};
select col_date, part_id from ext_date_varchar where part_id = 1 order by col_date;

drop database if exists parquet_cross_type_db;
