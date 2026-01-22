-- Test Parquet DECIMAL binary format (INT32/INT64/FixedLenByteArray) to MatrixOne DECIMAL

drop database if exists test_binary_decimal;
create database test_binary_decimal;
use test_binary_decimal;

create table test_decimal(
    id int,
    dec_int32 decimal(5, 2),
    dec_int64 decimal(12, 2),
    dec_fixed decimal(24, 4)
);

load data infile {'filepath'='$resources/load_data/binary_decimal.parq', 'format'='parquet'} 
into table test_decimal;

select * from test_decimal order by id;

select count(*) as total, count(dec_int32) as non_null from test_decimal;

drop database test_binary_decimal;
