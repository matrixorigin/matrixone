-- Test for Integer Widening Conversion in Parquet Loading
-- Supports: INT32 → INT64, UINT32 → UINT64

-- Setup
drop database if exists test_int_widening;
create database test_int_widening;
use test_int_widening;

-- Test 1: INT32 → INT64 widening conversion
create table test_int32_to_int64(
    id bigint,
    value bigint
);

load data infile {'filepath'='$resources/load_data/int32_to_int64.parq', 'format'='parquet'} 
into table test_int32_to_int64;

select * from test_int32_to_int64 order by id;

-- Test 2: UINT32 → UINT64 widening conversion
create table test_uint32_to_uint64(
    id bigint unsigned,
    value bigint unsigned
);

load data infile {'filepath'='$resources/load_data/uint32_to_uint64.parq', 'format'='parquet'} 
into table test_uint32_to_uint64;

select * from test_uint32_to_uint64 order by id;

-- Test 3: Backward compatibility - INT32 → INT32 still works
create table test_int32_to_int32(
    id int,
    value int
);

load data infile {'filepath'='$resources/load_data/int32_to_int64.parq', 'format'='parquet'} 
into table test_int32_to_int32;

select * from test_int32_to_int32 order by id limit 3;

-- Cleanup
drop database test_int_widening;

