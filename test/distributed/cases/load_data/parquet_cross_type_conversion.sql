-- Test representative Parquet cross-type conversions for issue #24914.

drop database if exists test_parquet_cross_type_conversion;
create database test_parquet_cross_type_conversion;
use test_parquet_cross_type_conversion;
SET time_zone = '+00:00';

create table scalar_cross_type(
    bool_as_float double,
    bool_as_json json,
    bool_as_decimal decimal(10,2),
    int_as_bool bool,
    int_as_json json,
    int_as_enum enum('red','green','blue'),
    float_as_bool bool,
    float_as_int bigint,
    float_as_json json,
    float_as_decimal decimal(10,2),
    dec_as_int bigint,
    dec_as_json json,
    string_as_bool bool,
    string_as_bit bit(1),
    date_as_timestamp timestamp(6),
    ts_as_date date,
    ts_as_time time(6)
);

load data infile {'filepath'='$resources/load_data/parquet_cross_type_scalar.parq', 'format'='parquet'}
into table scalar_cross_type;

select
    bool_as_float,
    bool_as_json,
    bool_as_decimal,
    int_as_bool,
    int_as_json,
    int_as_enum,
    float_as_bool,
    float_as_int,
    float_as_json,
    float_as_decimal,
    dec_as_int,
    dec_as_json,
    string_as_bool,
    cast(string_as_bit as unsigned) as string_as_bit,
    date_as_timestamp,
    ts_as_date,
    ts_as_time
from scalar_cross_type;

create table list_float_to_vecf64(v vecf64(3));

load data infile {'filepath'='$resources/load_data/parquet_cross_type_list_float.parq', 'format'='parquet'}
into table list_float_to_vecf64;

select v from list_float_to_vecf64;

create table list_double_to_vecf32(v vecf32(3));

load data infile {'filepath'='$resources/load_data/parquet_cross_type_list_double.parq', 'format'='parquet'}
into table list_double_to_vecf32;

select v from list_double_to_vecf32;

drop database test_parquet_cross_type_conversion;
