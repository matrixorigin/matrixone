-- Test for STRING to DECIMAL conversion in Parquet loading
-- Supports: STRING → DECIMAL64/DECIMAL128
-- Features: Scientific notation (lowercase e), whitespace trim, leading zeros

drop database if exists test_string_to_decimal;
create database test_string_to_decimal;
use test_string_to_decimal;

-- Test 1: Basic STRING → DECIMAL64 conversions
create table test_basic_decimal64(
    int_string decimal(10, 0),
    decimal_string decimal(10, 2),
    sci_notation decimal(18, 2),
    negative decimal(10, 2)
);

load data infile {'filepath'='$resources/load_data/string_to_decimal_basic.parq', 'format'='parquet'} 
into table test_basic_decimal64;

select * from test_basic_decimal64 order by int_string;

-- Test 2: Special formats (whitespace, leading zeros, positive sign, uppercase E)
create table test_special_formats(
    leading_space decimal(10, 2),
    trailing_space decimal(10, 2),
    both_spaces decimal(10, 2),
    leading_zero decimal(10, 2),
    positive_sign decimal(10, 2),
    uppercase_e decimal(18, 2)
);

load data infile {'filepath'='$resources/load_data/string_to_decimal_special.parq', 'format'='parquet'} 
into table test_special_formats;

select * from test_special_formats order by leading_space;

-- Test 3: NULL value handling
create table test_nulls(
    id int,
    value decimal(10, 2)
);

load data infile {'filepath'='$resources/load_data/string_to_decimal_nulls.parq', 'format'='parquet'} 
into table test_nulls;

select * from test_nulls order by id;

select count(*) as total, count(value) as non_null from test_nulls;

-- Test 4: Boundary values for DECIMAL64
create table test_boundaries(
    max_int decimal(18, 0),
    min_int decimal(18, 0),
    max_decimal decimal(18, 2)
);

load data infile {'filepath'='$resources/load_data/string_to_decimal_boundaries.parq', 'format'='parquet'} 
into table test_boundaries;

select * from test_boundaries;

-- Test 5: DECIMAL128 support
create table test_decimal128(
    large_int decimal(38, 0),
    high_precision decimal(38, 10)
);

load data infile {'filepath'='$resources/load_data/string_to_decimal128.parq', 'format'='parquet'} 
into table test_decimal128;

select * from test_decimal128;

-- Test 6: Invalid format - should fail
create table test_invalid(
    test_case varchar(50),
    value decimal(10, 2)
);

-- @bvt:issue#15626
load data infile {'filepath'='$resources/load_data/string_to_decimal_invalid.parq', 'format'='parquet'} 
into table test_invalid;
-- @bvt:issue

-- Cleanup
drop database test_string_to_decimal;

