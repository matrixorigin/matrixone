-- Test for STRING to INT conversion in Parquet loading
-- Supports: STRING → INT8/16/32/64, UINT8/16/32/64

drop database if exists test_string_to_int;
create database test_string_to_int;
use test_string_to_int;

-- Test 1: STRING → All integer types
create table test_string_to_int_all(
    int8_col tinyint not null,
    int16_col smallint not null,
    int32_col int not null,
    int64_col bigint not null,
    uint8_col tinyint unsigned not null,
    uint16_col smallint unsigned not null,
    uint32_col int unsigned not null,
    uint64_col bigint unsigned not null
);

load data infile {'filepath'='$resources/load_data/string_to_int.parq', 'format'='parquet'} 
into table test_string_to_int_all;

select * from test_string_to_int_all order by int32_col;

-- Test 2: Verify extreme values
select 
    min(int8_col) as min_int8, max(int8_col) as max_int8,
    min(int16_col) as min_int16, max(int16_col) as max_int16,
    min(int32_col) as min_int32, max(int32_col) as max_int32,
    max(uint8_col) as max_uint8,
    max(uint16_col) as max_uint16,
    max(uint32_col) as max_uint32
from test_string_to_int_all;

-- Test 3: NULL value handling
create table test_string_to_int_nulls(
    id int not null,
    value bigint
);

load data infile {'filepath'='$resources/load_data/string_to_int_nulls.parq', 'format'='parquet'} 
into table test_string_to_int_nulls;

select * from test_string_to_int_nulls order by id;

select count(*) as total, count(value) as non_null, count(*) - count(value) as null_count
from test_string_to_int_nulls;

-- Test 4: Special formats (leading zeros, positive sign, whitespace trim)
create table test_string_to_int_special(
    leading_zero int not null,
    positive_sign int not null,
    leading_space int not null,
    trailing_space int not null,
    both_spaces int not null,
    tab_newline int not null
);

load data infile {'filepath'='$resources/load_data/string_to_int_special.parq', 'format'='parquet'} 
into table test_string_to_int_special;

select * from test_string_to_int_special order by leading_zero;

-- Test 5: Overflow - INT8 should fail with value > 127
create table test_int8_overflow(
    test_case varchar(50) not null,
    value tinyint not null
);

-- @bvt:issue#15626
load data infile {'filepath'='$resources/load_data/string_to_int_overflow.parq', 'format'='parquet'} 
into table test_int8_overflow;
-- @bvt:issue

-- Test 6: Invalid format - should fail with non-numeric string
create table test_invalid_format(
    test_case varchar(50) not null,
    value int not null
);

-- @bvt:issue#15626
load data infile {'filepath'='$resources/load_data/string_to_int_invalid.parq', 'format'='parquet'} 
into table test_invalid_format;
-- @bvt:issue

-- Test 7: Only whitespace strings - should fail
create table test_only_whitespace(
    test_case varchar(50) not null,
    value int not null
);

-- @bvt:issue#15626
load data infile {'filepath'='$resources/load_data/string_to_int_only_spaces.parq', 'format'='parquet'} 
into table test_only_whitespace;
-- @bvt:issue

-- Test 8: Scientific notation - should fail
create table test_scientific_notation(
    test_case varchar(50) not null,
    value int not null
);

-- @bvt:issue#15626
load data infile {'filepath'='$resources/load_data/string_to_int_scientific.parq', 'format'='parquet'} 
into table test_scientific_notation;
-- @bvt:issue

-- Test 9: Hexadecimal notation - should fail
create table test_hexadecimal(
    test_case varchar(50) not null,
    value int not null
);

-- @bvt:issue#15626
load data infile {'filepath'='$resources/load_data/string_to_int_hex.parq', 'format'='parquet'} 
into table test_hexadecimal;
-- @bvt:issue

-- Test 10: Very long strings - should fail
create table test_very_long_string(
    test_case varchar(50) not null,
    value bigint not null
);

-- @bvt:issue#15626
load data infile {'filepath'='$resources/load_data/string_to_int_very_long.parq', 'format'='parquet'} 
into table test_very_long_string;
-- @bvt:issue

-- Test 11: Unicode digits - should fail
create table test_unicode_digits(
    test_case varchar(50) not null,
    value int not null
);

-- @bvt:issue#15626
load data infile {'filepath'='$resources/load_data/string_to_int_unicode.parq', 'format'='parquet'} 
into table test_unicode_digits;
-- @bvt:issue

-- Cleanup
drop database test_string_to_int;

