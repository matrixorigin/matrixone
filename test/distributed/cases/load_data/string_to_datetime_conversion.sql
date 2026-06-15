-- Test for STRING to DATE/TIME/DATETIME/TIMESTAMP conversion in Parquet loading
-- Supports: STRING → DATE, TIME, DATETIME, TIMESTAMP

drop database if exists test_string_to_datetime;
create database test_string_to_datetime;
use test_string_to_datetime;

-- Test 1: STRING → DATE
create table test_string_to_date(
    date_col date not null
);

load data infile {'filepath'='$resources/load_data/string_to_date.parq', 'format'='parquet'} 
into table test_string_to_date;

select * from test_string_to_date order by date_col;

-- Test 2: STRING → TIME
create table test_string_to_time(
    time_col time not null
);

load data infile {'filepath'='$resources/load_data/string_to_time.parq', 'format'='parquet'} 
into table test_string_to_time;

select * from test_string_to_time order by time_col;

-- Test 3: STRING → TIME with scale
create table test_string_to_time_scale(
    time_col time(6) not null
);

load data infile {'filepath'='$resources/load_data/string_to_time_scale.parq', 'format'='parquet'} 
into table test_string_to_time_scale;

select * from test_string_to_time_scale order by time_col;

-- Test 4: STRING → TIMESTAMP
create table test_string_to_timestamp(
    timestamp_col timestamp not null
);

load data infile {'filepath'='$resources/load_data/string_to_timestamp.parq', 'format'='parquet'} 
into table test_string_to_timestamp;

select * from test_string_to_timestamp order by timestamp_col;

-- Test 5: STRING → TIMESTAMP with scale
create table test_string_to_timestamp_scale(
    timestamp_col timestamp(6) not null
);

load data infile {'filepath'='$resources/load_data/string_to_timestamp_scale.parq', 'format'='parquet'} 
into table test_string_to_timestamp_scale;

select * from test_string_to_timestamp_scale order by timestamp_col;

-- Test 6: NULL value handling
create table test_datetime_nulls(
    id int not null,
    date_val date,
    time_val time,
    timestamp_val timestamp
);

load data infile {'filepath'='$resources/load_data/string_to_datetime_nulls.parq', 'format'='parquet'} 
into table test_datetime_nulls;

select * from test_datetime_nulls order by id;

-- Test 7: optional STRING → DATETIME NULL (issue #24914)
create table test_string_to_datetime_optional(
    id int not null,
    datetime_val datetime
);

load data infile {'filepath'='$resources/load_data/string_to_datetime_optional.parq', 'format'='parquet'}
into table test_string_to_datetime_optional parallel 'true';

select * from test_string_to_datetime_optional order by id;

-- Test 8: Special formats (whitespace)
create table test_datetime_special(
    date_val date not null,
    time_val time not null,
    timestamp_val timestamp not null
);

load data infile {'filepath'='$resources/load_data/string_to_datetime_special.parq', 'format'='parquet'} 
into table test_datetime_special;

select * from test_datetime_special order by date_val;

-- Test 9: Invalid format - should fail
create table test_datetime_invalid(
    test_case varchar(50),
    date_val date
);

load data infile {'filepath'='$resources/load_data/string_to_datetime_invalid.parq', 'format'='parquet'} 
into table test_datetime_invalid;

-- Cleanup
drop database test_string_to_datetime;
