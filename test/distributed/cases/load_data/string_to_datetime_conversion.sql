-- Test for optional STRING to nullable DATETIME conversion in Parquet loading.
-- Covers issue #24914.

drop database if exists test_string_to_datetime;
create database test_string_to_datetime;
use test_string_to_datetime;

create table test_string_to_datetime_optional(
    id int not null,
    datetime_val datetime
);

load data infile {'filepath'='$resources/load_data/string_to_datetime_optional.parq', 'format'='parquet'}
into table test_string_to_datetime_optional parallel 'true';

select * from test_string_to_datetime_optional order by id;

drop database test_string_to_datetime;
