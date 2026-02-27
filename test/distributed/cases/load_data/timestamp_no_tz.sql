-- Test loading parquet TIMESTAMP with IsAdjustedToUTC=false
-- When IsAdjustedToUTC=false, the timestamp represents a "local time literal"
-- and should be displayed as-is regardless of session timezone

-- pre
drop database if exists ts_no_tz_test;
create database ts_no_tz_test;
use ts_no_tz_test;

-- Create table and load parquet file with IsAdjustedToUTC=false timestamp
create table ts_test (id int, ts_no_tz timestamp);
load data infile {'filepath'='$resources/load_data/timestamp_no_tz.parq', 'format'='parquet'} into table ts_test;

-- The displayed values should match the original literal values in the parquet file:
-- 2024-01-15 10:30:00, 2024-06-20 14:45:30, 2024-12-31 23:59:59
select * from ts_test order by id;

-- post
drop database ts_no_tz_test;
