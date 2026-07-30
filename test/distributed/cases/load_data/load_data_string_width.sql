-- LOAD DATA strict-mode width check for CHAR/VARCHAR columns.
-- In strict SQL mode (default), an over-width value is rejected for both serial
-- and parallel LOAD; a value that fits the column width still loads.
drop database if exists load_string_width;
create database load_string_width;
use load_string_width;

drop table if exists t_width;
create table t_width(c1 varchar(3), c2 char(3));

-- over-width value rejected (serial load)
load data infile '$resources/load_data/test_string_width_over.csv' into table t_width fields terminated by ',';
select count(*) from t_width;

-- over-width value rejected (parallel load)
load data infile '$resources/load_data/test_string_width_over.csv' into table t_width fields terminated by ',' parallel 'true';
select count(*) from t_width;

-- fitting values load successfully
load data infile '$resources/load_data/test_string_width_fit.csv' into table t_width fields terminated by ',';
select * from t_width;

drop table t_width;
drop database load_string_width;
