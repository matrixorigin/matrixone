-- External TIME assignment shares native bounds, mode diagnostics and rollback.
set @saved_sql_mode = @@sql_mode;
set @saved_time_zone = @@time_zone;
set time_zone = '+00:00';
set sql_mode = 'STRICT_TRANS_TABLES';
drop database if exists external_time_assignment_db;
create database external_time_assignment_db;
use external_time_assignment_db;
drop table if exists external_time_assignment;
create table external_time_assignment(id int primary key, v time(6));

-- csv: exact endpoints, -1us and NULL; strict mixed batch is atomic.
load data infile {'filepath'='$resources/load_data/time_assignment_range.csv','format'='csv'} into table external_time_assignment fields terminated by ',';
show warnings;
select count(*) from external_time_assignment;
load data infile {'filepath'='$resources/load_data/time_assignment_valid.csv','format'='csv'} into table external_time_assignment fields terminated by ',';
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = '';
load data infile {'filepath'='$resources/load_data/time_assignment_range.csv','format'='csv'} into table external_time_assignment fields terminated by ',';
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = 'STRICT_TRANS_TABLES';

-- jsonline: exact endpoints, -1us and NULL; strict mixed batch is atomic.
load data infile {'filepath'='$resources/load_data/time_assignment_range.jl','format'='jsonline','jsondata'='object'} into table external_time_assignment;
show warnings;
select count(*) from external_time_assignment;
load data infile {'filepath'='$resources/load_data/time_assignment_valid.jl','format'='jsonline','jsondata'='object'} into table external_time_assignment;
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = '';
load data infile {'filepath'='$resources/load_data/time_assignment_range.jl','format'='jsonline','jsondata'='object'} into table external_time_assignment;
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = 'STRICT_TRANS_TABLES';

-- parquet: exact endpoints, -1us and NULL; strict mixed batch is atomic.
load data infile {'filepath'='$resources/load_data/time_assignment_range.parquet','format'='parquet'} into table external_time_assignment;
show warnings;
select count(*) from external_time_assignment;
load data infile {'filepath'='$resources/load_data/time_assignment_valid.parquet','format'='parquet'} into table external_time_assignment;
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = '';
load data infile {'filepath'='$resources/load_data/time_assignment_range.parquet','format'='parquet'} into table external_time_assignment;
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = 'STRICT_TRANS_TABLES';

-- Native controls: the same strict errors and permissive assignments.
insert into external_time_assignment values (1,'838:59:59.000001');
insert into external_time_assignment values (2,'-838:59:59.000001');
select count(*) from external_time_assignment;
set sql_mode = '';
insert into external_time_assignment values (1,'838:59:59.000001'),(2,'-838:59:59.000001');
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = 'STRICT_TRANS_TABLES';
load data infile {'filepath'='$resources/load_data/time_assignment_range.csv','format'='csv'} ignore into table external_time_assignment fields terminated by ',';
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
drop table external_time_assignment;

-- Parsing rounds once; TIME_TRUNCATE_FRACTIONAL applies before domain checks.
create table external_time_assignment(v time(0));
load data inline format='csv', data='00:00:00.4999999' into table external_time_assignment;
load data inline format='csv', data='838:59:59.9' into table external_time_assignment;
select concat('t=',cast(v as char)) as time_value from external_time_assignment;
truncate table external_time_assignment;
set sql_mode = 'STRICT_TRANS_TABLES,TIME_TRUNCATE_FRACTIONAL';
load data inline format='csv', data='838:59:59.9' into table external_time_assignment;
select concat('t=',cast(v as char)) as time_value from external_time_assignment;
drop table external_time_assignment;
-- Raw duration extremes must retain their sign before precision normalization.
set sql_mode = 'STRICT_TRANS_TABLES';
create table external_time_assignment(id int primary key, v time(0));
load data infile {'filepath'='$resources/load_data/time_assignment_extreme.parquet','format'='parquet'} into table external_time_assignment;
show warnings;
select count(*) from external_time_assignment;
set sql_mode = '';
load data infile {'filepath'='$resources/load_data/time_assignment_extreme.parquet','format'='parquet'} into table external_time_assignment;
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
drop table external_time_assignment;
-- Text beyond the internal duration range must use native assignment policy.
create table external_time_assignment(id int primary key, v time(6));
set sql_mode = 'STRICT_TRANS_TABLES';
insert into external_time_assignment values (2,'2562047788:00:00');
show warnings;
select count(*) from external_time_assignment;
set sql_mode = '';
insert into external_time_assignment values (2,'2562047788:00:00'),(3,'-2562047788:00:00');
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = 'STRICT_TRANS_TABLES';
load data inline format='csv', data='1,00:00:01.9876549\n2,2562047788:00:00\n3,-2562047788:00:00\n4,' into table external_time_assignment fields terminated by ',';
show warnings;
select count(*) from external_time_assignment;
set sql_mode = '';
load data inline format='csv', data='1,00:00:01.9876549\n2,2562047788:00:00\n3,-2562047788:00:00\n4,' into table external_time_assignment fields terminated by ',';
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = 'STRICT_TRANS_TABLES';
load data infile {'filepath'='$resources/load_data/time_assignment_text_overflow.jl','format'='jsonline','jsondata'='object'} into table external_time_assignment;
show warnings;
select count(*) from external_time_assignment;
set sql_mode = '';
load data infile {'filepath'='$resources/load_data/time_assignment_text_overflow.jl','format'='jsonline','jsondata'='object'} into table external_time_assignment;
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
set sql_mode = 'STRICT_TRANS_TABLES';
load data infile {'filepath'='$resources/load_data/time_assignment_text_overflow.parquet','format'='parquet'} into table external_time_assignment;
show warnings;
select count(*) from external_time_assignment;
load data infile {'filepath'='$resources/load_data/time_assignment_text_overflow.parquet','format'='parquet'} ignore into table external_time_assignment;
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
truncate table external_time_assignment;
-- Prepared text input retains the same raw value as sql_mode changes.
prepare time_text_load from "load data inline format='csv', data='2,2562047788:00:00' into table external_time_assignment fields terminated by ','";
execute time_text_load;
select count(*) from external_time_assignment;
set sql_mode = '';
execute time_text_load;
show warnings;
select id, concat('t=',cast(v as char)) as time_value from external_time_assignment order by id;
deallocate prepare time_text_load;
drop table external_time_assignment;
drop database external_time_assignment_db;
set time_zone = @saved_time_zone;
set sql_mode = @saved_sql_mode;
