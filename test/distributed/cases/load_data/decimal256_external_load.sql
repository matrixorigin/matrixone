-- @label:bvt
-- Issue #28590: CSV and JSONLINE external paths must materialize Decimal256.
drop database if exists decimal256_external_load;
create database decimal256_external_load;
use decimal256_external_load;

-- CSV external SELECT and INSERT ... SELECT, including high precision,
-- negative values with fractional scale and an empty numeric record.
create external table decimal256_csv_external(
    id int,
    d decimal(39,0),
    s decimal(50,10)
) infile{'filepath'='$resources/load_data/decimal256_external.csv'}
fields terminated by ',' lines terminated by '\n';

select id, d, s from decimal256_csv_external order by id;

create table decimal256_csv_dest(
    id int,
    d decimal(39,0),
    s decimal(50,10)
);
insert into decimal256_csv_dest select id, d, s from decimal256_csv_external;
select id, d, s from decimal256_csv_dest order by id;

-- JSONLINE external SELECT and INSERT ... SELECT, including JSON nulls.
create external table decimal256_json_external(
    id int,
    d decimal(39,0),
    s decimal(50,10)
) infile{'filepath'='$resources/load_data/decimal256_external.jl', 'format'='jsonline', 'jsondata'='object'};

select id, d, s from decimal256_json_external order by id;

create table decimal256_json_dest(
    id int,
    d decimal(39,0),
    s decimal(50,10)
);
insert into decimal256_json_dest select id, d, s from decimal256_json_external;
select id, d, s from decimal256_json_dest order by id;

-- LOAD DATA reaches the same CSV/JSONLINE field conversion entry point.
create table decimal256_load_csv(
    id int,
    d decimal(39,0),
    s decimal(50,10)
);
load data inline format='csv', data='1,999999999999999999999999999999999999999,1234567890123456789012345678901234567890.1234567890\n2,-1,-0.0000000001\n' into table decimal256_load_csv fields terminated by ',';
select id, d, s from decimal256_load_csv order by id;

create table decimal256_load_json(
    id int,
    d decimal(39,0),
    s decimal(50,10)
);
load data inline format='jsonline', data='{"id":1,"d":"999999999999999999999999999999999999999","s":"1234567890123456789012345678901234567890.1234567890"}\n{"id":2,"d":-1,"s":-0.0000000001}\n{"id":3,"d":null,"s":null}\n', jsontype='object' into table decimal256_load_json;
select id, d, s from decimal256_load_json order by id;

-- Invalid and over-precision values keep the existing error text and do not
-- append a partial row.
create table decimal256_bad_load(id int, d decimal(39,0));
load data inline format='csv', data='1,not-a-decimal\n' into table decimal256_bad_load fields terminated by ',';
select count(*) as invalid_rows from decimal256_bad_load;
load data inline format='csv', data='1,9999999999999999999999999999999999999999\n' into table decimal256_bad_load fields terminated by ',';
select count(*) as overflow_rows from decimal256_bad_load;

-- Strict fixed-column conversion must validate the tail after retained digits.
set @decimal256_saved_sql_mode = @@session.sql_mode;
set session sql_mode = 'STRICT_ALL_TABLES';
create table decimal256_tail_load(id int, p39 decimal(39,2), p65 decimal(65,2));

-- 76-digit rejection control, then the first dropped digit with a valid row
-- before the malformed row: the failed statement must insert neither row.
load data inline format='csv', data='1,0.1111111111111111111111111111111111111111111111111111111111111111111111111111z,0.5\n' into table decimal256_tail_load fields terminated by ',';
select count(*) as boundary_rows from decimal256_tail_load;
load data inline format='csv', data='1,0.25,0.5\n2,0.11111111111111111111111111111111111111111111111111111111111111111111111111111z,0.5\n' into table decimal256_tail_load fields terminated by ',';
select count(*) as malformed_csv_rows from decimal256_tail_load;

-- JSONLINE reaches the same parser, including a repeated decimal point.
load data inline format='jsonline', data='{"id":1,"p39":"0.25","p65":"0.5"}\n{"id":2,"p39":"0.25","p65":"0.11111111111111111111111111111111111111111111111111111111111111111111111111111111.1"}\n', jsontype='object' into table decimal256_tail_load;
select count(*) as malformed_json_rows from decimal256_tail_load;

-- Valid long fractions and exponents still round to the final column scale.
load data inline format='csv', data='1,0.11111111111111111111111111111111111111111111111111111111111111111111111111111111,0.11111111111111111111111111111111111111111111111111111111111111111111111111111e1\n' into table decimal256_tail_load fields terminated by ',';
load data inline format='jsonline', data='{"id":2,"p39":"-0.11111111111111111111111111111111111111111111111111111111111111111111111111111","p65":"-0.11111111111111111111111111111111111111111111111111111111111111111111111111111111E-1"}\n', jsontype='object' into table decimal256_tail_load;
select id, concat('d=',p39) as p39_text, concat('d=',p65) as p65_text from decimal256_tail_load order by id;
set session sql_mode = @decimal256_saved_sql_mode;

drop database decimal256_external_load;
