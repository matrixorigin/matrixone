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

drop database decimal256_external_load;
