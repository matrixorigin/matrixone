-- @suite
-- @case
-- @desc: preserve nullable metadata for strict functions that can synthesize NULL
-- @label:bvt

drop database if exists issue26830_strict_nullability;
create database issue26830_strict_nullability;
use issue26830_strict_nullability;

create table src (id int primary key, v int not null, s varchar(20) not null, j json not null);
insert into src values (1, 10, 'abc', '{"a": 1}'), (2, 20, 'abc', '{"a": 1}');

create view strict_null_view as select id, v / (id - 1) as div_zero, json_extract(j, '$.missing') as missing_json, regexp_substr(s, 'z+') as no_match, inet6_aton('not-an-ip') as invalid_inet, elt(0, 'x', 'y') as invalid_elt, unhex('xyz') as invalid_hex, makedate(2024, 0) as invalid_date from src;
select column_name, is_nullable from information_schema.columns where table_schema = 'issue26830_strict_nullability' and table_name = 'strict_null_view' and column_name <> 'id' order by ordinal_position;
select div_zero is null as div_zero_is_null, missing_json is null as missing_json_is_null, no_match is null as no_match_is_null, invalid_inet is null as invalid_inet_is_null, invalid_elt is null as invalid_elt_is_null, invalid_hex is null as invalid_hex_is_null, invalid_date is null as invalid_date_is_null from strict_null_view where id = 1;

create table strict_null_ctas as select id, v / (id - 1) as div_zero, json_extract(j, '$.missing') as missing_json, regexp_substr(s, 'z+') as no_match, inet6_aton('not-an-ip') as invalid_inet, elt(0, 'x', 'y') as invalid_elt, unhex('xyz') as invalid_hex, makedate(2024, 0) as invalid_date from src;
select column_name, is_nullable from information_schema.columns where table_schema = 'issue26830_strict_nullability' and table_name = 'strict_null_ctas' and column_name <> 'id' order by ordinal_position;
select div_zero is null as div_zero_is_null, missing_json is null as missing_json_is_null, no_match is null as no_match_is_null, invalid_inet is null as invalid_inet_is_null, invalid_elt is null as invalid_elt_is_null, invalid_hex is null as invalid_hex_is_null, invalid_date is null as invalid_date_is_null from strict_null_ctas where id = 1;

create table coalesced as select id, coalesce(v / (id - 1), -1) as x from src;
select column_name, is_nullable from information_schema.columns where table_schema = 'issue26830_strict_nullability' and table_name = 'coalesced' and column_name = 'x';
select id, x from coalesced order by id;

create table preserved_not_null as select id, v + 1 as plus_value, v = 10 as equals_value, row_number() over (order by id) as row_number_value, rank() over (order by id) as rank_value, dense_rank() over (order by id) as dense_rank_value, percent_rank() over (order by id) as percent_rank_value, ntile(2) over (order by id) as ntile_value, cume_dist() over (order by id) as cume_dist_value from src;
select column_name, is_nullable from information_schema.columns where table_schema = 'issue26830_strict_nullability' and table_name = 'preserved_not_null' and column_name <> 'id' order by ordinal_position;

drop database issue26830_strict_nullability;
