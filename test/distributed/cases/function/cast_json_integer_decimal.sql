-- An exact JSON integer casts to DECIMAL without a float64 round trip (issue #31669).
select cast(json_extract('{"n":9007199254740993}', '$.n') as decimal(38,0));
select cast(json_extract('{"n":9007199254740995}', '$.n') as decimal(38,0)), cast(json_extract('{"n":-9007199254740993}', '$.n') as decimal(38,0));
select cast(json_extract('{"n":9223372036854775807}', '$.n') as decimal(38,0)), cast(json_extract('{"n":18446744073709551615}', '$.n') as decimal(38,2));
select cast(json_extract('{"n":9007199254740993}', '$.n') as decimal(18,0)), cast(json_extract('{"n":12}', '$.n') as decimal(10,2));
select cast(json_extract('{"n":1.25}', '$.n') as decimal(10,2)), cast(json_extract('{"n":null}', '$.n') as decimal(10,2));
select cast(json_extract('{"n":123456}', '$.n') as decimal(5,0));
drop database if exists json_int_dec;
create database json_int_dec;
use json_int_dec;
create table json_numbers(id int primary key, doc json);
insert into json_numbers values (1,'{"n":9007199254740992}'), (2,'{"n":9007199254740993}');
select id, json_unquote(json_extract(doc,'$.n')) as original_value, cast(json_extract(doc,'$.n') as decimal(38,0)) as direct_cast from json_numbers order by id;
select cast(json_extract(doc,'$.n') as decimal(38,0)) as k, count(*) from json_numbers group by k order by k;
drop database json_int_dec;
