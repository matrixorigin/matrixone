-- Old MatrixOne archives require explicit binary provenance; default strings remain text.
drop database if exists jsonline_legacy_binary;
create database jsonline_legacy_binary;
use jsonline_legacy_binary;
create table restored(id int, bin binary(2), raw varbinary(16), payload blob, txt varchar(16));
load data infile {'filepath'='$resources/load_data/jsonline_legacy_binary.jl','format'='jsonline','jsondata'='object','json_binary'='legacy_base64'} into table restored;
select id, coalesce(concat('0x',hex(bin)),'NULL') as b, coalesce(concat('0x',hex(raw)),'NULL') as v, coalesce(concat('0x',hex(payload)),'NULL') as p, coalesce(concat('<',txt,'>'),'NULL') as s from restored order by id;
create table plain_text(id int, raw varbinary(16));
load data infile {'filepath'='$resources/load_data/jsonline_legacy_binary.jl','format'='jsonline','jsondata'='object'} into table plain_text;
select id, coalesce(concat('0x',hex(raw)),'NULL') as v from plain_text order by id;
create external table legacy_external(id int, raw varbinary(16)) infile {'filepath'='$resources/load_data/jsonline_legacy_binary.jl','format'='jsonline','jsondata'='object','json_binary'='legacy_base64'};
select id, coalesce(concat('0x',hex(raw)),'NULL') as v from legacy_external order by id;
create table rejected(id int, raw varbinary(16));
load data infile {'filepath'='$resources/load_data/jsonline_legacy_binary_invalid.jl','format'='jsonline','jsondata'='object','json_binary'='legacy_base64'} into table rejected;
select count(*) from rejected;
drop database jsonline_legacy_binary;
