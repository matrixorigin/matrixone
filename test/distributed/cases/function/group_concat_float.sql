-- @suite
-- @case

drop database if exists group_concat_float_28660;
create database group_concat_float_28660;
use group_concat_float_28660;

create table float_values (id int primary key, f float, d double);
insert into float_values values
    (1, 1.5, 1.5),
    (2, 1e20, 1e20),
    (3, 1e-7, 1e-7),
    (4, 1e-14, 1e-14),
    (5, -0.0, -0.0),
    (6, null, null);

select group_concat(f order by id separator '|') from float_values;
select group_concat(d order by id separator '|') from float_values;
select group_concat(d order by id separator '|') =
       group_concat(cast(d as char) order by id separator '|')
from float_values;
select group_concat(distinct d order by d separator '|') from float_values;

prepare group_concat_float_stmt from
    'select group_concat(d order by id separator \'|\') from float_values';
execute group_concat_float_stmt;
deallocate prepare group_concat_float_stmt;

-- Approximate numeric text uses MySQL bytes across scalar and JSON consumers.
select hex(cast(cast('-1e20' as double) as char)) as negative_scalar_hex, hex(cast(json_array(cast('-1e20' as double)) as char)) as negative_json_hex;
select id, hex(cast(d as char)) as scalar_hex, hex(concat(d)) as concat_hex from float_values where id in (2,3,4) order by id;
select hex(cast(greatest(9007199254740993,cast(9007199254740992 as double)) as char)) as greatest_hex, hex(cast(least(9007199254740993,cast(9007199254740992 as double)) as char)) as least_hex;
select id, json_type(json_extract(json_array(d),'$[0]')) as numeric_type, hex(cast(json_array(d) as char)) as json_hex, hex(json_unquote(json_extract(json_array(d),'$[0]'))) as unquote_hex, json_contains(json_array(d),cast(d as char)) as contains_value from float_values where id in (2,3,4) order by id;
select id, cast(json_arrayagg(d) as char) = concat('[',cast(d as char),']') as array_text, cast(json_objectagg(cast(id as char),d) as char) = concat('{"',cast(id as char),'": ',cast(d as char),'}') as object_text from float_values where id in (2,3,4) group by id order by id;
select id, cast(json_set(json_object(),'$.v',d) as char) = concat('{"v": ',cast(d as char),'}') as set_text from float_values where id in (2,3,4) order by id;
select cast(json_array(cast(2 as double),cast(2 as signed),cast('2.00' as decimal(3,2)),'2',null) as char) as json_controls;
select cast(cast('1e-15' as double) as char) as fixed_boundary, cast(cast('1e-16' as double) as char) as exponent_boundary, cast(cast('1000000000000000.2' as double) as char) as fractional_large;
insert into float_values values (7,1.23456789,1.23456789);
select cast(f as char) as float_text, cast(d as char) as double_text, group_concat(f) as float_aggregate from float_values where id = 7 group by f,d;
select cast(null as char) is null as null_cast, concat(cast(null as double)) is null as null_concat, cast(cast('123.450' as decimal(6,3)) as char) as decimal_control;

-- Implicit string casts must retain every digit of long fixed DOUBLE text.
select concat(cast('-1.2345678901234567e-15' as double)) as literal_concat;
insert into float_values (id,d) values
    (8,cast('-1.2345678901234567e-15' as double)),
    (9,cast('1.2345678901234567e-15' as double)),
    (10,cast('-1.2345678901234567e-14' as double)),
    (11,cast('-1.2345678901234567e-16' as double));
select id, cast(d as char) as scalar_text, concat(d) as concat_text, concat_ws('',d) as concat_ws_text, quote(d) as quoted_text from float_values where id >= 8 order by id;

-- Stored expressions preserve implicit precision and explicit CHAR limits.
create table float_generated (id int primary key, a double, g varchar(100) generated always as (concat(a)) stored, e varchar(100) generated always as (concat(cast(a as char(32)))) stored, r varchar(200) generated always as (right(concat(a),10)) stored, rep varchar(200) generated always as (repeat(concat(a),2)) stored, c varchar(200) generated always as (coalesce(concat(a),'')) stored, ci varchar(200) generated always as (if(a<0,concat(a),'fallback')) stored, cs varchar(200) generated always as (case when a<0 then concat(a) else '' end) stored, cn varchar(200) generated always as (ifnull(concat(a),'fallback')) stored, d varchar(200) default (coalesce(concat(a),'')));
insert into float_generated (id,a) values (1,cast('-1.2345678901234567e-15' as double)), (2,null);
select g as generated_text, e as explicit_text, r as right_text, rep as repeat_text, c as coalesce_text, ci as if_text, cs as case_text, cn as ifnull_text, concat('[',d,']') as default_text from float_generated order by id;
update float_generated set a = cast('1.2345678901234567e-15' as double), d = default where id = 1;
select g as generated_text, e as explicit_text, r as right_text, rep as repeat_text, c as coalesce_text, ci as if_text, cs as case_text, cn as ifnull_text, concat('[',d,']') as default_text from float_generated order by id;

-- Persisted FORMAT and internal SET bindings compose with widened float text.
create table float_composed (id int primary key, a double, b int, s set('a','b'), g varchar(200) generated always as (concat(a,format(b,2))) stored, f varchar(200) generated always as (concat(a,find_in_set('b',s))) stored, n varchar(200) generated always as (concat(a,format(concat(a),2))) stored);
insert into float_composed (id,a,b,s) values (1,-1.2345678901234567e-15,1,'a,b');
select g = '-0.00000000000000123456789012345681.00' as format_sibling, f = '-0.00000000000000123456789012345682' as set_sibling, n = concat(cast(a as char),format(concat(a),2)) as nested_format from float_composed;
update float_composed set a = 1.2345678901234567e-15;
select g = '0.00000000000000123456789012345681.00' as format_sibling, f = '0.00000000000000123456789012345682' as set_sibling, n = concat(cast(a as char),format(concat(a),2)) as nested_format from float_composed;

-- IN owns list conversions, including comparisons expanded around NULL.
create table float_membership (id int primary key, a double, c varchar(100), g bool generated always as (concat(a) in ('1','2',c)) stored, n bool generated always as (concat(a) not in ('1','2',c)) stored, z bool generated always as (concat(a) in ('1',null,c)) stored, e bool generated always as (concat(a) in ('1','2',cast(c as char(32)))) stored);
insert into float_membership (id,a,c) values (1,-1.2345678901234567e-15,'-0.0000000000000012345678901234568'),(2,1.5,'other'),(3,null,'-0.0000000000000012345678901234568');
select id,g,n,z,e from float_membership order by id;
update float_membership set a=1.5,c='1.5' where id=1;
select id,g,n,z,e from float_membership where id=1;

-- Proof-only historical folding must not leak into newly authored defaults.
create table float_folded (id int primary key,a double,g varchar(100) default (concat(a,cast('1e20' as double))),j varchar(100) default (json_unquote(json_array(cast('1e20' as double)))));
insert into float_folded (id,a) values (1,-1.2345678901234567e-15);
select g = '-0.00000000000000123456789012345681e20' as current_default,j = '[1e20]' as current_json from float_folded;
update float_folded set a=1.2345678901234567e-15,g=default;
select g = '0.00000000000000123456789012345681e20' as current_default,j = '[1e20]' as current_json from float_folded;

-- Independent MySQL byte oracles for approximate numeric consumers.
create table float_compat_oracle (id int primary key, d double);
insert into float_compat_oracle values
    (1,cast('1e20' as double)),
    (2,cast('-1e20' as double)),
    (3,cast('1e-7' as double)),
    (4,cast('1e14' as double)),
    (5,cast('1e-8' as double));
-- B01-B08: each consumer independently prints the fixed MySQL bytes.
select id, hex(cast(d as char)) as cast_hex, hex(concat(d)) as concat_hex,
       hex(group_concat(d)) as aggregate_hex
from float_compat_oracle group by id,d order by id;
-- G03: preserve the original numerical result in addition to G01/G02 HEX.
select greatest(9007199254740993,cast(9007199254740992 as double)) = 9007199254740992 as greatest_value,
       least(9007199254740993,cast(9007199254740992 as double)) = 9007199254740992 as least_value;
-- J01/J02/J06/J07: all three reported DOUBLE values, including the negative.
select id, json_type(json_extract(json_array(d),'$[0]')) as numeric_type,
       hex(cast(json_array(d) as char)) as array_hex,
       hex(json_unquote(json_extract(json_array(d),'$[0]'))) as unquote_hex,
       json_contains(json_array(d),cast(d as char)) as contains_value
from float_compat_oracle where id <= 3 order by id;
-- J03/J04: singleton groups avoid aggregate-order dependence.
select id, hex(cast(json_arrayagg(d) as char)) as arrayagg_hex,
       hex(cast(json_objectagg(cast(id as char),d) as char)) as objectagg_hex
from float_compat_oracle where id <= 3 group by id order by id;
-- J05: complete object bytes, including MySQL's colon-space.
select id, hex(cast(json_set(json_object(),'$.v',d) as char)) as set_hex
from float_compat_oracle where id <= 3 order by id;
drop table float_compat_oracle;

drop database group_concat_float_28660;
