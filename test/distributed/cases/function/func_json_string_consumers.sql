-- @suite
-- @case
-- @desc: JSON operands in CONCAT, CONCAT_WS, and ELT
-- @label:bvt

drop database if exists issue28905_json_string_consumers;
create database issue28905_json_string_consumers;
use issue28905_json_string_consumers;

create table src (id int primary key, js json, v varchar(20));
insert into src values
    (1, '{"b":2,"a":1}', 'A'),
    (2, '1', ''),
    (3, 'null', null),
    (4, null, null);

select id, concat('', js) as concat_left, concat(js, '') as concat_right, concat('', js) is null as concat_left_is_null, concat(js, '') is null as concat_right_is_null, concat_ws('|', 'x', js, 'y') as concat_ws_middle, concat_ws('|', js, v) as concat_ws_column, concat_ws('|', js, v) = '' as concat_ws_empty from src order by id;
select id, elt(2, 'fallback', js, 'other') as elt_selected from src order by id;

select id, elt(2, 'fallback', js, 'other') is null as selected_is_null from src order by id;
create view json_elt_view as select id, elt(2, 'fallback', js) as c from src;
select id, c, c is null as selected_is_null from json_elt_view order by id;
create table json_elt_ctas as select id, elt(2, 'fallback', js) as c from src;
select id, c, c is null as selected_is_null from json_elt_ctas order by id;
prepare json_elt_column_stmt from 'select id, elt(2, ''fallback'', js) as c, elt(2, ''fallback'', js) is null as selected_is_null from src order by id';
execute json_elt_column_stmt;
deallocate prepare json_elt_column_stmt;


select concat(cast('{"k":1}' as json), '!') as constant_concat, concat_ws('|', cast('[1,2]' as json), 'x') as constant_concat_ws, elt(1, cast('{"k":1}' as json), 'fallback') as constant_elt;

create view json_string_view as select id, concat_ws('|', 'x', js, 'y') as c from src;
select id, c from json_string_view order by id;

create table json_string_ctas as select id, concat('', js) as c from src;
select id, c from json_string_ctas order by id;

set @json_string_param = '{"p":1}';
prepare json_string_stmt from 'select id, concat(cast(? as json), js) as c from src order by id';
-- @metacmp(true)
execute json_string_stmt using @json_string_param;
set @json_string_param = '[2]';
-- @metacmp(true)
execute json_string_stmt using @json_string_param;
deallocate prepare json_string_stmt;


-- The binary/text controls assert physical source and non-object behavior;
-- they do not assert the unclosed opaque visible rendering contract.
prepare json_kind_stmt from 'select case when ? is null then ''NULL'' else charset(?) end as source_charset, coalesce(json_type(cast(? as json)) = ''OBJECT'', false) as parsed_object, cast(? as json) is null as value_is_null';
set @json_string_param = binary '{"p":1}';
execute json_kind_stmt using @json_string_param, @json_string_param, @json_string_param, @json_string_param;
set @json_string_param = '{"p":1}';
execute json_kind_stmt using @json_string_param, @json_string_param, @json_string_param, @json_string_param;
set @json_string_param = binary '{"p":1}';
execute json_kind_stmt using @json_string_param, @json_string_param, @json_string_param, @json_string_param;
set @json_string_param = null;
execute json_kind_stmt using @json_string_param, @json_string_param, @json_string_param, @json_string_param;
deallocate prepare json_kind_stmt;

select elt(1, 'ok', cast('bad' as json)) as selected_text, elt(null, cast('bad' as json), 'ok') is null as null_selector, elt(0, cast('bad' as json), 'ok') is null as zero_selector, elt(3, cast('bad' as json), 'ok') is null as outside_selector;
prepare json_elt_lazy_stmt from 'select elt(?, ''ok'', cast(''bad'' as json)) as selected_text';
set @json_elt_selector = 1;
execute json_elt_lazy_stmt using @json_elt_selector;
set @json_elt_selector = null;
execute json_elt_lazy_stmt using @json_elt_selector;
set @json_elt_selector = 0;
execute json_elt_lazy_stmt using @json_elt_selector;
deallocate prepare json_elt_lazy_stmt;

create table lazy_src(id int primary key, n int, js json, txt varchar(20));
insert into lazy_src values (1, 1, '{"p":1}', 'bad'), (2, 2, '{"p":1}', '[2]');
select id, elt(n, js, cast(txt as json)) as selected_value from lazy_src order by id;

-- Folding a parent must preserve both lazy selection and selected diagnostics.
select concat(elt(1, 'ok', cast('bad' as json)), '!') as lazy_parent, elt(1, elt(1, 'ok', cast('bad' as json)), cast('bad' as json)) as lazy_nested;
select elt(1, 'ok', cast(bit_count('3x') as json)) as unselected_warning;
show count(*) warnings;
select concat(elt(1, cast(bit_count('3x') as json), 'ok'), '!') as selected_warning;
show count(*) warnings;
select elt(1, cast(cast('a' as binary(1)) as json)) = '"base64:type254:YQ=="' as binary_subtype;
create table opaque_stored(j json);
insert into opaque_stored values (cast(cast(repeat('a',58) as blob) as json));
select elt(1,j) = concat('"base64:type252:',repeat('YWFh',19),char(92),'nYQ=="') as wrapped_elt, concat('',j) = concat('"base64:type252:',repeat('YWFh',19),char(92),'nYQ=="') as wrapped_stored from opaque_stored;
select (select group_concat(j) from opaque_stored) = (select concat('',j) from opaque_stored) as group_concat_json_visible, (select listagg(j, '') within group (order by 1) from opaque_stored) = (select concat('',j) from opaque_stored) as listagg_json_visible;
select json_schema_valid('{"type":"string","maxLength":95}',j) as rejects_compact_length, json_schema_valid('{"type":"string","maxLength":96}',j) as accepts_visible_length from opaque_stored;

-- Remaining functions reported in issue #28905 consume JSON visible text
-- through the same value-preserving cast used by explicit CAST(js AS CHAR).
create table remaining(id int primary key, js json, bits bigint);
insert into remaining values
    (1, '{"b":2,"a":1}', 256),
    (2, '[1,true,"x"]', 224),
    (3, '1', 384),
    (4, 'null', 123),
    (5, null, null);
select id, md5(js), sha1(js), sha2(js,256) from remaining order by id;
select id,
    md5(js) <=> md5(cast(js as char)) as md5_ok,
    sha1(js) <=> sha1(cast(js as char)) as sha1_ok,
    sha2(js,256) <=> sha2(cast(js as char),256) as sha2_const_ok,
    sha2(js,bits) <=> sha2(cast(js as char),bits) as sha2_dynamic_ok,
    compress(js) <=> compress(cast(js as char)) as compress_ok,
    quote(js) <=> quote(cast(js as char)) as quote_ok,
    soundex(js) <=> soundex(cast(js as char)) as soundex_ok
from remaining order by id;
create view remaining_hash_view as select id, md5(js) as h from remaining;
create table remaining_hash_ctas as select id, sha1(js) as h from remaining;
select count(*) as view_rows from remaining_hash_view;
select count(*) as ctas_rows from remaining_hash_ctas;
set @json_param = '{"p":1}';
prepare remaining_hash_stmt from 'select md5(cast(? as json)), sha1(cast(? as json)), sha2(cast(? as json),256)';
execute remaining_hash_stmt using @json_param, @json_param, @json_param;
set @json_param = '[2]';
execute remaining_hash_stmt using @json_param, @json_param, @json_param;
set @json_param = null;
execute remaining_hash_stmt using @json_param, @json_param, @json_param;
deallocate prepare remaining_hash_stmt;
drop database issue28905_json_string_consumers;
