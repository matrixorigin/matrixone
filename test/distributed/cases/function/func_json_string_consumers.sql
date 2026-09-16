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

select concat(cast('{"k":1}' as json), '!') as constant_concat, concat_ws('|', cast('[1,2]' as json), 'x') as constant_concat_ws, elt(1, cast('{"k":1}' as json), 'fallback') as constant_elt;

create view json_string_view as select id, concat_ws('|', 'x', js, 'y') as c from src;
select id, c from json_string_view order by id;

create table json_string_ctas as select id, concat('', js) as c from src;
select id, c from json_string_ctas order by id;

set @json_string_param = '{"p":1}';
prepare json_string_stmt from 'select id, concat(cast(? as json), js) as c from src order by id';
execute json_string_stmt using @json_string_param;
set @json_string_param = '[2]';
execute json_string_stmt using @json_string_param;
deallocate prepare json_string_stmt;

drop database issue28905_json_string_consumers;
