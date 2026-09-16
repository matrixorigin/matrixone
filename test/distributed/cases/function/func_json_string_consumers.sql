-- @suite
-- @case
-- @desc: JSON operands in CONCAT, CONCAT_WS, and ELT
-- @label:bvt
--- @metacmp(false)

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

create view json_string_view as select id, concat_ws('|', 'x', js, 'y') as c from src;
select id, c from json_string_view order by id;

create table json_string_ctas as select id, concat('', js) as c from src;
select id, c from json_string_ctas order by id;

prepare json_string_stmt from 'select id, concat_ws(''|'',''x'',js,''y'') as c from src order by id';
execute json_string_stmt;
deallocate prepare json_string_stmt;

drop database issue28905_json_string_consumers;
