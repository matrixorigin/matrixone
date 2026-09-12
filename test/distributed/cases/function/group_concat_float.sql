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

drop database group_concat_float_28660;
