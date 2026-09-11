-- @suite
-- @case

drop database if exists group_concat_bool_28658;
create database group_concat_bool_28658;
use group_concat_bool_28658;

create table bool_values (id int primary key, b bool);
insert into bool_values values (1, false), (2, true), (3, null), (4, true);

select group_concat(b order by id separator '|') from bool_values;
select group_concat(distinct b order by b separator '|') from bool_values;
select group_concat(b, ':', cast(b as char) order by id separator '|') from bool_values;
select group_concat(b order by id separator '|') =
       group_concat(cast(b as char) order by id separator '|')
from bool_values;

prepare group_concat_bool_stmt from
    'select group_concat(b order by id separator \'|\') from bool_values';
execute group_concat_bool_stmt;
deallocate prepare group_concat_bool_stmt;

create view bool_concat_view as
    select group_concat(b order by id separator '|') as value from bool_values;
select value from bool_concat_view;

create table bool_concat_ctas as
    select group_concat(b order by id separator '|') as value from bool_values;
select value from bool_concat_ctas;

drop database group_concat_bool_28658;
