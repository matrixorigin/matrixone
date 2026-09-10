-- @suit

-- @case
-- @desc: Issue #28582: prepared numeric CAST rounds according to its source type.
-- @label:bvt
drop database if exists issue_28582_prepared_numeric_cast;
create database issue_28582_prepared_numeric_cast;
use issue_28582_prepared_numeric_cast;

prepare cast_numeric from
    'select cast(? as signed) as signed_value, cast(? as unsigned) as unsigned_value';

-- SQL numeric assignments preserve exact DECIMAL semantics.
set @decimal_value = 1.5;
execute cast_numeric using @decimal_value, @decimal_value;
set @decimal_value = -1.5;
execute cast_numeric using @decimal_value, @decimal_value;
set @decimal_value = 2.5;
execute cast_numeric using @decimal_value, @decimal_value;
set @decimal_value = -2.5;
execute cast_numeric using @decimal_value, @decimal_value;

-- Scientific notation is an approximate FLOAT parameter and keeps ties-to-even.
set @float_value = 2.5e0;
execute cast_numeric using @float_value, @float_value;
set @float_value = -2.5e0;
execute cast_numeric using @float_value, @float_value;

-- Ordinary strings retain integer-prefix cast semantics.
set @string_value = '1.5';
execute cast_numeric using @string_value, @string_value;
set @string_value = '-1.5';
execute cast_numeric using @string_value, @string_value;

deallocate prepare cast_numeric;

create table cast_target (id int primary key, value decimal(10,1));
prepare cast_insert from
    'insert into cast_target values (?, cast(? as signed))';
set @id = 1;
set @decimal_value = 1.5;
execute cast_insert using @id, @decimal_value;
set @id = 2;
set @decimal_value = -1.5;
execute cast_insert using @id, @decimal_value;
deallocate prepare cast_insert;
select id, value from cast_target order by id;

drop database issue_28582_prepared_numeric_cast;
