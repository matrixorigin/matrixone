-- @suite
-- @case
-- @desc: Temporal integer casts round in the calendar and clock domains
-- @label:bvt

set @temporal_integer_saved_mode = @@session.sql_mode;
set @temporal_integer_saved_zone = @@session.time_zone;
set session sql_mode = '';
set time_zone = '+00:00';
drop database if exists temporal_integer_cast;
create database temporal_integer_cast;
use temporal_integer_cast;
create table temporal_values(id int, d date, dt datetime(6), ts timestamp(6), tm time(6));
insert into temporal_values values
(1, '2024-02-29', '2024-02-29 23:59:59.499999', '2024-02-29 23:59:59.499999', '12:59:59.499999'),
(2, '2024-02-29', '2024-02-29 23:59:59.500000', '2024-02-29 23:59:59.500000', '12:59:59.500000'),
(3, '2024-12-31', '2024-12-31 23:59:59.999999', '2024-12-31 23:59:59.999999', '-00:00:00.500000'),
(4, null, null, null, null);

-- Flat column inputs exercise binding and vector conversion, including NULL.
select id, cast(d as signed) as d_signed, cast(d as unsigned) as d_unsigned,
cast(dt as signed) as dt_signed, cast(dt as unsigned) as dt_unsigned,
cast(ts as signed) as ts_signed, cast(ts as unsigned) as ts_unsigned,
cast(tm as signed) as tm_signed, cast(tm as unsigned) as tm_unsigned
from temporal_values order by id;

-- Constant folding must agree with column execution; decimal keeps its fraction.
select cast(cast('2024-02-29 23:59:59.99' as datetime(2)) as signed) as dt_signed,
convert(cast('2024-02-29 23:59:59.99' as datetime(2)), unsigned) as dt_unsigned,
cast(cast('12:00:00.5' as time(1)) as signed) as tm_signed,
cast(cast('2024-02-29 23:59:59.99' as datetime(2)) as decimal(20,2)) as dt_decimal;
select cast(cast('9999-12-31 23:59:59.999999' as datetime(6)) as signed) as dt_max,
cast(cast('838:59:59.999999' as time(6)) as signed) as tm_max,
cast(cast('-838:59:59.999999' as time(6)) as signed) as tm_min;

-- TIMESTAMP uses the session's civil clock, including the date carry.
set time_zone = '+08:00';
select cast(ts as signed) as ts_signed, cast(ts as unsigned) as ts_unsigned
from temporal_values where id = 2;
set time_zone = '+00:00';

-- Stored temporal values keep integer rounding across prepared reuse and SQL modes.
prepare temporal_integer_query from 'select cast(dt as signed) as dt_signed, cast(dt as unsigned) as dt_unsigned, cast(ts as signed) as ts_signed, cast(tm as signed) as tm_signed, cast(tm as unsigned) as tm_unsigned from temporal_values where id = 2';
execute temporal_integer_query;
set session sql_mode = 'TIME_TRUNCATE_FRACTIONAL';
execute temporal_integer_query;
select id, cast(dt as signed) as dt_signed, cast(ts as unsigned) as ts_unsigned,
cast(tm as signed) as tm_signed, cast(tm as unsigned) as tm_unsigned
from temporal_values where id in (2, 3) order by id;
set session sql_mode = '';
execute temporal_integer_query;
deallocate prepare temporal_integer_query;

-- An explicit temporal CAST has a mode-sensitive integer policy, unlike storage columns.
create table temporal_text_values(id int, dt varchar(40), tm varchar(40));
insert into temporal_text_values values
(1, '2024-02-29 23:59:59.500000', '-00:00:00.500000'), (2, null, null);
select id, cast(cast(dt as datetime(6)) as signed) as dt_signed,
cast(cast(tm as time(6)) as unsigned) as tm_unsigned
from temporal_text_values order by id;
prepare temporal_integer_param from 'select cast(cast(? as datetime(6)) as signed) as dt_signed, cast(cast(? as time(6)) as unsigned) as tm_unsigned';
set @temporal_integer_dt = '2024-02-29 23:59:59.500000';
set @temporal_integer_tm = '-00:00:00.500000';
execute temporal_integer_param using @temporal_integer_dt, @temporal_integer_tm;
set session sql_mode = 'TIME_TRUNCATE_FRACTIONAL';
select cast(cast('2024-02-29 23:59:59.500000' as datetime(6)) as signed) as dt_signed,
cast(cast('-00:00:00.500000' as time(6)) as unsigned) as tm_unsigned;
select id, cast(cast(dt as datetime(6)) as signed) as dt_signed,
cast(cast(tm as time(6)) as unsigned) as tm_unsigned
from temporal_text_values order by id;
select cast(dt as signed) as stored_dt, cast(cast(dt as datetime(6)) as signed) as recast_dt,
cast(tm as unsigned) as stored_tm, cast(cast(tm as time(6)) as unsigned) as recast_tm
from temporal_values where id = 3;
execute temporal_integer_param using @temporal_integer_dt, @temporal_integer_tm;
set session sql_mode = '';
execute temporal_integer_param using @temporal_integer_dt, @temporal_integer_tm;
deallocate prepare temporal_integer_param;
drop table temporal_text_values;

drop database temporal_integer_cast;
set time_zone = @temporal_integer_saved_zone;
set session sql_mode = @temporal_integer_saved_mode;
