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

-- Common-value producers own their policy; IF/CASE forward the selected branch.
set session sql_mode = 'TIME_TRUNCATE_FRACTIONAL';
select cast(coalesce(cast('2024-02-29 23:59:59.500000' as datetime(6)), null) as signed) as coalesce_dt,
cast(ifnull(cast('2024-02-29 23:59:59.500000' as datetime(6)), null) as signed) as ifnull_dt,
cast(if(true, cast('2024-02-29 23:59:59.500000' as datetime(6)), null) as signed) as if_dt,
cast(case when true then cast('-00:00:00.500000' as time(6)) else null end as unsigned) as case_tm;
select id, cast(coalesce(dt, null) as signed) as common_dt,
cast(ifnull(dt, null) as signed) as ifnull_dt,
cast(if(id = 2, dt, cast('2024-02-29 23:59:59.500000' as datetime(6))) as signed) as mixed_if,
cast(case when id = 2 then dt else cast('2024-02-29 23:59:59.500000' as datetime(6)) end as signed) as mixed_case,
cast(if(id = 2, tm, cast('-00:00:00.500000' as time(6))) as unsigned) as mixed_tm
from temporal_values where id in (2,3,4) order by id;
select id, cast(coalesce(if(id = 2, dt, cast('2024-02-29 23:59:59.500000' as datetime(6))), null) as signed) as nested_dt
from temporal_values where id in (2,3) order by id;

select id, cast(coalesce(cast(dt as datetime(6)), null) as signed) as text_dt,
cast(ifnull(cast(tm as time(6)), null) as unsigned) as text_tm
from temporal_text_values order by id;

-- Scalar output conversion must preserve empty, ordered, correlated and cardinality behavior.
select cast((select cast('2024-02-29 23:59:59.500000' as datetime(6))) as signed) as scalar_dt,
cast((select cast('-00:00:00.500000' as time(6))) as unsigned) as scalar_tm,
cast((select cast(dt as datetime(6)) from temporal_values where id = 0) as signed) as empty_dt;
select cast((select case when id = 2 then cast('2024-02-28 12:00:00.100000' as datetime(6)) else cast('2024-02-29 13:00:00.100000' as datetime(6)) end from temporal_values where id in (2,3) order by id desc limit 1) as signed) as ordered_dt;
select id, cast((select if(temporal_values.id = 2, temporal_values.dt, cast('2024-02-29 23:59:59.500000' as datetime(6)))) as signed) as correlated_dt
from temporal_values where id in (2,3) order by id;
select cast((select distinct cast(dt as datetime(6)) from temporal_values where id in (1,2)) as signed) as distinct_scalar;

set session sql_mode = '';
prepare temporal_wrapper_param from 'select cast(coalesce(cast(? as datetime(6)), null) as signed) as common_dt, cast(if(?, dt, cast(? as datetime(6))) as signed) as mixed_dt, cast((select cast(? as datetime(6))) as signed) as scalar_dt from temporal_values where id = 2';
set @temporal_wrapper_choice = 0;
execute temporal_wrapper_param using @temporal_integer_dt, @temporal_wrapper_choice, @temporal_integer_dt, @temporal_integer_dt;
set session sql_mode = 'TIME_TRUNCATE_FRACTIONAL';
execute temporal_wrapper_param using @temporal_integer_dt, @temporal_wrapper_choice, @temporal_integer_dt, @temporal_integer_dt;
set @temporal_wrapper_choice = 1;
execute temporal_wrapper_param using @temporal_integer_dt, @temporal_wrapper_choice, @temporal_integer_dt, @temporal_integer_dt;
set session sql_mode = '';
execute temporal_wrapper_param using @temporal_integer_dt, @temporal_wrapper_choice, @temporal_integer_dt, @temporal_integer_dt;
set @temporal_wrapper_null = null;
execute temporal_wrapper_param using @temporal_wrapper_null, @temporal_wrapper_choice, @temporal_wrapper_null, @temporal_wrapper_null;
deallocate prepare temporal_wrapper_param;

-- A correlated scalar's WHERE match must guard integer evaluation and preserve NULL.
select id, cast((select dt where id = 1) as signed) as filtered_dt,
cast((select tm where id = 1) as unsigned) as filtered_tm,
cast((select cast(dt as datetime(0)) where id = 1) as signed) as filtered_cast
from temporal_values order by id;

-- Compare temporal values first; only the selected LEAST/GREATEST result is converted.
set session sql_mode = 'TIME_TRUNCATE_FRACTIONAL';
select id, cast(least(dt,dt) as signed) as least_dt, cast(greatest(dt,dt) as signed) as greatest_dt,
cast(least(tm,tm) as unsigned) as least_tm, cast(greatest(tm,tm) as unsigned) as greatest_tm
from temporal_values order by id;
select cast(least(cast('2024-02-29 23:59:59.500000' as datetime(6)),cast('2024-03-01 00:00:00' as datetime(6))) as signed) as least_dt,
cast(greatest(cast('2024-02-29 23:59:59.500000' as datetime(6)),cast('2024-02-28 00:00:00' as datetime(6))) as signed) as greatest_dt,
cast(least(cast('-00:00:00.500000' as time(6)),cast('00:00:00' as time(6))) as unsigned) as least_tm,
cast(greatest(cast('-00:00:00.500000' as time(6)),cast('-00:00:01' as time(6))) as unsigned) as greatest_tm;
set session sql_mode = '';
prepare temporal_minmax_param from 'select cast(least(cast(? as datetime(6)),cast("2024-03-01 00:00:00" as datetime(6))) as signed) as least_dt, cast(greatest(cast(? as datetime(6)),cast("2024-02-28 00:00:00" as datetime(6))) as signed) as greatest_dt, cast(least(cast(? as time(6)),cast("00:00:00" as time(6))) as unsigned) as least_tm, cast(greatest(cast(? as time(6)),cast("-00:00:01" as time(6))) as unsigned) as greatest_tm';
execute temporal_minmax_param using @temporal_integer_dt,@temporal_integer_dt,@temporal_integer_tm,@temporal_integer_tm;
set session sql_mode = 'TIME_TRUNCATE_FRACTIONAL';
execute temporal_minmax_param using @temporal_integer_dt,@temporal_integer_dt,@temporal_integer_tm,@temporal_integer_tm;
set session sql_mode = '';
execute temporal_minmax_param using @temporal_integer_dt,@temporal_integer_dt,@temporal_integer_tm,@temporal_integer_tm;
execute temporal_minmax_param using @temporal_wrapper_null,@temporal_wrapper_null,@temporal_wrapper_null,@temporal_wrapper_null;
deallocate prepare temporal_minmax_param;

-- Every scalar exit retains its value owner, including outer ON and dependent programs.
set session sql_mode = 'TIME_TRUNCATE_FRACTIONAL';
select id, cast(cast(dt as datetime(6)) as signed) as direct_dt,
cast((select cast(dt as datetime(6))) as signed) as scalar_dt,
(select (select cast(dt as datetime(6)))) as original_dt,
cast((select (select cast(dt as datetime(6)))) as signed) as deep_dt
from temporal_values where id in (2,4) order by id;
select v.id,m.n from temporal_values v join (select 20240229235959 as n) m
on cast((select cast(v.dt as datetime(6))) as signed)=m.n where v.id in (2,4) order by v.id;
select v.id,m.n from temporal_values v left join (select 20240229235959 as n) m
on cast((select cast(v.dt as datetime(6))) as signed)=m.n where v.id in (2,4) order by v.id;
select v.id,m.n from (select 20240229235959 as n) m right join temporal_values v
on m.n=cast((select cast(v.dt as datetime(6))) as signed) where v.id in (2,4) order by v.id;
select id, cast((select (select if(id=2,dt,cast(dt as datetime(6))))) as signed) as deep_mixed,
cast((select (select cast(tm as time(6)))) as unsigned) as deep_tm
from temporal_values where id in (2,3,4) order by id;
-- Distinct temporal inputs must not collapse merely because their integers coincide.
select cast((select distinct if(t.id=1,v.dt,cast('2024-02-29 23:59:59.100000' as datetime(6)))
from temporal_values t where t.id in (1,2)) as signed) as correlated_distinct
from temporal_values v where v.id=1;
set session sql_mode = '';
prepare temporal_exit_param from 'select v.id, cast((select (select cast(v.dt as datetime(6)))) as signed) as deep_dt,m.n from temporal_values v left join (select 20240229235959 as n) m on cast((select cast(v.dt as datetime(6))) as signed)=m.n where v.id=?';
set @temporal_exit_id = 2;
execute temporal_exit_param using @temporal_exit_id;
set session sql_mode = 'TIME_TRUNCATE_FRACTIONAL';
execute temporal_exit_param using @temporal_exit_id;
set session sql_mode = '';
execute temporal_exit_param using @temporal_exit_id;
set @temporal_exit_id = 4;
execute temporal_exit_param using @temporal_exit_id;
deallocate prepare temporal_exit_param;

-- Alias consumers must not rewrite the selected scalar's value or datetime metadata.
select (select cast('2024-02-29 23:59:59.500000' as datetime(6))) as d order by cast(d as signed);
select id,(select cast(dt as datetime(6))) as scalar_d from temporal_values
where id in (1,2,4) order by cast(scalar_d as signed),cast(scalar_d as unsigned),id desc;
select id,coalesce((select cast(dt as datetime(6))),null) as scalar_d from temporal_values
where id in (2,3,4) having cast(scalar_d as signed)>20240229235959 order by cast(scalar_d as unsigned),id;
prepare temporal_alias_order from 'select id,(select (select cast(dt as datetime(6)))) as scalar_d from temporal_values where id in (1,2,4) order by cast(scalar_d as signed),id desc';
execute temporal_alias_order;
set session sql_mode = 'TIME_TRUNCATE_FRACTIONAL';
execute temporal_alias_order;
select id,coalesce((select cast(dt as datetime(6))),null) as scalar_d from temporal_values
where id in (2,3,4) having cast(scalar_d as signed)>20240229235959 order by cast(scalar_d as unsigned),id;
set session sql_mode = '';
execute temporal_alias_order;
deallocate prepare temporal_alias_order;
drop table temporal_text_values;

drop database temporal_integer_cast;
set time_zone = @temporal_integer_saved_zone;
set session sql_mode = @temporal_integer_saved_mode;
