-- @suite

-- @case
-- @desc: LAG and LEAD reject negative offsets
-- @label:bvt

drop database if exists mysql_compat_window_lag_lead_negative_offset;
create database mysql_compat_window_lag_lead_negative_offset;
use mysql_compat_window_lag_lead_negative_offset;

create table t (id int primary key, v int);
insert into t values (1, 10), (2, 20), (3, 30);

-- Literal offsets are rejected while binding, with and without a default.
select id, lag(v, -1) over (order by id) from t order by id;
select id, lag(v, -2, 99) over (order by id) from t order by id;
select id, lead(v, -1) over (order by id) from t order by id;
select id, lead(v, -2, 88) over (order by id) from t order by id;

-- Constant and row-dependent expressions cannot bypass validation.
select id, lag(v, 0 - 1) over (order by id) from t order by id;
select id, lead(v, id - 2, 88) over (order by id) from t order by id;

-- Prepared offsets are checked on every execution. A failed execution must
-- leave the prepared statement usable for zero and positive controls.
prepare lag_offset from 'select id, lag(v, ?) over (order by id) lag_v from t order by id';
set @offset = -1;
execute lag_offset using @offset;
set @offset = 0;
execute lag_offset using @offset;
set @offset = 1;
execute lag_offset using @offset;
deallocate prepare lag_offset;

prepare lead_offset from 'select id, lead(v, ?, 88) over (order by id) lead_v from t order by id';
set @offset = -2;
execute lead_offset using @offset;
set @offset = 0;
execute lead_offset using @offset;
set @offset = 1;
execute lead_offset using @offset;
deallocate prepare lead_offset;

drop database mysql_compat_window_lag_lead_negative_offset;
