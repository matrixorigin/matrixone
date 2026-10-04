-- @suite

-- @case
-- @desc: MySQL-compatible default frames for ordered value window functions
-- @label:bvt

drop database if exists mysql_compat_window_default_frame;
create database mysql_compat_window_default_frame;
use mysql_compat_window_default_frame;

create table t (id int primary key, o int, v int);
insert into t values (1,1,20),(2,1,20),(3,2,30),(4,3,40);

-- An omitted ordered frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
select id,
  last_value(v) over (order by o) as lv,
  nth_value(v, 3) over (order by o) as nv
from t order by id;

-- The omitted and explicit default frames must be equivalent.
select id,
  last_value(v) over (
    order by o range between unbounded preceding and current row
  ) as lv,
  nth_value(v, 3) over (
    order by o range between unbounded preceding and current row
  ) as nv
from t order by id;

-- A full-partition frame remains observably different and is still honored.
select id,
  last_value(v) over (
    order by o rows between unbounded preceding and unbounded following
  ) as lv,
  nth_value(v, 3) over (
    order by o rows between unbounded preceding and unbounded following
  ) as nv
from t order by id;

drop database mysql_compat_window_default_frame;
show databases like 'mysql_compat_window_default_frame';
