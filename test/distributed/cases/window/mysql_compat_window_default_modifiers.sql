-- @suite

-- @case
-- @desc: MySQL default value-window modifiers are accepted explicitly
-- @label:bvt

drop database if exists mysql_compat_window_default_modifiers;
create database mysql_compat_window_default_modifiers;
use mysql_compat_window_default_modifiers;

create table t (id int primary key, g int, ordv int, v int);
insert into t values
  (1,1,1,10),(2,1,2,20),(3,1,3,null),(4,1,4,40),
  (5,2,1,50),(6,2,2,60);

-- RESPECT NULLS is the default null treatment. The NULL predecessor remains visible.
select id,
  lag(v) respect nulls over (partition by g order by ordv, id) explicit_lag,
  lag(v) over (partition by g order by ordv, id) default_lag
from t order by id;

-- FROM FIRST is the default NTH_VALUE direction.
select id,
  nth_value(v, 2) from first over (
    partition by g order by ordv, id
    rows between unbounded preceding and unbounded following
  ) explicit_nth,
  nth_value(v, 2) over (
    partition by g order by ordv, id
    rows between unbounded preceding and unbounded following
  ) default_nth
from t order by id;

drop database mysql_compat_window_default_modifiers;
show databases like 'mysql_compat_window_default_modifiers';
