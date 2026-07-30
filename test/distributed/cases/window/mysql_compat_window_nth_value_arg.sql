-- @suite

-- @case
-- @desc: MySQL compatibility for NTH_VALUE offset argument validation
-- @label:bvt

drop database if exists mysql_compat_window_nth_value_arg;
create database mysql_compat_window_nth_value_arg;
use mysql_compat_window_nth_value_arg;

create table t (id int primary key, grp varchar(8), txt text);
insert into t values
  (1,'A','alpha'),(2,'A','beta'),(3,'A','gamma'),
  (4,'B','delta'),(5,'B','epsilon');

-- MySQL rejects a row-dependent NTH_VALUE offset.
select nth_value(txt, id - 1) over (
  partition by grp order by id
  rows between unbounded preceding and unbounded following
) nth_txt
from t;

-- MySQL requires the NTH_VALUE offset to be positive.
select nth_value(txt, 0) over (
  partition by grp order by id
  rows between unbounded preceding and unbounded following
) nth_txt
from t;

-- Constant expressions remain valid after binding-time validation.
select id, nth_value(id, 1 + 1) over (
  partition by grp order by id
  rows between unbounded preceding and unbounded following
) nth_id
from t order by grp, id;

drop database mysql_compat_window_nth_value_arg;
