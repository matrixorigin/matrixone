-- @suite

-- @case
-- @desc: MySQL compatibility cases for NULL with statistical aggregate functions
-- @label:bvt

drop database if exists mysql_compat_null_stat_aggregation;
create database mysql_compat_null_stat_aggregation;
use mysql_compat_null_stat_aggregation;

create table t_stat (
  grp int,
  v int
);

select variance(v) as empty_variance,
       var_pop(v) as empty_var_pop,
       var_samp(v) as empty_var_samp,
       std(v) as empty_std,
       stddev_samp(v) as empty_stddev_samp
from t_stat;

insert into t_stat values
  (1, null),
  (1, null),
  (2, 5),
  (2, null),
  (3, 5),
  (3, 7),
  (3, null);

select grp,
       count(*) as count_all,
       count(v) as count_v,
       cast(variance(v) as decimal(10,4)) as variance_v,
       cast(var_pop(v) as decimal(10,4)) as var_pop_v,
       cast(var_samp(v) as decimal(10,4)) as var_samp_v,
       cast(std(v) as decimal(10,4)) as std_v,
       cast(stddev_samp(v) as decimal(10,4)) as stddev_samp_v
from t_stat
group by grp
order by grp;

drop database mysql_compat_null_stat_aggregation;
