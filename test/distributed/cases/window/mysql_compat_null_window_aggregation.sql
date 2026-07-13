-- @suite

-- @case
-- @desc: MySQL compatibility cases for NULL with aggregate window frames
-- @label:bvt

drop database if exists mysql_compat_null_window_aggregation;
create database mysql_compat_null_window_aggregation;
use mysql_compat_null_window_aggregation;

create table t_win_null (
  id int primary key,
  grp int,
  ord int,
  v int
);

insert into t_win_null values
  (1, 1, 1, null),
  (2, 1, 2, 10),
  (3, 1, 3, null),
  (4, 1, 4, 20),
  (5, 2, 1, null),
  (6, 2, 2, null),
  (7, 2, 3, 5);

select id, grp, ord,
       count(v) over (partition by grp order by ord rows between 1 preceding and current row) as cnt_v_prev_cur,
       sum(v) over (partition by grp order by ord rows between 1 preceding and current row) as sum_v_prev_cur,
       cast(avg(v) over (partition by grp order by ord rows between 1 preceding and current row) as decimal(10,4)) as avg_v_prev_cur,
       min(v) over (partition by grp order by ord rows between 1 preceding and current row) as min_v_prev_cur,
       max(v) over (partition by grp order by ord rows between 1 preceding and current row) as max_v_prev_cur,
       count(*) over (partition by grp order by ord rows between 1 preceding and 1 preceding) as cnt_all_prev_only,
       count(v) over (partition by grp order by ord rows between 1 preceding and 1 preceding) as cnt_v_prev_only,
       sum(v) over (partition by grp order by ord rows between 1 preceding and 1 preceding) as sum_v_prev_only
from t_win_null
order by grp, ord, id;

drop database mysql_compat_null_window_aggregation;
