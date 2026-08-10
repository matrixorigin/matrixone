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

-- issue #26825: non-NULL aggregate window output can materialize into NOT NULL and CTAS targets.
create table t_win_not_null_src (id int primary key, v int not null);
insert into t_win_not_null_src values (1, 5), (2, 1), (3, 9);
create table t_win_not_null_dst (id int not null, sum_v bigint not null, min_v int not null, max_v int not null);
insert into t_win_not_null_dst select id, sum(v) over (order by id rows between unbounded preceding and current row), min(v) over (order by id rows between unbounded preceding and current row), max(v) over (order by id rows between unbounded preceding and current row) from t_win_not_null_src order by id;
select * from t_win_not_null_dst order by id;
create table t_win_not_null_ctas as select id, sum(v) over (order by id rows between unbounded preceding and current row) as sum_v, min(v) over (order by id rows between unbounded preceding and current row) as min_v, max(v) over (order by id rows between unbounded preceding and current row) as max_v from t_win_not_null_src order by id;
select * from t_win_not_null_ctas order by id;
create table t_win_not_null_true_null (sum_v bigint not null);
insert into t_win_not_null_true_null select sum(v) over (order by id rows between 1 preceding and 1 preceding) from t_win_not_null_src order by id;
select count(*) as row_count from t_win_not_null_true_null;

drop database mysql_compat_null_window_aggregation;
