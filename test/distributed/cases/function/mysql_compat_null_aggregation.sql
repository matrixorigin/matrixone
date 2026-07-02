-- @suite

-- @case
-- @desc: MySQL compatibility cases for NULL with aggregation, grouping, distinct, and ordering
-- @label:bvt

drop database if exists mysql_compat_null_aggregation;
create database mysql_compat_null_aggregation;
use mysql_compat_null_aggregation;

create table t_null_agg (
  id int primary key,
  g int,
  v int,
  txt varchar(8)
);

select count(*) as empty_count_all,
       count(v) as empty_count_v,
       count(distinct v) as empty_count_distinct_v,
       sum(v) as empty_sum_v,
       avg(v) as empty_avg_v,
       min(v) as empty_min_v,
       max(v) as empty_max_v,
       group_concat(txt order by txt separator '|') as empty_gc
from t_null_agg;

insert into t_null_agg values
  (1, null, null, 'n1'),
  (2, null, 10,   'n2'),
  (3, null, null, null),
  (4, 1,    null, 'a'),
  (5, 1,    5,    'b'),
  (6, 1,    5,    null),
  (7, 2,    null, null),
  (8, 2,    null, null),
  (9, 2,    7,    'c'),
  (10, 3,   null, null),
  (11, 3,   null, null);

select if(g is null, 'NULL', cast(g as char)) as g_label,
       count(*) as count_all,
       count(v) as count_v,
       count(distinct v) as count_distinct_v,
       sum(v) as sum_v,
       cast(avg(v) as decimal(10,4)) as avg_v,
       min(v) as min_v,
       max(v) as max_v,
       group_concat(txt order by txt separator '|') as gc_txt
from t_null_agg
group by g
order by g is not null, g;

select if(g is null, 'NULL', cast(g as char)) as g_label,
       count(*) as count_all,
       sum(v) as sum_v
from t_null_agg
group by g
having sum(v) is null or count(v) = 0
order by g is not null, g;

select count(distinct g) as count_distinct_g,
       count(distinct v) as count_distinct_v,
       count(distinct txt) as count_distinct_txt
from t_null_agg;

select distinct if(g is null, 'NULL', cast(g as char)) as g_label,
       if(v is null, 'NULL', cast(v as char)) as v_label
from t_null_agg
order by g_label, v_label;

select id, if(g is null, 'NULL', cast(g as char)) as g_label,
       if(v is null, 'NULL', cast(v as char)) as v_label
from t_null_agg
order by g asc, id asc;

select id, if(g is null, 'NULL', cast(g as char)) as g_label,
       if(v is null, 'NULL', cast(v as char)) as v_label
from t_null_agg
order by g desc, id asc;

drop database mysql_compat_null_aggregation;
