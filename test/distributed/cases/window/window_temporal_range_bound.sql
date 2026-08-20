-- issue #27350: an out-of-domain temporal RANGE bound is an unreachable
-- search key, not a row-value conversion error.
drop database if exists window_temporal_range_bound;
create database window_temporal_range_bound;
use window_temporal_range_bound;

create table t(id int primary key, d date, dt datetime(6), v int);
insert into t values
  (1, null, null, 1),
  (2, null, null, 2),
  (3, '1000-01-01', '1000-01-01 00:00:00.000000', 10),
  (4, '1000-01-02', '1000-01-02 00:00:00.000000', 20),
  (5, '2024-01-01', '2024-01-01 12:00:00.000000', 30),
  (6, '9999-12-30', '9999-12-30 23:59:59.999999', 40),
  (7, '9999-12-31', '9999-12-31 23:59:59.999999', 50);

-- ASC maximum FOLLOWING: rows 6 and 7 exercise an exact-domain and an
-- out-of-domain upper search key; NULL keys remain a peer group.
select id, d, sum(v) over (
         order by d
         range between current row and interval 1 year following
       ) as s
from t
order by d, id;

select id, dt, sum(v) over (
         order by dt
         range between current row and interval 1 day following
       ) as s
from t
order by dt, id;

-- DESC FOLLOWING reverses the arithmetic direction.
select id, d, sum(v) over (
         order by d desc
         range between current row and interval 1 year following
       ) as s
from t
order by d desc, id;

select id, dt, sum(v) over (
         order by dt desc
         range between current row and interval 1 day following
       ) as s
from t
order by dt desc, id;

-- Minimum-value PRECEDING and interior controls.
select id, d, sum(v) over (
         order by d
         range between interval 1 year preceding and current row
       ) as s
from t
order by d, id;

select id, dt, sum(v) over (
         order by dt
         range between interval 1 day preceding and current row
       ) as s
from t
order by dt, id;

-- TIME and TIMESTAMP use the same out-of-domain insertion-point rule.
create table temporal_time_timestamp_bound(id int primary key, tm time(6), ts timestamp(6), v int);
insert into temporal_time_timestamp_bound values
  (1, '2562047787:59:59.999998', '9999-12-30 23:59:59.999999', 40),
  (2, '2562047787:59:59.999999', '9999-12-31 23:59:59.999999', 50);

select id, tm, sum(v) over (
         order by tm
         range between current row and interval 1 microsecond following
       ) as s
from temporal_time_timestamp_bound
order by tm, id;

select id, ts, sum(v) over (
         order by ts
         range between current row and interval 1 day following
       ) as s
from temporal_time_timestamp_bound
order by ts, id;

drop table t;
drop table temporal_time_timestamp_bound;
drop database window_temporal_range_bound;
