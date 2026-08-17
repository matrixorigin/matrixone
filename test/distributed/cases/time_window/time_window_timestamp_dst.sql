drop database if exists time_window_timestamp_dst;
create database time_window_timestamp_dst;
use time_window_timestamp_dst;

set @old_time_zone_time_window_timestamp_dst = @@time_zone;
set time_zone = 'America/New_York';

create table tw_dst_spring(event_ts timestamp(6), value int);
insert into tw_dst_spring values
  ('2026-03-08 01:30:00', 1),
  ('2026-03-08 03:30:00', 2);

select unix_timestamp(_wstart) as ws, unix_timestamp(_wend) as we, count(value) as n
from tw_dst_spring
interval(event_ts, 1, hour)
order by ws, we;

create table tw_dst_gap(k int, event_ts timestamp(6), value int);
insert into tw_dst_gap values
  (1, '2026-03-08 01:30:00', 1),
  (1, '2026-03-08 03:30:00', 2);

select _wstart, _wend, sum(value) as value_sum
from tw_dst_gap
group by k interval(event_ts, 2, hour)
order by _wstart;

select _wstart, _wend, sum(value) as value_sum
from tw_dst_gap
group by k interval(event_ts, 2, hour) gapfill(partition)
order by _wstart;

select _wstart, _wend, sum(value) as value_sum
from tw_dst_gap
group by k interval(event_ts, 2, hour) sliding(1, hour)
order by _wstart;

select _wstart, _wend, sum(value) as value_sum
from tw_dst_gap
group by k interval(event_ts, 2, hour) sliding(1, hour) gapfill(partition)
order by _wstart;

create table tw_dst_day(k int, event_ts timestamp(6), value int);
insert into tw_dst_day values
  (1, '2026-03-08 00:00:00', 1),
  (1, '2026-03-09 00:00:00', 2);

select _wstart, _wend, sum(value) as value_sum
from tw_dst_day
group by k interval(event_ts, 1, day)
order by _wstart;

select _wstart, _wend, sum(value) as value_sum
from tw_dst_day
group by k interval(event_ts, 1, day) gapfill(partition)
order by _wstart;

set time_zone = '+00:00';
create table tw_dst_fall(event_ts timestamp(6), value int);
insert into tw_dst_fall values
  ('2026-11-01 05:30:00', 1),
  ('2026-11-01 06:30:00', 2);

set time_zone = 'America/New_York';

select unix_timestamp(_wstart) as ws, unix_timestamp(_wend) as we, count(value) as n
from tw_dst_fall
interval(event_ts, 1, hour)
order by ws, we;

set time_zone = '+00:00';
create table tw_dst_fall_gap(k int, event_ts timestamp(6), value int);
insert into tw_dst_fall_gap values
  (1, '2026-11-01 05:30:00', 1),
  (1, '2026-11-01 06:30:00', 2);
set time_zone = 'America/New_York';

select _wstart, _wend, sum(value) as value_sum
from tw_dst_fall_gap
group by k interval(event_ts, 1, hour) gapfill(partition)
order by _wstart, _wend;

create table tw_dst_day_fall(k int, event_ts timestamp(6), value int);
insert into tw_dst_day_fall values
  (1, '2026-11-01 04:00:00', 1),
  (1, '2026-11-02 05:00:00', 2);

select _wstart, _wend, sum(value) as value_sum
from tw_dst_day_fall
group by k interval(event_ts, 1, day)
order by _wstart;

select _wstart, _wend, sum(value) as value_sum
from tw_dst_day_fall
group by k interval(event_ts, 1, day) gapfill(partition)
order by _wstart;

set time_zone = @old_time_zone_time_window_timestamp_dst;
drop database time_window_timestamp_dst;
