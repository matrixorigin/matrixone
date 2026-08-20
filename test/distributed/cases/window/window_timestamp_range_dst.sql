-- Regression for issue #27351. TIMESTAMP RANGE interval boundaries must use
-- the session time zone, including DST gaps and folds.
drop database if exists window_timestamp_range_dst;
create database window_timestamp_range_dst;
use window_timestamp_range_dst;

set @old_time_zone_window_timestamp_range_dst = @@time_zone;
set time_zone = '+00:00';

create table spring_forward(id int primary key, ts timestamp(6), v int);
insert into spring_forward values
  (1, '2024-03-10 06:59:59.999999', 10),
  (2, '2024-03-10 07:00:00.000000', 20),
  (3, '2024-03-10 07:30:00.000000', 30);

set time_zone = 'America/New_York';
select id,
       sum(v) over (
         order by ts
         range between interval 1 hour preceding and current row
       ) as s
from spring_forward
order by id;

-- Fixed-offset and non-DST named-zone controls retain instant arithmetic.
set time_zone = '+00:00';
select id,
       sum(v) over (
         order by ts
         range between interval 1 hour preceding and current row
       ) as s
from spring_forward
order by id;

set time_zone = 'Asia/Shanghai';
select id,
       sum(v) over (
         order by ts
         range between interval 1 hour preceding and current row
       ) as s
from spring_forward
order by id;

set time_zone = '+00:00';
create table fall_back(id int primary key, ts timestamp(6), v int);
insert into fall_back values
  (1, '2024-11-03 05:30:00.000000', 10),
  (2, '2024-11-03 06:30:00.000000', 20),
  (3, '2024-11-03 07:00:00.000000', 30);

-- The first two instants both display as 01:30 in New York. The 02:00 row's
-- one-hour civil frame includes both occurrences.
set time_zone = 'America/New_York';
select id,
       sum(v) over (
         order by ts
         range between interval 1 hour preceding and current row
       ) as s
from fall_back
order by id;

set time_zone = '+00:00';
select id,
       sum(v) over (
         order by ts
         range between interval 1 hour preceding and current row
       ) as s
from fall_back
order by id;

set time_zone = @old_time_zone_window_timestamp_range_dst;
drop database window_timestamp_range_dst;
