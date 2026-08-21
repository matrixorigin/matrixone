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

-- A fall-back fold can make a civil-time RANGE membership non-contiguous in
-- the timestamp-instant order. These four controls cover both sort directions
-- and both finite interval bound directions.
create table fall_back_fold(id int primary key, ts timestamp(6), v int);
insert into fall_back_fold values
  (1, '2024-11-03 05:00:00.000000', 1),
  (2, '2024-11-03 05:30:00.000000', 2),
  (3, '2024-11-03 05:59:00.000000', 3),
  (4, '2024-11-03 06:00:00.000000', 4),
  (5, '2024-11-03 06:30:00.000000', 5),
  (6, '2024-11-03 06:59:00.000000', 6),
  (7, '2024-11-03 07:00:00.000000', 7),
  (8, '2024-11-03 07:30:00.000000', 8);

set time_zone = 'America/New_York';
select id, sum(v) over (
  order by ts
  range between interval 30 minute preceding and current row
) as s from fall_back_fold order by id;

select id, sum(v) over (
  order by ts
  range between current row and interval 30 minute following
) as s from fall_back_fold order by id;

select id, sum(v) over (
  order by ts desc
  range between interval 30 minute preceding and current row
) as s from fall_back_fold order by id;

select id, sum(v) over (
  order by ts desc
  range between current row and interval 30 minute following
) as s from fall_back_fold order by id;

drop table fall_back_fold;

-- Fold detection must come from the timezone transition, not merely an
-- observed reversal in sampled civil values. These sparse/equal controls
-- cross the same transition without a strictly decreasing display value.
create table fall_back_sparse(id int primary key, ts timestamp(6), v int);
insert into fall_back_sparse values
  (1, '2024-11-03 05:00:00.000000', 1),
  (2, '2024-11-03 06:30:00.000000', 10);

select id, sum(v) over (
  order by ts
  range between current row and interval 30 minute following
) as s from fall_back_sparse order by id;

drop table fall_back_sparse;

create table fall_back_equal(id int primary key, ts timestamp(6), v int);
insert into fall_back_equal values
  (1, '2024-11-03 05:30:00.000000', 1),
  (2, '2024-11-03 06:30:00.000000', 10);

select id, sum(v) over (
  order by ts
  range between current row and current row
) as s from fall_back_equal order by id;

drop table fall_back_equal;

set time_zone = @old_time_zone_window_timestamp_range_dst;
drop database window_timestamp_range_dst;
