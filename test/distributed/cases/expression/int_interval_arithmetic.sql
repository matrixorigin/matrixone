-- @suite
-- @case
-- @desc: regression test for #24412 — INT (YYYYMMDD) +/- INTERVAL arithmetic

drop database if exists test_int_interval;
create database test_int_interval;
use test_int_interval;

create table readings (site_id int, date_id int, value double);
insert into readings values (1, 20260514, 10.0), (1, 20260515, 11.0), (2, 20260514, 20.0), (2, 20260515, 21.0), (3, 20260514, 30.0), (3, 20260515, 31.0);

-- INT +/- INTERVAL DAY with column
select max(date_id) + interval 7 day from readings;
select max(date_id) - interval 7 day from readings;

-- CAST INT +/- INTERVAL DAY
select cast(20260515 as int) + interval 7 day;
select cast(20260515 as int) - interval 7 day;

-- INTERVAL DAY + INT (symmetric)
select interval 7 day + cast(20260515 as int);

-- WEEK / MONTH / YEAR
select cast(20260515 as int) + interval 2 week;
select cast(20260515 as int) + interval 1 month;
select cast(20260515 as int) - interval 1 year;

-- Sub-day intervals should fall through with original error
select cast(20260515 as int) + interval 1 hour;
select cast(20260515 as int) - interval 30 minute;
select cast(20260515 as int) + interval 1 second;

-- NULL handling
insert into readings values (4, null, 0.0);
select date_id + interval 7 day from readings where site_id = 4;
select date_id - interval 7 day from readings where site_id = 4;

-- Issue #24412 Case B full repro: CASE BETWEEN with subquery + INTERVAL
select site_id, case when date_id between (select max(date_id) from readings) - interval 7 day and (select max(date_id) from readings) then 'recent' else 'older' end as bucket from readings where site_id <= 3 order by site_id, date_id;

drop database test_int_interval;
