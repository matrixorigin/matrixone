-- Issue #26986: native ASOF JOIN.
-- Common ASOF semantics follow DuckDB: equality keys partition the right side,
-- the inequality chooses the nearest predecessor, and each left row produces
-- at most one match.

drop database if exists asof_join_db;
create database asof_join_db;
use asof_join_db;
set @saved_time_zone = @@session.time_zone;
set time_zone = '+00:00';

create table readings (
    id int,
    device varchar(8),
    region varchar(8),
    event_ts timestamp(6),
    reading int
);
create table device_config (
    device varchar(8),
    region varchar(8),
    effective_ts timestamp(6),
    setting varchar(16)
);

insert into device_config values
    ('a', 'west', '2026-01-01 09:58:00.000000', 'old'),
    ('a', 'west', '2026-01-01 10:00:00.000000', 'exact'),
    ('a', 'west', '2026-01-01 10:02:00.000000', 'new'),
    ('a', 'east', '2026-01-01 09:59:00.000000', 'east'),
    ('b', 'west', '2026-01-01 10:01:00.000000', 'b-new'),
    (null, 'west', '2026-01-01 09:00:00.000000', 'null-key');

insert into readings values
    (1,  'a', 'west', '2026-01-01 09:57:00.000000', 101),
    (2,  'a', 'west', '2026-01-01 10:00:00.000000', 102),
    (3,  'a', 'west', '2026-01-01 10:01:00.000000', 103),
    (4,  'a', 'west', '2026-01-01 10:02:00.000000', 104),
    (5,  'a', 'west', '2026-01-01 10:04:00.000000', 105),
    (6,  'a', 'west', '2026-01-01 10:04:00.000001', 106),
    (7,  'a', 'east', '2026-01-01 10:00:00.000000', 107),
    (8,  'b', 'west', '2026-01-01 10:00:00.000000', 108),
    (9,  'c', 'west', '2026-01-01 10:00:00.000000', 109),
    (10, null, 'west', '2026-01-01 10:00:00.000000', 110),
    (11, 'a', 'west', null, 111);

-- Inclusive predecessor and composite equality keys. ASOF LEFT preserves every
-- reading, including NULL keys/timestamps and times before the first config.
select r.id, r.device, r.region, r.event_ts, c.effective_ts, c.setting
from readings r asof left join device_config c
  on r.device = c.device
 and r.region = c.region
 and r.event_ts >= c.effective_ts
order by r.id;

-- ASOF JOIN is inner: left rows without a predecessor are omitted.
select r.id, c.effective_ts, c.setting
from readings r asof join device_config c
  on r.device = c.device
 and r.region = c.region
 and r.event_ts >= c.effective_ts
order by r.id;

-- Strict predecessor: an equal timestamp is not eligible.
select r.id, c.effective_ts, c.setting
from readings r asof left join device_config c
  on r.device = c.device
 and r.region = c.region
 and r.event_ts > c.effective_ts
where r.id in (2, 4)
order by r.id;

-- The commuted spelling is equivalent to the inclusive predecessor form.
select r.id, c.effective_ts, c.setting
from readings r asof left join device_config c
  on c.device = r.device
 and c.region = r.region
 and c.effective_ts <= r.event_ts
where r.id between 2 and 5
order by r.id;

-- MatrixOne extension: tolerance is inclusive at exactly two minutes and
-- rejects a predecessor older by one additional microsecond.
select r.id, c.effective_ts, c.setting
from readings r asof left join device_config c
  on r.device = c.device
 and r.region = c.region
 and r.event_ts >= c.effective_ts
tolerance interval 2 minute
order by r.id;

-- Empty build-side corners for inner and left ASOF joins.
create table empty_config like device_config;
select r.id, c.setting
from readings r asof join empty_config c
  on r.device = c.device and r.event_ts >= c.effective_ts
order by r.id;
select r.id, c.setting
from readings r asof left join empty_config c
  on r.device = c.device and r.event_ts >= c.effective_ts
where r.id in (1, 2)
order by r.id;

-- DATE, DATETIME, and TIME ordering columns use the same predecessor rule.
create table temporal_left (id int, k int, d date, dt datetime(6), tm time(6));
create table temporal_right (k int, d date, dt datetime(6), tm time(6), label varchar(8));
insert into temporal_left values
    (1, 1, '2026-01-03', '2026-01-03 10:00:00.500000', '10:00:00.500000');
insert into temporal_right values
    (1, '2026-01-01', '2026-01-03 09:59:00.000000', '09:59:00.000000', 'older'),
    (1, '2026-01-03', '2026-01-03 10:00:00.500000', '10:00:00.500000', 'exact');
select l.id, r.d, r.label from temporal_left l asof join temporal_right r
  on l.k = r.k and l.d >= r.d;
select l.id, r.dt, r.label from temporal_left l asof join temporal_right r
  on l.k = r.k and l.dt >= r.dt;
select l.id, r.tm, r.label from temporal_left l asof join temporal_right r
  on l.k = r.k and l.tm > r.tm;

-- Equal-timestamp ties are arbitrary across distributed producer order; this
-- single-source case follows the materialized build order.
create table duplicate_config (k int, ts timestamp(6), value varchar(8));
insert into duplicate_config values
    (1, '2026-01-01 10:00:00.000000', 'first'),
    (1, '2026-01-01 10:00:00.000000', 'second');
select l.id, r.value
from (select 1 id, 1 k, cast('2026-01-01 10:01:00' as timestamp) ts) l
asof join duplicate_config r on l.k = r.k and l.ts >= r.ts;

drop database asof_join_db;
set session time_zone = @saved_time_zone;
