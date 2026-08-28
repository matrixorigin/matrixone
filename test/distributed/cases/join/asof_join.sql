-- Issue #26986: native ASOF JOIN.
-- Common ASOF semantics follow DuckDB: equality keys partition the right side,
-- the inequality chooses the nearest predecessor, and each left row produces
-- at most one match.

drop database if exists asof_join_db;
create database asof_join_db;
use asof_join_db;
set @saved_time_zone = @@session.time_zone;
set time_zone = '+00:00';

-- ASOF remains an ordinary table name and implicit alias for SQL that was
-- valid before native ASOF JOIN. The inequality join must retain both rows.
create table asof (k int, v int);
create table legacy_right (k int, v int);
insert into asof values (1, 3);
insert into legacy_right values (1, 1), (1, 2);
select asof.k, u.v as right_v
from asof join legacy_right u on asof.k = u.k
order by right_v;
select asof.k, asof.v as left_v, u.v as right_v
from asof asof join legacy_right u
  on asof.k = u.k and asof.v > u.v
order by right_v;

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

-- Force a multi-pipeline ASOF plan. The build is physically descending and
-- each of the four probe pipelines must emit exactly one predecessor per row.
set @@max_dop = 4;
create table parallel_left (id int, k int, ts datetime);
create table parallel_right (id int, k int, ts datetime);
insert into parallel_left
select result, result % 32,
       date_add('2026-01-01', interval result second)
from generate_series(1, 20000) g;
insert into parallel_right
select result, result % 32,
       date_add('2026-01-01', interval result second)
from generate_series(20000, 1, -1) g;
select count(*) as matched_rows,
       min(l.id - r.id) as min_delta,
       max(l.id - r.id) as max_delta
from parallel_left l asof join parallel_right r
  on l.k = r.k and l.ts >= r.ts;
set @@max_dop = 0;

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

-- Issue #26986 regression: ASOF must also work through real table metadata
-- (primary/unique keys, NOT NULL, DEFAULT and CHECK), including a prepared
-- execution.  These are deliberately separate from the duplicate-timestamp
-- case above: duplicate ties have no cross-CN ordering contract yet.
create table constrained_readings (
    id int primary key,
    device varchar(12) not null,
    region varchar(12) not null,
    event_ts timestamp(6) not null,
    reading decimal(10,2) not null,
    constraint constrained_reading_nonnegative check (reading >= 0)
);
create table constrained_device_cfg (
    device varchar(12) not null,
    region varchar(12) not null,
    effective_ts timestamp(6) not null,
    setting varchar(32) not null default 'default-setting',
    primary key (device, region, effective_ts)
);
create index constrained_cfg_setting_idx on constrained_device_cfg (setting);
insert into constrained_device_cfg (device, region, effective_ts, setting) values
    ('a', 'west', '2026-02-01 09:59:00.000000', 'v1'),
    ('a', 'west', '2026-02-01 10:01:00.000000', 'v2');
insert into constrained_device_cfg (device, region, effective_ts) values
    ('b', 'east', '2026-02-01 10:00:00.000000');
insert into constrained_readings values
    (1, 'a', 'west', '2026-02-01 09:59:30.000000', 1.00),
    (2, 'a', 'west', '2026-02-01 10:01:00.000000', 2.00),
    (3, 'b', 'east', '2026-02-01 10:00:00.000000', 3.00);
-- @regex("(?i)check",true)
insert into constrained_readings values
    (4, 'a', 'west', '2026-02-01 10:02:00.000000', -1.00);
-- @regex("(?i)cannot be null",true)
insert into constrained_device_cfg values
    (null, 'east', '2026-02-01 10:02:00.000000', 'invalid-null-key');
select r.id, c.setting
from constrained_readings r asof left join constrained_device_cfg c
  on r.device = c.device
 and r.region = c.region
 and r.event_ts >= c.effective_ts
order by r.id;
select r.id, c.setting
from constrained_readings r asof left join constrained_device_cfg c
  on r.device = c.device
 and r.region = c.region
 and r.event_ts >= c.effective_ts
tolerance interval 30 second
order by r.id;
prepare constrained_asof from
    'select r.id, c.setting from constrained_readings r asof join constrained_device_cfg c on r.device = c.device and r.region = c.region and r.event_ts >= c.effective_ts where r.id = ?';
set @constrained_id = 2;
execute constrained_asof using @constrained_id;
deallocate prepare constrained_asof;
-- @regex("Duplicate entry",true)
insert into constrained_device_cfg values
    ('a', 'west', '2026-02-01 10:01:00.000000', 'duplicate');
-- @regex("ASOF JOIN requires at least one equality key",true)
select r.id
from constrained_readings r asof join constrained_device_cfg c
  on r.event_ts >= c.effective_ts;
-- @regex("ASOF JOIN temporal predicate must look backward",true)
select r.id
from constrained_readings r asof join constrained_device_cfg c
  on r.device = c.device
 and r.region = c.region
 and r.event_ts <= c.effective_ts;

drop database asof_join_db;
set session time_zone = @saved_time_zone;
