-- Copyright 2026 Matrix Origin
-- Licensed under the Apache License, Version 2.0.
--
-- Bounded MongoDB history repair. Populate one non-overlapping site/day row
-- per recoverable source or archive slice, then call run_backfill_slice(id, fence).
-- The procedure commits the repaired target rows and checkpoint atomically.

create table if not exists telemetry_backfill_control (
    backfill_id varchar(128) primary key,
    source_name varchar(128) not null,
    site_id varchar(10) not null,
    slice_low datetime(3) not null,
    slice_high datetime(3) not null,
    state enum('PENDING', 'PAUSED', 'COMMITTED') not null default 'PENDING',
    fence bigint unsigned not null default 1,
    attempts bigint unsigned not null default 0,
    committed_at datetime(3),
    updated_at timestamp not null default utc_timestamp,
    unique key slice_identity (source_name, site_id, slice_low, slice_high)
);

-- Example queue row; generate equivalent rows for every site/day. An archive
-- external table can replace telemetry_events_external without changing the
-- transaction or target contract.
insert into telemetry_backfill_control(
    backfill_id, source_name, site_id, slice_low, slice_high
)
select 'REPLACE_BACKFILL_ID', 'telemetry_events', 'REPLACE_SITE_ID',
       cast('2026-01-01 00:00:00' as datetime),
       cast('2026-01-02 00:00:00' as datetime)
where not exists (
    select 1 from telemetry_backfill_control where backfill_id = 'REPLACE_BACKFILL_ID'
);

drop procedure if exists run_backfill_slice;
create procedure run_backfill_slice(
    in p_backfill_id varchar(128),
    in p_expected_fence bigint unsigned
)'
begin
select assert(
    (select count(*)
       from telemetry_backfill_control
      where backfill_id = p_backfill_id
        and fence = p_expected_fence
        and state = ''PENDING'') = 1,
    ''MongoDB backfill slice is absent, paused, committed, or has a stale fence''
);

-- This write obtains the per-slice row lock. A duplicate invocation either
-- waits and then observes the advanced fence/state, or fails before scanning.
update telemetry_backfill_control
   set attempts = attempts + 1,
       updated_at = utc_timestamp
 where backfill_id = p_backfill_id
   and fence = p_expected_fence
   and state = ''PENDING'';
select assert(
    row_count() = 1,
    ''MongoDB backfill slice lost its fence before source scan''
);

replace into telemetry_minute_aggregate
select a.device_id,
       a.site_id,
       a.window_start,
       a.temperature_celsius,
       a.humidity_percent,
       a.pressure_kpa,
       a.flow_rate_lpm,
       a.vibration_mm_s,
       a.voltage_volts,
       a.total_runtime_hours,
       a.active_runtime_hours,
       a.samples_in_window,
       a.source_batch,
       c.slice_high,
       sha2(concat(
           ''mongodb-aggregate-v1-exact|'',
           ''D:'', hex(a.device_id),
           ''|S:'', hex(a.site_id),
           ''|T:'', date_format(a.window_start, ''%Y-%m-%dT%H:%i:%sZ''),
           ''|N:'', cast(a.samples_in_window as char),
           ''|B:'', case when a.source_batch is null then ''N''
                         else concat(''V'', hex(a.source_batch)) end
       ), 256),
       ''mongodb-aggregate-v1''
  from (
        select r.device_id,
               r.site_id,
               _wstart as window_start,
               round(avg(r.temperature_celsius), 2) as temperature_celsius,
               round(avg(r.humidity_percent), 2) as humidity_percent,
               round(avg(r.pressure_kpa), 2) as pressure_kpa,
               round(avg(r.flow_rate_lpm), 2) as flow_rate_lpm,
               round(avg(r.vibration_mm_s), 2) as vibration_mm_s,
               round(avg(r.voltage_volts), 2) as voltage_volts,
               max_by_non_null(r.total_runtime_hours, r.ts, r.mongo_id) as total_runtime_hours,
               max_by_non_null(r.active_runtime_hours, r.ts, r.mongo_id) as active_runtime_hours,
               coalesce(count(*), 0) as samples_in_window,
               max_by(r.source_batch, r.ts, r.mongo_id) as source_batch
          from telemetry_events_external r
          join telemetry_backfill_control c
            on c.backfill_id = p_backfill_id
           and c.fence = p_expected_fence
           and c.state = ''PENDING''
           and r.site_id = c.site_id
           and r.ts >= c.slice_low
           and r.ts < c.slice_high
         group by r.device_id, r.site_id
         interval(r.ts, 1, minute) gapfill(partition) fill(null)
       ) a
  join telemetry_backfill_control c
    on c.backfill_id = p_backfill_id
   and c.fence = p_expected_fence;

update telemetry_backfill_control
   set state = ''COMMITTED'',
       committed_at = utc_timestamp(3),
       fence = fence + 1,
       updated_at = utc_timestamp
 where backfill_id = p_backfill_id
   and fence = p_expected_fence
   and state = ''PENDING'';
end';

-- Operational controls:
--   pause:  UPDATE ... SET state='PAUSED', fence=fence+1 WHERE backfill_id=?;
--   resume: UPDATE ... SET state='PENDING', fence=fence+1 WHERE backfill_id=?;
--   replay: set PENDING with a new fence, then call with that exact fence.
-- A failed/canceled call rolls back attempts, data, and state, so retry starts
-- from the last COMMITTED slice. Record unrecoverable intervals as PAUSED rows
-- and require a named business owner before cutover.
