-- Copyright 2026 Matrix Origin
-- Licensed under the Apache License, Version 2.0.
--
-- Bounded NESR history repair. Populate one non-overlapping crew/day row per
-- recoverable source/archive slice, then call nesr_backfill_slice(id, fence).
-- The procedure commits the repaired target rows and checkpoint atomically.

create table if not exists nesr_backfill_control (
    backfill_id varchar(128) primary key,
    source_name varchar(128) not null,
    crew varchar(10) not null,
    slice_low datetime(3) not null,
    slice_high datetime(3) not null,
    state enum('PENDING', 'PAUSED', 'COMMITTED') not null default 'PENDING',
    fence bigint unsigned not null default 1,
    attempts bigint unsigned not null default 0,
    committed_at datetime(3),
    updated_at timestamp not null default utc_timestamp,
    unique key slice_identity (source_name, crew, slice_low, slice_high)
);

-- Example queue row; generate equivalent rows for every crew/day. An archive
-- external table can be substituted for nesr_raw_external without changing the
-- transaction or target contract.
insert into nesr_backfill_control(
    backfill_id, source_name, crew, slice_low, slice_high
)
select 'REPLACE_BACKFILL_ID', 'nesr_raw', 'REPLACE_CREW',
       cast('2026-01-01 00:00:00' as datetime),
       cast('2026-01-02 00:00:00' as datetime)
where not exists (
    select 1 from nesr_backfill_control where backfill_id = 'REPLACE_BACKFILL_ID'
);

drop procedure if exists nesr_backfill_slice;
create procedure nesr_backfill_slice(
    in p_backfill_id varchar(128),
    in p_expected_fence bigint unsigned
)'
begin
select assert(
    (select count(*)
       from nesr_backfill_control
      where backfill_id = p_backfill_id
        and fence = p_expected_fence
        and state = ''PENDING'') = 1,
    ''NESR backfill slice is absent, paused, committed, or has a stale fence''
);

-- This write obtains the per-slice row lock. A duplicate invocation either
-- waits and then observes the advanced fence/state, or fails before scanning.
update nesr_backfill_control
   set attempts = attempts + 1,
       updated_at = utc_timestamp
 where backfill_id = p_backfill_id
   and fence = p_expected_fence
   and state = ''PENDING'';
select assert(
    row_count() = 1,
    ''NESR backfill slice lost its fence before source scan''
);

replace into nesr_minute_v2
select a.pump,
       a.crew,
       a.datetime,
       a.engine_rpm,
       a.pump_rate,
       a.disch_pressure,
       a.engine_oil_pressure,
       a.engine_coolant_temp,
       a.lube_oil_pressure,
       a.engine_hours,
       a.pumping_hours,
       a.readings_in_minute,
       a.source_batch,
       c.slice_high,
       sha2(concat(
           ''nesr-mongo-v1-exact|'',
           ''P:'', hex(a.pump),
           ''|C:'', hex(a.crew),
           ''|T:'', date_format(a.datetime, ''%Y-%m-%dT%H:%i:%sZ''),
           ''|N:'', cast(a.readings_in_minute as char),
           ''|B:'', case when a.source_batch is null then ''N''
                         else concat(''V'', hex(a.source_batch)) end
       ), 256),
       ''nesr-mongo-v1''
  from (
        select r.pump,
               r.crew,
               _wstart as datetime,
               round(avg(r.engine_rpm), 2) as engine_rpm,
               round(avg(r.pump_rate), 2) as pump_rate,
               round(avg(r.disch_pressure), 2) as disch_pressure,
               round(avg(r.engine_oil_pressure), 2) as engine_oil_pressure,
               round(avg(r.engine_coolant_temp), 2) as engine_coolant_temp,
               round(avg(r.lube_oil_pressure), 2) as lube_oil_pressure,
               max_by_non_null(r.engine_hours, r.ts, r.mongo_id) as engine_hours,
               max_by_non_null(r.pumping_hours, r.ts, r.mongo_id) as pumping_hours,
               coalesce(count(*), 0) as readings_in_minute,
               max_by(r.source_batch, r.ts, r.mongo_id) as source_batch
          from nesr_raw_external r
          join nesr_backfill_control c
            on c.backfill_id = p_backfill_id
           and c.fence = p_expected_fence
           and c.state = ''PENDING''
           and r.crew = c.crew
           and r.ts >= c.slice_low
           and r.ts < c.slice_high
         group by r.pump, r.crew
         interval(r.ts, 1, minute) gapfill(partition) fill(null)
       ) a
  join nesr_backfill_control c
    on c.backfill_id = p_backfill_id
   and c.fence = p_expected_fence;

update nesr_backfill_control
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
