-- Copyright 2026 Matrix Origin
-- Licensed under the Apache License, Version 2.0.
--
-- Executable NESR numeric-v1 ingestion template. Rename the source/target once
-- per deployment; keep the transaction body intact. The task performs no raw
-- row hop through MOI: MongoDB -> MongoScan -> MO TimeWin/Group -> MO storage.

create table if not exists nesr_ingest_control (
    source_name varchar(128) primary key,
    committed_high datetime(3) not null,
    requested_high datetime(3),
    overlap_ms bigint not null default 300000,
    safety_lag_ms bigint not null default 120000,
    pending_low datetime(3),
    pending_high datetime(3),
    fence bigint unsigned not null default 0,
    archive_cutoff datetime(3),
    archive_replay_ready bool not null default false,
    updated_at timestamp not null default utc_timestamp
);

create table if not exists nesr_minute_v2 (
    pump varchar(20) not null,
    crew varchar(10) not null,
    datetime datetime not null,
    engine_rpm float,
    pump_rate float,
    disch_pressure float,
    engine_oil_pressure float,
    engine_coolant_temp float,
    lube_oil_pressure float,
    engine_hours float,
    pumping_hours float,
    readings_in_minute bigint not null,
    source_batch varchar(128),
    source_high datetime(3) not null,
    exact_row_hash char(64) not null,
    hash_version varchar(32) not null,
    primary key (pump, crew, datetime)
);

-- Bootstrap once with the agreed source lower bound.
insert into nesr_ingest_control(source_name, committed_high)
select 'nesr_raw', cast('2026-01-01 00:00:00' as datetime)
where not exists (select 1 from nesr_ingest_control where source_name = 'nesr_raw');

-- A top-level MO stored procedure executes in one transaction. A CALL issued
-- by SQL TASK reuses the task statement transaction, so a scan/getMore,
-- conversion, aggregate, write, timeout, or commit failure rolls target rows
-- and pending bounds back together. The UPDATE locks the singleton control row
-- and serializes task/manual invocations for this source.
drop procedure if exists nesr_ingest_once;
create procedure nesr_ingest_once() '
begin
select assert(
    (select count(*) from nesr_ingest_control where source_name = ''nesr_raw'') = 1,
    ''NESR MongoDB ingestion control row is missing or duplicated''
);
update nesr_ingest_control
   set pending_low = cast(date_format(
           timestampadd(microsecond, -1000 * overlap_ms, committed_high),
           ''%Y-%m-%d %H:%i:00.000''
       ) as datetime(3)),
       pending_high = greatest(committed_high, cast(date_format(
           least(
               coalesce(requested_high, timestampadd(microsecond, -1000 * safety_lag_ms, utc_timestamp(3))),
               timestampadd(microsecond, -1000 * safety_lag_ms, utc_timestamp(3))
           ),
           ''%Y-%m-%d %H:%i:00.000''
       ) as datetime(3))),
       updated_at = utc_timestamp
 where source_name = ''nesr_raw'';

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
       c.pending_high,
       -- v1 exact hash intentionally excludes every FLOAT column. Hex makes
       -- pump/crew/batch delimiter-safe, and the N/V marker distinguishes a
       -- NULL batch from an empty string. Floating values are compared under
       -- the separately versioned numeric contract after target FLOAT cast.
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
        select pump,
               crew,
               _wstart as datetime,
               round(avg(engine_rpm), 2) as engine_rpm,
               round(avg(pump_rate), 2) as pump_rate,
               round(avg(disch_pressure), 2) as disch_pressure,
               round(avg(engine_oil_pressure), 2) as engine_oil_pressure,
               round(avg(engine_coolant_temp), 2) as engine_coolant_temp,
               round(avg(lube_oil_pressure), 2) as lube_oil_pressure,
               max_by_non_null(engine_hours, ts, mongo_id) as engine_hours,
               max_by_non_null(pumping_hours, ts, mongo_id) as pumping_hours,
               coalesce(count(*), 0) as readings_in_minute,
               max_by(source_batch, ts, mongo_id) as source_batch
          from nesr_raw_external
         where ts >= (select pending_low from nesr_ingest_control where source_name = ''nesr_raw'')
           and ts <  (select pending_high from nesr_ingest_control where source_name = ''nesr_raw'')
         group by pump, crew
         interval(ts, 1, minute) gapfill(partition) fill(null)
       ) a
  join nesr_ingest_control c on c.source_name = ''nesr_raw'';

update nesr_ingest_control
   set committed_high = pending_high,
       pending_low = null,
       pending_high = null,
       requested_high = null,
       fence = fence + 1,
       updated_at = utc_timestamp
 where source_name = ''nesr_raw''
   and pending_high is not null;
end';

-- The task body is deliberately one CALL statement. SQL TASK compound bodies
-- execute separate statements independently, whereas the procedure provides
-- the required all-or-nothing transaction. MOI may instead call this procedure
-- for scheduling/alerting, but it must not materialize or relay raw rows.
drop task if exists nesr_mongodb_incremental;
create task nesr_mongodb_incremental
schedule '0 */5 * * * *'
timezone 'UTC'
retry 1
timeout '4m'
as begin call nesr_ingest_once(); end;
