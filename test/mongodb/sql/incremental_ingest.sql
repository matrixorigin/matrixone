-- Copyright 2026 Matrix Origin
-- Licensed under the Apache License, Version 2.0.
--
-- Executable MongoDB aggregate-v1 ingestion template. Rename the source and
-- target once per deployment; keep the transaction body intact. Rows flow
-- directly from MongoDB through MongoScan and MO aggregation into MO storage.

create table if not exists telemetry_ingest_control (
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

create table if not exists telemetry_minute_aggregate (
    device_id varchar(20) not null,
    site_id varchar(10) not null,
    window_start datetime not null,
    temperature_celsius float,
    humidity_percent float,
    pressure_kpa float,
    flow_rate_lpm float,
    vibration_mm_s float,
    voltage_volts float,
    total_runtime_hours float,
    active_runtime_hours float,
    samples_in_window bigint not null,
    source_batch varchar(128),
    source_high datetime(3) not null,
    exact_row_hash char(64) not null,
    hash_version varchar(32) not null,
    primary key (device_id, site_id, window_start)
);

-- Bootstrap once with the agreed source lower bound.
insert into telemetry_ingest_control(source_name, committed_high)
select 'telemetry_events', cast('2026-01-01 00:00:00' as datetime)
where not exists (select 1 from telemetry_ingest_control where source_name = 'telemetry_events');

-- A top-level MO stored procedure executes in one transaction. A CALL issued
-- by SQL TASK reuses the task statement transaction, so a scan/getMore,
-- conversion, aggregate, write, timeout, or commit failure rolls target rows
-- and pending bounds back together. The UPDATE locks the singleton control row
-- and serializes task/manual invocations for this source.
drop procedure if exists run_incremental_ingest;
create procedure run_incremental_ingest() '
begin
select assert(
    (select count(*) from telemetry_ingest_control where source_name = ''telemetry_events'') = 1,
    ''MongoDB ingestion control row is missing or duplicated''
);
update telemetry_ingest_control
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
 where source_name = ''telemetry_events'';

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
       c.pending_high,
       -- v1 exact hash intentionally excludes every FLOAT column. Hex makes
       -- device_id/site_id/source_batch delimiter-safe, and the N/V marker
       -- distinguishes a NULL source batch from an empty string. Floating
       -- values use the separately versioned numeric contract after target
       -- FLOAT cast.
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
        select device_id,
               site_id,
               _wstart as window_start,
               round(avg(temperature_celsius), 2) as temperature_celsius,
               round(avg(humidity_percent), 2) as humidity_percent,
               round(avg(pressure_kpa), 2) as pressure_kpa,
               round(avg(flow_rate_lpm), 2) as flow_rate_lpm,
               round(avg(vibration_mm_s), 2) as vibration_mm_s,
               round(avg(voltage_volts), 2) as voltage_volts,
               max_by_non_null(total_runtime_hours, ts, mongo_id) as total_runtime_hours,
               max_by_non_null(active_runtime_hours, ts, mongo_id) as active_runtime_hours,
               coalesce(count(*), 0) as samples_in_window,
               max_by(source_batch, ts, mongo_id) as source_batch
          from telemetry_events_external
         where ts >= (select pending_low from telemetry_ingest_control where source_name = ''telemetry_events'')
           and ts <  (select pending_high from telemetry_ingest_control where source_name = ''telemetry_events'')
         group by device_id, site_id
         interval(ts, 1, minute) gapfill(partition) fill(null)
       ) a
  join telemetry_ingest_control c on c.source_name = ''telemetry_events'';

update telemetry_ingest_control
   set committed_high = pending_high,
       pending_low = null,
       pending_high = null,
       requested_high = null,
       fence = fence + 1,
       updated_at = utc_timestamp
 where source_name = ''telemetry_events''
   and pending_high is not null;
end';

-- The task body is deliberately one CALL statement. SQL TASK compound bodies
-- execute separate statements independently, whereas the procedure provides
-- the required all-or-nothing transaction. An external orchestrator may call
-- this procedure for scheduling and alerting, but it must not relay source rows.
drop task if exists mongodb_incremental_ingest;
create task mongodb_incremental_ingest
schedule '0 */5 * * * *'
timezone 'UTC'
retry 1
timeout '4m'
as begin call run_incremental_ingest(); end;
