-- Composite CLUSTER BY time-series reads must return every committed row
-- from CN logtail/replay state for the reported predicate, duplicates,
-- NULL key parts, updates, deletes, rollback and commit semantics.
drop database if exists ts_harness;
create database ts_harness;
use ts_harness;
create table metrics (tenant_id int, device_id int, metric_id int, event_ts datetime, value double, quality smallint) cluster by(tenant_id,device_id,metric_id,event_ts);
create table d10(device_id int);
insert into d10 values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
create table d100(device_id int);
insert into d100 select a.device_id * 10 + b.device_id from d10 a, d10 b;
create table device_seed(device_id int);
insert into device_seed select a.device_id * 10 + b.device_id from d100 a, d10 b;
create table met(metric_id int);
insert into met values (0),(1),(2),(3),(4);
create table ord(event_ts datetime, value double);
insert into ord values ('2026-01-01 00:00:00',1.0),('2026-01-01 00:00:10',2.0),('2026-01-01 00:00:20',3.0),('2026-01-01 00:00:30',4.0),('2026-01-01 00:00:40',5.0),('2026-01-01 00:00:50',6.0),('2026-01-01 00:01:00',7.0),('2026-01-01 00:01:10',8.0),('2026-01-01 00:01:20',9.0),('2026-01-01 00:01:30',10.0),('2026-01-01 00:01:40',11.0),('2026-01-01 00:01:50',12.0),('2026-01-01 00:02:00',13.0),('2026-01-01 00:02:10',14.0),('2026-01-01 00:02:20',15.0),('2026-01-01 00:02:30',16.0),('2026-01-01 00:02:40',17.0),('2026-01-01 00:02:50',18.0),('2026-01-01 00:03:00',19.0),('2026-01-01 00:03:10',20.0);
-- 1,000 devices x 5 metrics x 20 ten-second ordinals, tenant 0.
insert into metrics select 0, d.device_id, m.metric_id, o.event_ts, o.value, 1 from device_seed d, met m, ord o;
select count(*) as total_rows from metrics;
select count(*) as reported_prefix from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts >= '2026-01-01 00:03:00';
select count(*) as reported_value_19 from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts >= '2026-01-01 00:03:00' and value = 19.0;
select count(*) as reported_value_20 from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts >= '2026-01-01 00:03:00' and value = 20.0;
-- Duplicate cluster keys must all be returned by the interval.
insert into metrics values (0,0,0,'2026-01-01 00:04:00',37.5,1),(0,0,0,'2026-01-01 00:04:00',38.5,1),(0,0,0,'2026-01-01 00:04:00',39.5,1);
select count(*) as duplicate_cluster_key_rows from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts = '2026-01-01 00:04:00';
select count(*) as prefix_with_duplicates from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts >= '2026-01-01 00:03:00';
-- NULL key parts must stay invisible to the equality/interval predicate.
insert into metrics values (0,500,3,'2026-01-01 00:01:00',50.5,1),(0,null,4,'2026-01-01 00:02:00',51.5,1);
select count(*) as device_null_rows from metrics where device_id is null;
select count(*) as prefix_after_null_insert from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts >= '2026-01-01 00:03:00';
-- Update of an unflushed matching row.
update metrics set value = 90.5 where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts = '2026-01-01 00:03:10' and value = 20.0;
select count(*) as updated_rows from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts >= '2026-01-01 00:03:00' and value = 90.5;
select count(*) as old_value_gone from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts >= '2026-01-01 00:03:00' and value = 20.0;
-- Delete of an unflushed row.
delete from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts = '2026-01-01 00:00:00';
select count(*) as deleted_value_rows from metrics where value = 1.0;
select count(*) as prefix_after_delete from metrics where tenant_id = 0 and device_id = 0 and metric_id = 0 and event_ts >= '2026-01-01 00:03:00';
-- Rollback keeps unflushed state unchanged.
start transaction;
insert into metrics values (0,1,0,'2026-01-01 00:05:30',99.5,1);
rollback;
select count(*) as rolled_back_rows from metrics where tenant_id = 0 and device_id = 1 and metric_id = 0 and event_ts = '2026-01-01 00:05:30';
-- Commit makes the row visible to a fresh strongly consistent read.
start transaction;
insert into metrics values (0,1,0,'2026-01-01 00:06:30',98.5,1);
commit;
select count(*) as committed_rows from metrics where tenant_id = 0 and device_id = 1 and metric_id = 0 and event_ts = '2026-01-01 00:06:30';
-- A predicate without a leading cluster-key part returns correct results
-- through the general path.
select count(*) as nonleading_count from metrics where device_id = 999 and metric_id = 4 and event_ts >= '2026-01-01 00:00:00';
drop database ts_harness;
