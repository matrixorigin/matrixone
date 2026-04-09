drop database if exists sql_task_phase6;
create database sql_task_phase6;
use sql_task_phase6;

drop task if exists task_phase6_cron;
drop task if exists task_phase6_manual;
drop task if exists task_phase6_gate;
drop task if exists task_phase6_multistmt;
drop task if exists task_phase6_timeout;
drop task if exists task_phase6_retry;
drop task if exists task_phase6_overlap;
drop task if exists task_build_silver;
drop task if exists task_build_gold;
drop task if exists task_validate;

create table scheduled_events(
    marker varchar(32) not null,
    created_at timestamp default current_timestamp
);
create table manual_events(id int primary key);
create table gate_source(id int primary key);
create table gate_sink(tag varchar(32) primary key);
create table ms_target(v int);
create table timeout_sink(v int);
create table retry_target(v int);
create table overlap_sink(v int);
create table suspend_snapshot(cnt bigint);

create table ingest_meta(batch_id int primary key, ready int not null);
create table raw_orders(order_id int primary key, amount int not null);
create table silver_orders(order_id int primary key, amount int not null);
create table gold_summary(summary_id int primary key, total_orders int not null, total_amount int not null);
create table validate_results(result_id int primary key, status varchar(16) not null);

create task task_phase6_cron
    schedule '0 0 0 1 1 *'
    timezone 'UTC'
as begin
    insert into scheduled_events(marker) values ('cron');
end;

create task task_phase6_manual
as begin
    insert into manual_events
    select 1
    where not exists (select 1 from manual_events where id = 1);
end;

select sleep(2);
select count(*) from scheduled_events;
select count(*) from manual_events;
select count(*) from mo_task.sql_task_run where task_name = 'task_phase6_cron';
select count(*) from mo_task.sql_task_run where task_name = 'task_phase6_manual';

alter task task_phase6_cron set schedule '*/1 * * * * *' timezone 'UTC';
select sleep(7);
select count(*) >= 1 from scheduled_events;
select count(*) >= 1 from mo_task.sql_task_run where task_name = 'task_phase6_cron' and trigger_type = 'SCHEDULED' and status = 'SUCCESS';

execute task task_phase6_manual;
select count(*) from manual_events;

-- @ignore:6,8
show tasks;
-- @ignore:0,4,5,6
show task runs for task_phase6_cron limit 1;
-- @ignore:0,4,5,6
show task runs for task_phase6_manual limit 1;

alter task task_phase6_cron suspend;
truncate table suspend_snapshot;
insert into suspend_snapshot select count(*) from scheduled_events;
select sleep(4);
select (select cnt from suspend_snapshot limit 1) = (select count(*) from scheduled_events);

alter task task_phase6_cron set schedule '0 0 0 1 1 *' timezone 'UTC';
truncate table suspend_snapshot;
insert into suspend_snapshot select count(*) from scheduled_events;
alter task task_phase6_cron resume;
select sleep(4);
select (select cnt from suspend_snapshot limit 1) = (select count(*) from scheduled_events);

create task task_phase6_gate
    when (exists(select 1 from gate_source where id = 1))
as begin
    insert into gate_sink
    select 'gate-ok'
    where not exists (select 1 from gate_sink where tag = 'gate-ok');
end;

execute task task_phase6_gate;
select count(*) from gate_sink;
insert into gate_source values (1);
execute task task_phase6_gate;
select count(*) from gate_sink;
-- @sortkey:0,1
select trigger_type, status, count(*)
from mo_task.sql_task_run
where task_name = 'task_phase6_gate'
group by trigger_type, status;

create task task_phase6_multistmt
as begin
    insert into ms_target values (1);
    insert into no_such_table values (1);
    insert into ms_target values (2);
end;

-- @pattern
execute task task_phase6_multistmt;
select count(*), min(v), max(v) from ms_target;
select status, count(*) from mo_task.sql_task_run where task_name = 'task_phase6_multistmt' group by status order by status;

create task task_phase6_timeout
    timeout '1s'
as begin
    insert into timeout_sink select sleep(2);
end;

-- @pattern
execute task task_phase6_timeout;
select count(*) from timeout_sink;
select status, count(*) from mo_task.sql_task_run where task_name = 'task_phase6_timeout' group by status order by status;

create task task_phase6_retry
    retry 1
as begin
    insert into retry_target values (1);
    insert into no_such_retry_table values (1);
end;

-- @pattern
execute task task_phase6_retry;
select count(*) from retry_target;
select status, count(*) from mo_task.sql_task_run where task_name = 'task_phase6_retry' group by status order by status;

create task task_phase6_overlap
as begin
    insert into overlap_sink select sleep(4);
end;

-- @session:id=1{
use sql_task_phase6;
execute task task_phase6_overlap;
-- @session}

select sleep(1);
-- @pattern
execute task task_phase6_overlap;
select sleep(5);
select count(*) from overlap_sink;
select status, count(*) from mo_task.sql_task_run where task_name = 'task_phase6_overlap' group by status order by status;

insert into raw_orders values (1, 10), (2, 20);

create task task_build_silver
    when (exists(select 1 from ingest_meta where batch_id = 1 and ready = 1))
as begin
    insert into silver_orders
    select r.order_id, r.amount
    from raw_orders r
    where not exists (
        select 1 from silver_orders s where s.order_id = r.order_id
    );
end;

create task task_build_gold
    when (exists(select 1 from silver_orders))
as begin
    delete from gold_summary where summary_id = 1;
    insert into gold_summary
    select 1, count(*), sum(amount) from silver_orders;
end;

create task task_validate
    when (exists(select 1 from gold_summary where summary_id = 1))
as begin
    delete from validate_results where result_id = 1;
    insert into validate_results
    select 1, case when total_orders = 2 and total_amount = 30 then 'PASS' else 'FAIL' end
    from gold_summary
    where summary_id = 1;
end;

execute task task_validate;
execute task task_build_gold;
execute task task_build_silver;
insert into ingest_meta values (1, 1);
execute task task_build_silver;
execute task task_build_gold;
execute task task_validate;
execute task task_validate;

select count(*) from silver_orders;
select total_orders, total_amount from gold_summary where summary_id = 1;
select status from validate_results where result_id = 1;
-- @sortkey:0,1
select task_name, status, count(*)
from mo_task.sql_task_run
where task_name in ('task_build_silver', 'task_build_gold', 'task_validate')
group by task_name, status;
select count(*) from validate_results;

-- @ignore:6,8
show tasks;
-- @ignore:0,4,5,6
show task runs for task_validate limit 3;
