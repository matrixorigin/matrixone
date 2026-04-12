drop database if exists sql_task_case;
create database sql_task_case;
use sql_task_case;

drop task if exists sql_task_cron;
drop task if exists sql_task_manual;
drop task if exists sql_task_gate;
drop task if exists sql_task_multistmt;
drop task if exists sql_task_timeout;
drop task if exists sql_task_retry;
drop task if exists sql_task_overlap;
drop task if exists sql_task_build_silver;
drop task if exists sql_task_build_gold;
drop task if exists sql_task_validate;

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

-- @delimiter $$
create task sql_task_cron
    schedule '0 0 0 1 1 *'
    timezone 'UTC'
as begin
    insert into scheduled_events(marker) values ('cron');
end
$$
-- @delimiter ;

-- @delimiter $$
create task sql_task_manual
as begin
    insert into manual_events
    select 1
    where not exists (select 1 from manual_events where id = 1);
end
$$
-- @delimiter ;

select sleep(2);
select count(*) from scheduled_events;
select count(*) from manual_events;

alter task sql_task_cron set schedule '*/1 * * * * *' timezone 'UTC';
select sleep(10);
select count(*) from scheduled_events;
select count(*) from mo_task.sql_task_run where task_name = 'sql_task_cron' and trigger_type = 'SCHEDULED' and status = 'SUCCESS';

execute task sql_task_manual;
select count(*) from manual_events;

-- @ignore:0,4,5,6
show task runs for sql_task_cron limit 1;
-- @ignore:0,4,5,6
show task runs for sql_task_manual limit 1;

alter task sql_task_cron suspend;
select sleep(2);
truncate table suspend_snapshot;
insert into suspend_snapshot select count(*) from scheduled_events;
select sleep(4);
select (select cnt from suspend_snapshot limit 1) = (select count(*) from scheduled_events);

alter task sql_task_cron set schedule '0 0 0 1 1 *' timezone 'UTC';
truncate table suspend_snapshot;
insert into suspend_snapshot select count(*) from scheduled_events;
alter task sql_task_cron resume;
select sleep(4);
select (select cnt from suspend_snapshot limit 1) = (select count(*) from scheduled_events);

-- @delimiter $$
create task sql_task_gate
    when (exists(select 1 from gate_source where id = 1))
as begin
    insert into gate_sink
    select 'gate-ok'
    where not exists (select 1 from gate_sink where tag = 'gate-ok');
end
$$
-- @delimiter ;

execute task sql_task_gate;
select count(*) from gate_sink;
insert into gate_source values (1);
execute task sql_task_gate;
select count(*) from gate_sink;
select status
from mo_task.sql_task_run
where task_name = 'sql_task_gate'
order by run_id desc
limit 2;

-- @delimiter $$
create task sql_task_multistmt
as begin
    insert into ms_target values (1);
    insert into no_such_table values (1);
    insert into ms_target values (2);
end
$$
-- @delimiter ;

-- @pattern
execute task sql_task_multistmt;
select count(*), min(v), max(v) from ms_target;
select status from mo_task.sql_task_run where task_name = 'sql_task_multistmt' order by run_id desc limit 1;

-- @delimiter $$
create task sql_task_timeout
    timeout '1s'
as begin
    insert into timeout_sink select sleep(2);
end
$$
-- @delimiter ;

-- @pattern
execute task sql_task_timeout;
select count(*) from timeout_sink;
select status from mo_task.sql_task_run where task_name = 'sql_task_timeout' order by run_id desc limit 1;

-- @delimiter $$
create task sql_task_retry
    retry 1
as begin
    insert into retry_target values (1);
    insert into no_such_retry_table values (1);
end
$$
-- @delimiter ;

-- @pattern
execute task sql_task_retry;
select count(*) from retry_target;
select status from mo_task.sql_task_run where task_name = 'sql_task_retry' order by run_id desc limit 2;

-- @delimiter $$
create task sql_task_overlap
as begin
    insert into overlap_sink select sleep(4);
end
$$
-- @delimiter ;

-- @session:id=1{
use sql_task_case;
execute task sql_task_overlap;
-- @session}

select sleep(1);
execute task sql_task_overlap;
select sleep(5);
select count(*) from overlap_sink;
select status from mo_task.sql_task_run where task_name = 'sql_task_overlap' order by run_id desc limit 2;

use sql_task_case;
drop task if exists sql_task_build_silver;
drop task if exists sql_task_build_gold;
drop task if exists sql_task_validate;
insert into raw_orders values (1, 10), (2, 20);

-- @delimiter $$
create task sql_task_build_silver
    when (exists(select 1 from ingest_meta where batch_id = 1 and ready = 1))
as begin
    insert into silver_orders
    select r.order_id, r.amount
    from raw_orders r
    where not exists (
        select 1 from silver_orders s where s.order_id = r.order_id
    );
end
$$
-- @delimiter ;

-- @delimiter $$
create task sql_task_build_gold
    when (exists(select 1 from silver_orders))
as begin
    delete from gold_summary where summary_id = 1;
    insert into gold_summary
    select 1, count(*), sum(amount) from silver_orders;
end
$$
-- @delimiter ;

-- @delimiter $$
create task sql_task_validate
    when (exists(select 1 from gold_summary where summary_id = 1))
as begin
    delete from validate_results where result_id = 1;
    insert into validate_results
    select 1, case when total_orders = 2 and total_amount = 30 then 'PASS' else 'FAIL' end
    from gold_summary
    where summary_id = 1;
end
$$
-- @delimiter ;

execute task sql_task_validate;
execute task sql_task_build_gold;
execute task sql_task_build_silver;
insert into ingest_meta values (1, 1);
execute task sql_task_build_silver;
execute task sql_task_build_gold;
execute task sql_task_validate;
execute task sql_task_validate;

select count(*) from silver_orders;
select total_orders, total_amount from gold_summary where summary_id = 1;
select status from validate_results where result_id = 1;
select status from mo_task.sql_task_run where task_name = 'sql_task_build_silver' and trigger_type = 'MANUAL' order by run_id desc limit 1;
select status from mo_task.sql_task_run where task_name = 'sql_task_build_gold' and trigger_type = 'MANUAL' order by run_id desc limit 1;
select status from mo_task.sql_task_run where task_name = 'sql_task_validate' and trigger_type = 'MANUAL' order by run_id desc limit 1;
select count(*) from validate_results;
-- @ignore:0,4,5,6
show task runs for sql_task_validate limit 3;

drop account if exists sql_task_tenant;
create account sql_task_tenant admin_name 'admin' identified by '111';

-- @session:id=1&user=sql_task_tenant:admin:accountadmin&password=111
create database if not exists tenant_task_case;
use tenant_task_case;
create table tenant_sink(v int primary key);
create task tenant_task_show
schedule '0 0 0 1 1 *'
timezone 'UTC'
when (1)
timeout '30s'
as begin
insert into tenant_sink
select 1
where not exists (select 1 from tenant_sink where v = 1);
end;
execute task tenant_task_show;
alter task tenant_task_show set when (0);
execute task tenant_task_show;
-- @ignore:6,8
show tasks;
-- @ignore:0,4,5,6
show task runs for tenant_task_show limit 2;
select count(*) from tenant_sink;
select count(*) from mo_task.sql_task_run;
drop task if exists tenant_task_show;
drop database if exists tenant_task_case;
-- @session

drop task if exists sql_task_validate;
drop task if exists sql_task_build_gold;
drop task if exists sql_task_build_silver;
drop task if exists sql_task_overlap;
drop task if exists sql_task_retry;
drop task if exists sql_task_timeout;
drop task if exists sql_task_multistmt;
drop task if exists sql_task_gate;
drop task if exists sql_task_manual;
drop task if exists sql_task_cron;
drop database if exists sql_task_case;
drop account if exists sql_task_tenant;
