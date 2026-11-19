-- Copyright 2026 Matrix Origin
-- Licensed under the Apache License, Version 2.0.

drop task if exists sql_task_db_unqualified;
drop task if exists sql_task_db_qualified;
drop task if exists sql_task_db_constant;
drop task if exists sql_task_db_scheduled;
drop task if exists sql_task_db_bound;
drop database if exists sql_task_db_owner;
drop database if exists sql_task_db_caller;
drop database if exists sql_task_db_reset;
create database sql_task_db_owner;
use sql_task_db_owner;
create table sink(v int);
create table gate_source(v int);
insert into gate_source values (1);
create database sql_task_db_caller;
use sql_task_db_caller;
create table sink(v int);
create table gate_source(v int);

-- Clear any inherited default database explicitly.
create database sql_task_db_reset;
use sql_task_db_reset;
drop database sql_task_db_reset;
select database() is null as no_default_database;

-- Every new definition requires a creation-time database context.
create task sql_task_db_unqualified as begin insert into sink values (1); end;
create task sql_task_db_qualified as begin insert into sql_task_db_owner.sink values (1); end;
create task sql_task_db_constant as begin select 1; end;
create task sql_task_db_scheduled schedule '0 0 0 1 1 *' when (1) as begin select 1; end;
select count(*) as rejected_definitions from mo_task.sql_task where task_name in ('sql_task_db_unqualified', 'sql_task_db_qualified', 'sql_task_db_constant', 'sql_task_db_scheduled');

-- Both the gate and body use the creation database after the caller switches.
use sql_task_db_owner;
create task sql_task_db_bound when (exists(select 1 from gate_source where v = 1)) as begin insert into sink values (1); end;
select database_name from mo_task.sql_task where task_name = 'sql_task_db_bound';
use sql_task_db_caller;
execute task sql_task_db_bound;
select count(*) as owner_rows from sql_task_db_owner.sink;
select count(*) as caller_rows from sink;
select status from mo_task.sql_task_run where task_name = 'sql_task_db_bound' order by run_id desc limit 1;

-- NoDB also precedes IF NOT EXISTS, but task execution and management need no USE.
create database sql_task_db_reset;
use sql_task_db_reset;
drop database sql_task_db_reset;
select database() is null as no_default_database;
create task if not exists sql_task_db_bound as begin select 2; end;
select count(*) as existing_definitions from mo_task.sql_task where task_name = 'sql_task_db_bound' and database_name = 'sql_task_db_owner';
execute task sql_task_db_bound;
select count(*) as owner_rows from sql_task_db_owner.sink;
select count(*) as caller_rows from sql_task_db_caller.sink;
alter task sql_task_db_bound suspend;
drop task sql_task_db_bound;

drop database sql_task_db_owner;
drop database sql_task_db_caller;
select count(*) as remaining_definitions from mo_task.sql_task where task_name in ('sql_task_db_unqualified', 'sql_task_db_qualified', 'sql_task_db_constant', 'sql_task_db_scheduled', 'sql_task_db_bound');
select count(*) as rejected_runs from mo_task.sql_task_run where task_name in ('sql_task_db_unqualified', 'sql_task_db_qualified', 'sql_task_db_constant', 'sql_task_db_scheduled');
select count(*) as remaining_databases from mo_catalog.mo_database where datname in ('sql_task_db_owner', 'sql_task_db_caller', 'sql_task_db_reset');
