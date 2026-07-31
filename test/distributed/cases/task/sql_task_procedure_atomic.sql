-- Copyright 2026 Matrix Origin
-- Licensed under the Apache License, Version 2.0.

drop database if exists sql_task_procedure_atomic;
create database sql_task_procedure_atomic;
use sql_task_procedure_atomic;

create table target_rows(id int primary key);
create table ingest_control(id int primary key, watermark int not null);
insert into ingest_control values (1, 0);

create procedure ingest_once() '
begin
    insert into target_rows values (1);
    update ingest_control set watermark = 1 where id = 1;
    insert into table_that_does_not_exist values (1);
end';

-- A direct CALL must roll back both the target and the checkpoint.
call ingest_once();
select count(*) as target_count from target_rows;
select watermark from ingest_control where id = 1;

-- SQL TASK must preserve the same all-or-nothing contract when its body is a
-- single CALL. This is the transaction shape used by MongoDB ingestion.
create task task_ingest_once as begin call ingest_once(); end;
execute task task_ingest_once;
select count(*) as target_count from target_rows;
select watermark from ingest_control where id = 1;
select status
  from mo_task.sql_task_run
 where task_name = 'task_ingest_once'
 order by run_id desc
 limit 1;

drop task task_ingest_once;
drop procedure ingest_once;
drop database sql_task_procedure_atomic;
