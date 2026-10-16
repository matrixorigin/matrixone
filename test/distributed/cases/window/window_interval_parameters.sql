-- @suite
-- @case
-- @desc: temporal frame parameter reuse and MySQL interval string semantics (#30290, #30292)
-- @label:bvt

drop database if exists window_interval_parameters;
create database window_interval_parameters;
use window_interval_parameters;
create table dt(id int primary key, at datetime(6), v int);
insert into dt values (1,'2026-01-01 00:00:00',10),(2,'2026-01-01 00:00:00.000500',20),(3,'2026-01-01 00:00:01.499999',30),(4,'2026-01-01 00:00:01.500000',40),(5,'2026-01-01 00:00:01.500001',50),(6,'2026-01-01 00:00:02',60);
select id, sum(v) over (order by at range between interval '0 00:00:01.5' day_second preceding and current row) as s from dt order by id;
select id, sum(v) over (order by at range between interval '00:00:01.5' hour_second preceding and current row) as s from dt order by id;
select id, sum(v) over (order by at range between interval '00:01.5' minute_second preceding and current row) as s from dt order by id;
select id, sum(v) over (order by at range between interval 'abc' day preceding and current row) as s from dt order by id;
select id, sum(v) over (order by at range between interval 1.5 second preceding and current row) as s from dt order by id;
select id, sum(v) over (order by at range between interval '0:01' minute_second preceding and current row) as s from dt order by id;
select id, sum(v) over (order by at range between interval TRUE second preceding and current row) as s from dt order by id;
prepare interval_frame from "select id, sum(v) over (order by at range between interval ? second preceding and current row) as s from dt order by id";
set @n=0;
execute interval_frame using @n;
set @n=1.5;
execute interval_frame using @n;
set @n=0.000001;
execute interval_frame using @n;
set @n=-1;
execute interval_frame using @n;
set @n=NULL;
execute interval_frame using @n;
set @n=0;
execute interval_frame using @n;
-- Boolean parameters use numeric 0/1 and recover after invalid executions.
set @n=TRUE;
execute interval_frame using @n;
set @n=FALSE;
execute interval_frame using @n;
deallocate prepare interval_frame;
prepare numeric_frame from "select id, sum(v) over (order by id range between ? preceding and current row) as s from dt order by id";
set @n=1;
execute numeric_frame using @n;
deallocate prepare numeric_frame;
select id, sum(v) over (order by at range between interval '-0:01' minute_second preceding and current row) as s from dt order by id;
drop database window_interval_parameters;
