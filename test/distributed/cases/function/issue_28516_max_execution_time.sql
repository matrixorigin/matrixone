-- issue #28516: a timed-out statement must leave its session reusable once
-- the per-statement limit is disabled.
set @@session.max_execution_time = 100;
select sleep(2);
set @@session.max_execution_time = 0;
select 1;

set @@session.max_execution_time = 100;
begin;
select sleep(2);
set @@session.max_execution_time = 0;
select 1;
rollback;

set @@session.max_execution_time = 100;
prepare issue_28516 from 'select sleep(?)';
set @duration = 2;
execute issue_28516 using @duration;
set @@session.max_execution_time = 0;
select 1;
deallocate prepare issue_28516;
