-- The session timer interrupts a read-only SELECT and leaves the connection usable.
set @saved_max_execution_time = @@session.max_execution_time;
set session max_execution_time = 500;
select sleep(3) as slept;
select 1 as reusable;

-- Zero disables the timer for a SELECT that exceeds the former limit.
set session max_execution_time = 0;
select sleep(0.75) as slept;
set session max_execution_time = @saved_max_execution_time;
