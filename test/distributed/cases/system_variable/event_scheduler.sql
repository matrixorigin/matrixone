-- @suit

-- @case
-- @desc:event_scheduler is not supported, the variable should report DISABLED instead of ON
-- @label:bvt

show variables like 'event_scheduler';
select @@event_scheduler;
select @@global.event_scheduler;
