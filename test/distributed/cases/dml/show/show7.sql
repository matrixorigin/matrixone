drop account if exists show_table_status_acc_10163;
create account show_table_status_acc_10163 admin_name = 'admin' identified by '111';

-- @session:id=1&user=show_table_status_acc_10163:admin&password=111
select 1;
select 2;
select 3;
select case when count(*) > 0 then 0 else sleep(5) end as wait_for_statement_info_ready from system.statement_info;
select case when count(*) > 0 then 0 else sleep(5) end as wait_for_statement_info_ready from system.statement_info;
select case when count(*) > 0 then 0 else sleep(5) end as wait_for_statement_info_ready from system.statement_info;

select count(*) > 0 as has_data from system.statement_info;
-- @ignore:0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
-- @metacmp(false)
-- @regex("(?m)^statement_info.*Tae.*Dynamic.*[1-9][0-9]*.*$",true)
show table status from system like 'statement_info';

-- @session
drop account show_table_status_acc_10163;
