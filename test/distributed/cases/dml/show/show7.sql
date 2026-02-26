drop account if exists show_table_status_acc_10163;
create account show_table_status_acc_10163 admin_name = 'admin' identified by '111';

-- @session:id=1&user=show_table_status_acc_10163:admin&password=111
select 1;

-- @bvt:issue#10163
-- @ignore:1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
-- @regex("(?m)^statement_info\\s+Tae\\s+Dynamic\\s+[1-9][0-9]*\\s+",true)
show table status from system like 'statement_info';
-- @bvt:issue

-- @session
drop account show_table_status_acc_10163;
