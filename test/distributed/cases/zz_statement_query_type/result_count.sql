-- origin op: in ../result_count/result_count.sql

-- result check
use system;

-- check case 1
-- PS: We do not have the source sql of 'select (select 1) from dual'. We just have the ast of 'select (select 1) from dual'.
--     So the statement_info.statement for 'select (select 1) from dual' only is empty string.
--     ==> NEED condition filter: length(statement) > 0
set @case_name="case1";
select statement, result_count from statement_info where account="bvt_result_count" and statement not like '%mo_ctl%' and length(statement) > 0 and status != 'Running' and aggr_count < 1 order by request_at desc limit 68;

-- check case 2
set @case_name="case2";
select statement, result_count from statement_info where user="dump" and sql_source_type="cloud_user_sql" and status != 'Running' and statement like '%bvt_result_count_test_case2%' and aggr_count < 1 order by request_at desc limit 2;
