-- check case 2, issue: 14,836
set @start_ts=(select start_time from bvt_result_count.result where name = 'case2' order by `timestamp` desc limit 1);
set @end_ts=(select end_time from bvt_result_count.result where name = 'case2' order by `timestamp` desc limit 1);
-- @ignore:2,3
select statement, result_count, request_at, concat(@start_ts, ', ', @end_ts) time_range from system.statement_info where user="dump" and sql_source_type="cloud_user_sql" and status != 'Running' and aggr_count < 1 and request_at between date_sub(@start_ts, interval 1 second) and date_add(@end_ts, interval 1 second) order by request_at desc limit 2;
