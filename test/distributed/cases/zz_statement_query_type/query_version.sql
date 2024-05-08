-- for issue 14,836
select `database`, `aggr_count` from system.statement_info where statement LIKE '%select * from user limit 0%' order by request_at desc limit 1;
