-- for issue 14,836, prepare op in ../log/query_version.sql
select `database`, `aggr_count` from system.statement_info where statement = 'select * from user limit 0' and sql_source_type = 'cloud_user_sql' order by request_at desc limit 1;
