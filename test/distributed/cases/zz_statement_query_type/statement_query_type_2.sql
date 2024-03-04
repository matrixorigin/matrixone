-- check result at the end of bvt.
-- check result of statement_query_type/statement_query_type_2.sql

-- RESULT CHECK: part 2
select statement,query_type,sql_source_type from  system.statement_info where account="bvt_query_type" and sql_source_type="cloud_user_sql" and status != "Running" and statement not like '%mo_ctl%' order by request_at desc limit 66;
