-- check result at the end of bvt.
-- check result of statement_query_type/statement_query_type_1.sql

-- RESULT CHECK: part 1
select statement,query_type,sql_source_type from  system.statement_info where account="bvt_query_type_part1" and sql_source_type="external_sql" and status != "Running" and statement not like '%mo_ctl%' and aggr_count <1 order by request_at desc limit 96;
