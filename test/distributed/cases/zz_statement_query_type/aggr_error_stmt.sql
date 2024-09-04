-- check result at the end of bvt.
-- check result of statement_query_type/aggr_error_stmt.sql

-- @ignore:1,2
select count(1) < sum(aggr_count) check_result, count(1) cnt, sum(aggr_count) sum, statement, error from system.statement_info where account="bvt_aggr_error_stmt" and sql_source_type="cloud_nonuser_sql" group by `statement`, error;
