-- check result at the end of bvt.
-- check result of statement_query_type/aggr_error_stmt.sql

-- Tips: cnt +1000, sum + 1000 both make sure result format is current.
-- @ignore:2,3,4
select error, count(1) < sum(aggr_count) check_result, count(1) cnt, sum(aggr_count) sum, statement from system.statement_info where account="bvt_aggr_error_stmt" and sql_source_type="cloud_nonuser_sql" group by `statement`, error;
