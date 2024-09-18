-- check result at the end of bvt.
-- check result of statement_query_type/aggr_error_stmt.sql

-- Tips: cnt +1000, sum + 1000 both make sure result format is current.
-- @ignore:3,4
select statement, error, count(1) < sum(IF(aggr_count=0, 1, aggr_count)) check_result, count(1) cnt, sum(IF(aggr_count=0, 1, aggr_count)) sum from system.statement_info where account="bvt_aggr_error_stmt" and sql_source_type="cloud_nonuser_sql" group by `statement`, error;
