
-- for issue 14,836 and 15,988, prepare op in ../log/query_stmt.sql

-- case 1: fix issue 8168, with syntax error
select status, err_code, error from system.statement_info where account = 'bvt_query_stmt' and statement in ('use bvt_query_stmt', 'select syntax error stmt', '/*issue_8168*/use bvt_query_stmt') and status != 'Running' order by request_at desc limit 3;

-- case 2
select /*case 2*/ account from system.statement_info where statement = 'create table bvt_query_stmt (i int)' order by request_at desc limit 1;

-- case 3
select /*case 3*/ account, statement from system.statement_info where statement = 'insert into bvt_query_stmt values (1)' order by request_at desc limit 1;

-- case 4: hide result
select /*case 4*/ statement from system.statement_info where statement_type in ('Create User', 'Create Table', 'Select') and account = 'bvt_query_stmt' order by request_at desc limit 4;

-- case: select span_kind issue #7571
select IF(span_kind="internal", 1, IF(span_kind="statement", 1, IF(span_kind="session", 1, IF(span_kind="remote", 1, 0)))) as exist from system.rawlog where `raw_item` = "log_info" limit 1;
