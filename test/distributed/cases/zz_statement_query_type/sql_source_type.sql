-- result check, issue 14,836
select statement, sql_source_type from system.statement_info where account="bvt_sql_source_type" and status != 'Running' and statement not like '%mo_ctl%' order by request_at desc limit 4;
