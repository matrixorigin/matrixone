-- others like select version();
set @ts=now();
select @@version_comment limit 1;

-- fir pr #7971
use system;
use mysql;
/* cloud_user */select * from user limit 0;

select count(1) as cnt, statement_id, statement, status from system.statement_info group by statement_id, statement, status having count(1) > 1;

-- fix issue 14,836: move check-sql into ../zz_statement_query_type/query_version.*
-- select `database`, `aggr_count` from system.statement_info where statement LIKE '%select * from user limit 0%' order by request_at desc limit 1;
