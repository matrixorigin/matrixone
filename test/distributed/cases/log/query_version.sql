-- others like select version();
set @ts=now();
select @@version_comment limit 1;

select sleep(15) as s;
use system;
use mysql;
select * from user limit 0;

select sleep(15) as s;

select count(1) as cnt, statement_id, statement, status from system.statement_info group by statement_id, statement, status having count(1) > 1;

select `database`, `aggr_count` from system.statement_info where statement LIKE '%select * from user limit 0%' order by request_at desc limit 1;
