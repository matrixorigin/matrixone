-- others like select version();
select @@version_comment limit 1;

select sleep(15) as s;

select count(1) as cnt, statement_id, statement, status from system.statement_info group by statement_id, statement, status having count(1) > 1;
