-- others like select version();
select @@version_comment limit 1;

set @ts=now();

use system;
use mysql; select * from user limit 0;

select sleep(15) as s;

select count(1) as cnt, statement_id, statement, status from system.statement_info group by statement_id, statement, status having count(1) > 1;

select `database` from system.statement_info where statement = 'select * from user limit 0' and request_at > @ts order by request_at desc limit 1;
