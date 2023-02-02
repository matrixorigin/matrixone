/* cloud_user */drop table if exists __mo_t1;
/* cloud_nonuser */ create table __mo_t1(a int);
insert into __mo_t1 values(1);
select * from __mo_t1;
use system;
select sleep(15);
select statement, sql_source_type from statement_info where user="dump" order by request_at desc limit 5;