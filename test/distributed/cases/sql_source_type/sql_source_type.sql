/* cloud_user */drop table if exists __mo_t1;
/* cloud_nouser */ create table __mo_t1(a int);
insert into __mo_t1 values(1);
select * from __mo_t1;
use system;
select sleep(15);
select statement, sql_source_type from statement_info where sql_source_type = 'cloud_user_sql' limit 1;

select statement, sql_source_type from statement_info where sql_source_type = 'cloud_nouser_sql' limit 1;

select statement, sql_source_type from statement_info where statement = 'insert into __mo_t1 values (1)' limit 1;
