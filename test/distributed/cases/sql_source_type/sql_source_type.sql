-- prepare
create account if not exists `sql_source_type` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

/* cloud_user */drop table if exists __mo_t1;
/* cloud_nonuser */ create table __mo_t1(a int);
insert into __mo_t1 values(1);
select * from __mo_t1;
/* cloud_nonuser */ use system;/* cloud_user */show tables;
-- @session:id=1&user=sql_source_type:admin:accountadmin&password=123456
select sleep(15);
-- @session
select statement, sql_source_type from system.statement_info where user="dump" order by request_at desc limit 4;

drop account if exists sql_source_type;
