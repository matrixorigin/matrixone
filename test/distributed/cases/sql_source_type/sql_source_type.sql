-- prepare
drop account if exists bvt_sql_source_type;
create account if not exists `bvt_sql_source_type` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- testcase
-- @session:id=1&user=bvt_sql_source_type:admin:accountadmin&password=123456
create database if not exists ssb;
use ssb;
/* cloud_user */drop table if exists __mo_t1;
/* cloud_nonuser */ create table __mo_t1(a int);
/* cloud_user */insert into __mo_t1 values(1);
/* cloud_user */select * from __mo_t1;
/* cloud_nonuser */ use system;/* cloud_user */show tables;
-- @session

-- result check, issue 14,836 move into ../zz_statement_query_type
-- select statement, sql_source_type from system.statement_info where account="bvt_sql_source_type" and status != 'Running' and statement not like '%mo_ctl%' order by request_at desc limit 4;

-- cleanup
drop account if exists bvt_sql_source_type;
