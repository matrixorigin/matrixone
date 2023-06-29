-- prepare
drop account if exists bvt_purge_log;
create account bvt_purge_log admin_name 'admin' identified by '111';

-- check not support
-- @session:id=2&user=bvt_purge_log:admin&password=111
select purge_log('rawlog', '2023-06-30') a;
-- @session

-- check valid args
select purge_log('rawlog', '2023-06-30') a;
select purge_log('statement_info', '2023-06-30') a;
select purge_log('metric', '2023-06-30') a;
select purge_log('rawlog,statement_info,metric', '2023-06-30') a;

-- check invalid args
select purge_log('rawlog_not_exist', '2023-06-30') a;
select purge_log('rawlog_not_exist', '123') a;

-- check null arg
select purge_log('rawlog_not_exist', NULL) a;
select purge_log(NULL, '2023-06-30') a;
select purge_log(NULL, NULL) a;
