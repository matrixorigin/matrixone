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

-- case for issue 10421, 15,455
set @ts=(select IFNULL(max(collecttime), now()) from system_metrics.metric);
select purge_log('statement_info,metric', DATE_ADD( @ts, interval 1 day)) a;
select count(1) val from system_metrics.metric where `collecttime` <  DATE_ADD( @ts, interval 5 minute);

-- clean
drop account if exists bvt_purge_log;
