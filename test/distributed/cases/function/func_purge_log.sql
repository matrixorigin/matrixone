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
-- case for issue 15,455 - ETLMerge casse
set @ts=(select max(collecttime) from system_metrics.metric);
set @metric_name=(select metric_name from system_metrics.metric where collecttime between @ts and date_add(@ts, interval 1 second) limit 1);
set @node=(select node from system_metrics.metric where collecttime between @ts and date_add(@ts, interval 1 second) and metric_name=@metric_name limit 1);
select purge_log('statement_info,metric', DATE_ADD( @ts, interval 1 day)) a;
-- @ignore:1,2,3
select count(1) cnt, @ts, @metric_name, @node from  system_metrics.metric where collecttime between @ts and date_add(@ts, interval 1 second) and metric_name=@metric_name and node=@node;

-- clean
drop account if exists bvt_purge_log;
