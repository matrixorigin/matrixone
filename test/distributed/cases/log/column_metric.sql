select metric_name, collecttime, value, `node`, role, account, type from system_metrics.metric limit 0;
-- issue https://github.com/matrixorigin/MO-Cloud/issues/2569
select count(1) cnt, collecttime, `node`, role, account, type from system_metrics.metric group by collecttime, `node`, role, account, type having cnt > 1;
