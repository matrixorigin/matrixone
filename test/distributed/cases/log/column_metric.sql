select metric_name, collecttime, value, `node`, role, account, type from system_metrics.metric limit 0;
-- issue https://github.com/matrixorigin/MO-Cloud/issues/2569
select count(1) cnt, metric_name, collecttime, `node`, role, account, type from system_metrics.metric group by metric_name, collecttime, `node`, role, account, type having cnt > 1;
-- issue https://github.com/matrixorigin/MO-Cloud/issues/2569, check server_storage_usage exist
select count(1) > 0 exist from system_metrics.metric where collecttime > date_sub(now(), interval 5 minute) and metric_name = 'server_storage_usage';
