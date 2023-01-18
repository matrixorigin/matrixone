select value < 1 as less_1, account from system_metrics.server_storage_usage order by collecttime desc limit 1;
