-- case: check system_metrics.server_connections running normally, issue #7663

-- create account
create account `query_metric` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- @session:id=1&user=query_metric:admin:accountadmin&password=123456
show databases;
-- @session

-- @session:id=2&user=query_metric:admin:accountadmin&password=123456
show databases;
-- @session


select sleep(16);

select value = 0 as empty_conn from system_metrics.server_connections where account = "query_metric" order by collecttime desc limit 1;
