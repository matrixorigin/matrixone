-- 1.在系统租户下，查看mo_catalog下的系统视图mo_locks, mo_cache, mo_transactions, mo_variables, mo_configurations, mo_sessions的列定义是某正确
use mo_catalog;
desc mo_locks;
desc mo_cache;
desc mo_transactions;
desc mo_variables;
desc mo_configurations;
desc mo_sessions;

-- 2.在普通租户下，查看mo_catalog下的系统视图mo_locks, mo_cache, mo_transactions, mo_variables, mo_configurations, mo_sessions的列定义是某正确
create account acc100 admin_name='root' identified by '123456';
-- @session:id=2&user=acc100:root:accountadmin&password=123456
use mo_catalog;
desc mo_locks;
desc mo_cache;
desc mo_transactions;
desc mo_variables;
desc mo_configurations;
desc mo_sessions;

-- @session
drop account acc100;