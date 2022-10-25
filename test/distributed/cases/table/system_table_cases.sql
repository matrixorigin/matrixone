-- @suit
-- @case
-- @desc:test for Some System tables status, content, availability, and so on...
-- @label:bvt

USE mo_catalog;
SHOW TABLES;
-- mo_database
SELECT COUNT(*) FROM mo_database;
SHOW COLUMNS FROM mo_database;

-- mo_tables
SHOW COLUMNS FROM mo_tables;
SELECT COUNT(*) FROM (SELECT * FROM mo_tables LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM mo_tables LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM mo_tables LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM mo_tables LIMIT 10) AS temp;

-- mo_columns
SHOW COLUMNS FROM mo_columns;
SELECT att_database_id, attr_has_update, att_length FROM mo_columns LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM mo_columns LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM mo_columns LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM mo_columns LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM mo_columns LIMIT 10) AS temp;

-- mo_user
SHOW COLUMNS FROM mo_user;
SELECT user_id, user_host, user_name, status FROM mo_user LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM mo_user LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM mo_user LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM mo_user LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM mo_user LIMIT 10) AS temp;

-- mo_account
SHOW COLUMNS FROM mo_account;
SELECT account_id, account_name, status FROM mo_account LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM mo_account LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM mo_account LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM mo_account LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM mo_account LIMIT 10) AS temp;

-- mo_role
SHOW COLUMNS FROM mo_role;
SELECT role_id, role_name, creator FROM mo_role LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM mo_role LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM mo_role LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM mo_role LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM mo_role LIMIT 10) AS temp;

-- mo_user_grant
SHOW COLUMNS FROM mo_user_grant;
SELECT role_id, user_id, with_grant_option FROM mo_user_grant LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM mo_user_grant LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM mo_user_grant LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM mo_user_grant LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM mo_user_grant LIMIT 10) AS temp;

-- mo_role_grant
SHOW COLUMNS FROM mo_role_grant;
SELECT granted_id, grantee_id, operation_role_id, operation_user_id FROM mo_role_grant LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM mo_role_grant LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM mo_role_grant LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM mo_role_grant LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM mo_role_grant LIMIT 10) AS temp;

-- mo_role_privs
SHOW COLUMNS FROM mo_role_privs;
SELECT role_id, role_name, obj_type, obj_id, privilege_id FROM mo_role_privs LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM mo_role_privs LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM mo_role_privs LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM mo_role_privs LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM mo_role_privs LIMIT 10) AS temp;

-- tables in system
USE system;
SHOW TABLES;
-- statement_info
SHOW COLUMNS FROM statement_info;
SELECT account, user, host FROM statement_info LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM statement_info LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM statement_info LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM statement_info LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM statement_info LIMIT 10) AS temp;

-- @bvt:issue#5895
(SELECT * FROM statement_info LIMIT 1) UNION ALL (SELECT * FROM statement_info LIMIT 1);
-- @bvt:issue

-- rawlog
SHOW COLUMNS FROM rawlog;
SELECT COUNT(*) FROM (SELECT * FROM rawlog LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM rawlog LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM rawlog LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM rawlog LIMIT 10) AS temp;

-- @bvt:issue#5892
SELECT * FROM rawlog LIMIT 1;
SELECT * FROM log_info LIMIT 1;
-- @bvt:issue

-- log_info
SHOW COLUMNS FROM log_info;
SELECT COUNT(*) FROM (SELECT * FROM log_info LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM log_info LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM log_info LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM log_info LIMIT 10) AS temp;

-- error_info
SHOW COLUMNS FROM error_info;

-- @bvt:issue#5901
SELECT COUNT(*) FROM (SELECT * FROM error_info LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM error_info LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM error_info LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM error_info LIMIT 10) AS temp;
-- @bvt:issue

-- span_info
SHOW COLUMNS FROM span_info;
SELECT COUNT(*) FROM (SELECT * FROM span_info LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM span_info LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM span_info LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM span_info LIMIT 10) AS temp;

-- tables in system_metrics
USE system_metrics;
SHOW TABLES;
-- metric
SHOW COLUMNS FROM metric;
SELECT metric_name, role, account FROM metric LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM metric LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM metric LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM metric LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM metric LIMIT 10) AS temp;

-- sql_statement_total
SHOW COLUMNS FROM sql_statement_total;
SELECT value, account, role FROM sql_statement_total LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM sql_statement_total LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sql_statement_total LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sql_statement_total LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sql_statement_total LIMIT 10) AS temp;

-- sql_statement_errors
SHOW COLUMNS FROM sql_statement_errors;
SELECT COUNT(NULL) FROM (SELECT * FROM sql_statement_errors LIMIT 10) AS temp;

-- sql_transaction_total
SHOW COLUMNS FROM sql_transaction_total;
SELECT value, account, role FROM sql_transaction_total LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM sql_transaction_total LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sql_transaction_total LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sql_transaction_total LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sql_transaction_total LIMIT 10) AS temp;

-- sql_transaction_errors
SHOW COLUMNS FROM sql_transaction_errors;
SELECT value, account, role FROM sql_transaction_errors LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM sql_transaction_errors LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sql_transaction_errors LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sql_transaction_errors LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sql_transaction_errors LIMIT 10) AS temp;

-- server_connections
SHOW COLUMNS FROM server_connections;
SELECT account, role, value FROM server_connections LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM server_connections LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM server_connections LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM server_connections LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM server_connections LIMIT 10) AS temp;

-- process_cpu_percent
SHOW COLUMNS FROM process_cpu_percent;
SELECT COUNT(*) FROM (SELECT * FROM process_cpu_percent LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM process_cpu_percent LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM process_cpu_percent LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM process_cpu_percent LIMIT 10) AS temp;

-- process_resident_memory_bytes
SHOW COLUMNS FROM process_resident_memory_bytes;
SELECT COUNT(*) FROM (SELECT * FROM process_resident_memory_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM process_resident_memory_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM process_resident_memory_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM process_resident_memory_bytes LIMIT 10) AS temp;

-- process_open_fds
SHOW COLUMNS FROM process_open_fds;
SELECT COUNT(*) FROM (SELECT * FROM process_open_fds LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM process_open_fds LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM process_open_fds LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM process_open_fds LIMIT 10) AS temp;

-- sys_cpu_seconds_total
SHOW COLUMNS FROM sys_cpu_seconds_total;
SELECT COUNT(*) FROM (SELECT * FROM sys_cpu_seconds_total LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_cpu_seconds_total LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_cpu_seconds_total LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_cpu_seconds_total LIMIT 10) AS temp;

-- sys_cpu_combined_percent
SHOW COLUMNS FROM sys_cpu_combined_percent;
SELECT COUNT(*) FROM (SELECT * FROM sys_cpu_combined_percent LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_cpu_combined_percent LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_cpu_combined_percent LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_cpu_combined_percent LIMIT 10) AS temp;

-- sys_memory_used
SHOW COLUMNS FROM sys_memory_used;
SELECT COUNT(*) FROM (SELECT * FROM sys_memory_used LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_memory_used LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_memory_used LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_memory_used LIMIT 10) AS temp;

-- sys_memory_available
SHOW COLUMNS FROM sys_memory_available;
SELECT COUNT(*) FROM (SELECT * FROM sys_memory_available LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_memory_available LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_memory_available LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_memory_available LIMIT 10) AS temp;

-- sys_disk_read_bytes
SHOW COLUMNS FROM sys_disk_read_bytes;
SELECT COUNT(*) FROM (SELECT * FROM sys_disk_read_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_disk_read_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_disk_read_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_disk_read_bytes LIMIT 10) AS temp;

-- sys_disk_write_bytes
SHOW COLUMNS FROM sys_disk_write_bytes;
SELECT COUNT(*) FROM (SELECT * FROM sys_disk_write_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_disk_write_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_disk_write_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_disk_write_bytes LIMIT 10) AS temp;

-- sys_net_recv_bytes
SHOW COLUMNS FROM sys_net_recv_bytes;
SELECT COUNT(*) FROM (SELECT * FROM sys_net_recv_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_net_recv_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_net_recv_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_net_recv_bytes LIMIT 10) AS temp;

-- sys_net_sent_bytes
SHOW COLUMNS FROM sys_net_sent_bytes;
SELECT COUNT(*) FROM (SELECT * FROM sys_net_sent_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_net_sent_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_net_sent_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_net_sent_bytes LIMIT 10) AS temp;

-- tables in mysql
USE mysql;
SHOW TABLES;
-- user
SHOW COLUMNS FROM user;
SELECT host, user, select_priv, insert_priv, update_priv FROM user LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM user LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM user LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM user LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM user LIMIT 10) AS temp;

-- db
SHOW COLUMNS FROM db;
SELECT host, db, user, select_priv FROM db LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM db LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM db LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM db LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM db LIMIT 10) AS temp;

-- procs_priv
SHOW COLUMNS FROM procs_priv;
SELECT host, db, user, routine_name FROM procs_priv LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM procs_priv LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM procs_priv LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM procs_priv LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM procs_priv LIMIT 10) AS temp;

-- columns_priv
SHOW COLUMNS FROM columns_priv;
SELECT host, db, user, table_name FROM columns_priv LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM columns_priv LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM columns_priv LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM columns_priv LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM columns_priv LIMIT 10) AS temp;

-- tables_priv
SHOW COLUMNS FROM tables_priv;
SELECT table_name, grantor, user, host FROM tables_priv LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM tables_priv LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM tables_priv LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM tables_priv LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM tables_priv LIMIT 10) AS temp;

-- tables in information_schema
USE information_schema;
SHOW TABLES;
-- key_column_usage
SHOW COLUMNS FROM key_column_usage;
SELECT constraint_catalog, constraint_schema, constraint_name FROM key_column_usage LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM key_column_usage LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM key_column_usage LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM key_column_usage LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM key_column_usage LIMIT 10) AS temp;

-- columns
SHOW COLUMNS FROM columns;
SELECT column_name, ordinal_position, column_default FROM columns LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM columns LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM columns LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM columns LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM columns LIMIT 10) AS temp;

-- profiling
SHOW COLUMNS FROM profiling;
SELECT query_id, seq, state FROM profiling LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM profiling LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM profiling LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM profiling LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM profiling LIMIT 10) AS temp;

-- user_privileges
SHOW COLUMNS FROM user_privileges;
SELECT grantee, table_catalog FROM user_privileges LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM user_privileges LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM user_privileges LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM user_privileges LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM user_privileges LIMIT 10) AS temp;

-- schemata
SHOW COLUMNS FROM schemata;
SELECT catalog_name, schema_name, sql_path FROM schemata LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM schemata LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM schemata LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM schemata LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM schemata LIMIT 10) AS temp;

-- character_sets
SHOW COLUMNS FROM character_sets;
SELECT character_set_name, description FROM character_sets LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM character_sets LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM character_sets LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM character_sets LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM character_sets LIMIT 10) AS temp;

-- triggers
SHOW COLUMNS FROM triggers;
SELECT trigger_catalog, trigger_schema, trigger_name FROM triggers LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM triggers LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM triggers LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM triggers LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM triggers LIMIT 10) AS temp;

-- tables
SHOW COLUMNS FROM tables;
SELECT table_name, table_type, engine, version FROM tables LIMIT 1;
SELECT COUNT(*) FROM (SELECT * FROM tables LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM tables LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM tables LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM tables LIMIT 10) AS temp;
