-- @suit
-- @case
-- @desc:test for Some System tables status, content, availability, and so on...
-- @label:bvt

-- tables in system
USE system;
-- statement_info
SELECT COUNT(*) FROM (SELECT * FROM statement_info LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM statement_info LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM statement_info LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM statement_info LIMIT 10) AS temp;

-- @bvt:issue#5895
(SELECT * FROM statement_info LIMIT 1) UNION ALL (SELECT * FROM statement_info LIMIT 1);
-- @bvt:issue

-- rawlog
SELECT COUNT(*) FROM (SELECT * FROM rawlog LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM rawlog LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM rawlog LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM rawlog LIMIT 10) AS temp;


-- log_info
SELECT COUNT(*) FROM (SELECT * FROM log_info LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM log_info LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM log_info LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM log_info LIMIT 10) AS temp;

-- @bvt:issue#5901
SELECT COUNT(*) FROM (SELECT * FROM error_info LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM error_info LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM error_info LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM error_info LIMIT 10) AS temp;
-- @bvt:issue

-- span_info
-- issue 11,947
-- for now, span_info is mostly close by default, so here is NO enough reocrds in table.
-- keep query to check table is exist.
SELECT COUNT(*) FROM (SELECT * FROM span_info LIMIT 0) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM span_info LIMIT 0) AS temp;
SELECT COUNT('') FROM (SELECT * FROM span_info LIMIT 0) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM span_info LIMIT 0) AS temp;
-- issue 11,947 end.

-- tables in system_metrics
USE system_metrics;
-- metric
SELECT COUNT(*) FROM (SELECT * FROM metric LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM metric LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM metric LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM metric LIMIT 10) AS temp;

-- sql_statement_total
SELECT COUNT(*) FROM (SELECT * FROM sql_statement_total LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sql_statement_total LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sql_statement_total LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sql_statement_total LIMIT 10) AS temp;

-- sql_statement_errors
SELECT COUNT(NULL) FROM (SELECT * FROM sql_statement_errors LIMIT 10) AS temp;

-- sql_transaction_total
SELECT COUNT(*) FROM (SELECT * FROM sql_transaction_total LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sql_transaction_total LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sql_transaction_total LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sql_transaction_total LIMIT 10) AS temp;

-- sql_transaction_errors
SELECT COUNT(*) FROM (SELECT * FROM sql_transaction_errors LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sql_transaction_errors LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sql_transaction_errors LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sql_transaction_errors LIMIT 10) AS temp;

-- server_connections
SELECT COUNT(*) FROM (SELECT * FROM server_connections LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM server_connections LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM server_connections LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM server_connections LIMIT 10) AS temp;

-- process_cpu_percent
SELECT COUNT(*) FROM (SELECT * FROM process_cpu_percent LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM process_cpu_percent LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM process_cpu_percent LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM process_cpu_percent LIMIT 10) AS temp;

-- process_resident_memory_bytes
SELECT COUNT(*) FROM (SELECT * FROM process_resident_memory_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM process_resident_memory_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM process_resident_memory_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM process_resident_memory_bytes LIMIT 10) AS temp;

-- sys_cpu_seconds_total
SELECT COUNT(*) FROM (SELECT * FROM sys_cpu_seconds_total LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_cpu_seconds_total LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_cpu_seconds_total LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_cpu_seconds_total LIMIT 10) AS temp;

-- sys_cpu_combined_percent
SELECT COUNT(*) FROM (SELECT * FROM sys_cpu_combined_percent LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_cpu_combined_percent LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_cpu_combined_percent LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_cpu_combined_percent LIMIT 10) AS temp;

-- sys_memory_used
SELECT COUNT(*) FROM (SELECT * FROM sys_memory_used LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_memory_used LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_memory_used LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_memory_used LIMIT 10) AS temp;

-- sys_memory_available
SELECT COUNT(*) FROM (SELECT * FROM sys_memory_available LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_memory_available LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_memory_available LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_memory_available LIMIT 10) AS temp;

-- sys_disk_read_bytes
SELECT COUNT(*) FROM (SELECT * FROM sys_disk_read_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_disk_read_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_disk_read_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_disk_read_bytes LIMIT 10) AS temp;

-- sys_disk_write_bytes
SELECT COUNT(*) FROM (SELECT * FROM sys_disk_write_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_disk_write_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_disk_write_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_disk_write_bytes LIMIT 10) AS temp;

-- sys_net_recv_bytes
SELECT COUNT(*) FROM (SELECT * FROM sys_net_recv_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_net_recv_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_net_recv_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_net_recv_bytes LIMIT 10) AS temp;

-- sys_net_sent_bytes
SELECT COUNT(*) FROM (SELECT * FROM sys_net_sent_bytes LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM sys_net_sent_bytes LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM sys_net_sent_bytes LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM sys_net_sent_bytes LIMIT 10) AS temp;

-- tables in mysql
USE mysql;
-- user
SELECT COUNT(*) FROM (SELECT * FROM user LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM user LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM user LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM user LIMIT 10) AS temp;

-- db
SELECT COUNT(*) FROM (SELECT * FROM db LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM db LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM db LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM db LIMIT 10) AS temp;

-- procs_priv
SELECT COUNT(*) FROM (SELECT * FROM procs_priv LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM procs_priv LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM procs_priv LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM procs_priv LIMIT 10) AS temp;

-- columns_priv
SELECT COUNT(*) FROM (SELECT * FROM columns_priv LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM columns_priv LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM columns_priv LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM columns_priv LIMIT 10) AS temp;

-- tables_priv
SELECT COUNT(*) FROM (SELECT * FROM tables_priv LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM tables_priv LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM tables_priv LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM tables_priv LIMIT 10) AS temp;

-- tables in information_schema
USE information_schema;
-- key_column_usage
SELECT COUNT(*) FROM (SELECT * FROM key_column_usage LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM key_column_usage LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM key_column_usage LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM key_column_usage LIMIT 10) AS temp;

-- columns
SELECT COUNT(*) FROM (SELECT * FROM columns LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM columns LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM columns LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM columns LIMIT 10) AS temp;

-- profiling
SELECT COUNT(*) FROM (SELECT * FROM profiling LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM profiling LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM profiling LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM profiling LIMIT 10) AS temp;

-- user_privileges
SELECT COUNT(*) FROM (SELECT * FROM user_privileges LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM user_privileges LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM user_privileges LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM user_privileges LIMIT 10) AS temp;

-- schemata
SELECT COUNT(*) FROM (SELECT * FROM schemata where schema_name = 'mo_catalog' or schema_name = 'mo_task' LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM schemata where schema_name = 'mo_catalog' or schema_name = 'mo_task' LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM schemata where schema_name = 'mo_catalog' or schema_name = 'mo_task' LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM schemata where schema_name = 'mo_catalog' or schema_name = 'mo_task' LIMIT 10) AS temp;

-- character_sets
SELECT COUNT(*) FROM (SELECT * FROM character_sets LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM character_sets LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM character_sets LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM character_sets LIMIT 10) AS temp;

-- triggers
SELECT COUNT(*) FROM (SELECT * FROM triggers LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM triggers LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM triggers LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM triggers LIMIT 10) AS temp;

-- tables
SELECT COUNT(*) FROM (SELECT * FROM tables LIMIT 10) AS temp;
SELECT COUNT(0) FROM (SELECT * FROM tables LIMIT 10) AS temp;
SELECT COUNT('') FROM (SELECT * FROM tables LIMIT 10) AS temp;
SELECT COUNT(NULL) FROM (SELECT * FROM tables LIMIT 10) AS temp;

-- tables in mo_catalog
USE mo_catalog;
SHOW CREATE TABLE mo_columns;
SHOW CREATE TABLE mo_database;
SHOW CREATE TABLE mo_tables;
