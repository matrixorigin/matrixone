-- @suit
-- @case
-- @desc:test for Some System tables status, content, availability, and so on...
-- @label:bvt

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
