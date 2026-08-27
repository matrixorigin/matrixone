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

-- table_constraints
SELECT COUNT(*) FROM table_constraints;

-- tables in mo_catalog
USE mo_catalog;
SHOW CREATE TABLE mo_columns;
SHOW CREATE TABLE mo_database;
SHOW CREATE TABLE mo_tables;

-- issue #27661: mo_columns must preserve unsigned integer metadata
drop database if exists issue_27661;
create database issue_27661;
use issue_27661;
create table unsigned_flags (signed_tiny tinyint, unsigned_tiny tinyint unsigned, signed_small smallint, unsigned_small smallint unsigned, signed_int int, unsigned_int int unsigned, signed_big bigint, unsigned_big bigint unsigned, bit_col bit(8), decimal_col decimal(10,2));
select attname, att_is_unsigned, mo_show_visible_bin(atttyp, 2) as data_type from mo_catalog.mo_columns where att_database = 'issue_27661' and att_relname = 'unsigned_flags' and att_is_hidden = 0 order by attnum;
alter table unsigned_flags add column unsigned_added bigint unsigned;
alter table unsigned_flags modify column signed_int int unsigned;
select attname, att_is_unsigned, mo_show_visible_bin(atttyp, 2) as data_type from mo_catalog.mo_columns where att_database = 'issue_27661' and att_relname = 'unsigned_flags' and att_is_hidden = 0 order by attnum;
drop database issue_27661;
