SELECT table_catalog,table_schema,table_name,table_type from `information_schema`.`tables` where table_name = 'mo_tables';
SELECT * FROM `information_schema`.`character_sets` LIMIT 0,1000;
SELECT * FROM `information_schema`.`columns` where TABLE_NAME = 'mo_tables' order by ORDINAL_POSITION LIMIT 2;
SELECT * FROM `information_schema`.`key_column_usage` LIMIT 0,1000;
SELECT * FROM `information_schema`.`profiling` LIMIT 0,1000;
SELECT * FROM `information_schema`.`schemata` where schema_name = 'information_schema';
SELECT * FROM `information_schema`.`triggers` LIMIT 0,1000;
SELECT * FROM `information_schema`.`user_privileges` LIMIT 0,1000;
-- keywords:collation
-- @ignore:8
SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, NULL AS INDEX_QUALIFIER, INDEX_NAME,3 AS TYPE, SEQ_IN_INDEX AS ORDINAL_POSITION, COLUMN_NAME,COLLATION AS ASC_OR_DESC, CARDINALITY, 0 AS PAGES, NULL AS FILTER_CONDITION FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = 'mysql' AND TABLE_NAME = 'procs_priv' ORDER BY NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX limit 1;
SELECT * FROM `mysql`.`columns_priv` LIMIT 0,1000;
SELECT * FROM `mysql`.`db` LIMIT 0,1000;
SELECT * FROM `mysql`.`procs_priv` LIMIT 0,1000;
SELECT * FROM `mysql`.`tables_priv` LIMIT 0,1000;
SELECT * FROM `mysql`.`user` LIMIT 0,1000;
use mysql;
show tables;
-- @ignore:0
# show columns from `user`;
-- @ignore:0
show columns from `db`;
-- @ignore:0
show columns from `procs_priv`;
-- @ignore:0
show columns from `columns_priv`;
-- @ignore:0
show columns from `tables_priv`;
use information_schema;
show tables;
-- @ignore:0
show columns from `KEY_COLUMN_USAGE`;
show columns from `COLUMNS`;
-- @ignore:0
show columns from `PROFILING`;
-- @ignore:0
show columns from `USER_PRIVILEGES`;
show columns from `SCHEMATA`;
-- @ignore:0
show columns from `CHARACTER_SETS`;
-- @ignore:0
show columns from `TRIGGERS`;
show columns from `TABLES`;
show columns from `PARTITIONS`;
drop database if exists test;
create database test;
use test;
drop table if exists t2;
create table t2(b int, a int);
desc t2;
drop table t2;
drop database test;
