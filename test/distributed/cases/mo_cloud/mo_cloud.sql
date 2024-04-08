-- create account
create account if not exists test_tenant_1 admin_name 'test_account' identified by '111';

-- All of the following sql is executed under ordinary tenants
-- @session:id=2&user=test_tenant_1:test_account&password=111
CREATE DATABASE IF NOT EXISTS mo_mo;
USE mo_mo;

-- select mo_catalog.mo_tables
select count(*) from mo_catalog.mo_tables where reldatabase = 'mo_catalog' and relname = 'statement_mo' and relkind = 'cluster';
select count(*) from mo_catalog.mo_tables where reldatabase = 'mo_mo' and relname = 'stu' and relkind = 'r';
select count(*) from mo_catalog.mo_tables where reldatabase = 'mo_mo' and relname = 'statement_time' and relkind = 'r';
select count(*) from mo_catalog.mo_tables where reldatabase = 'mo_mo' and relname = 'moins' and relkind = 'r';
select count(*) from mo_catalog.mo_tables where reldatabase = 'mo_mo' and relname = 'storage' and relkind = 'r';
select count(*) from mo_catalog.mo_tables where reldatabase = 'mo_mo' and relname = 'storage_time' and relkind = 'r';

-- create table
CREATE CLUSTER TABLE `mo_catalog`.`statement_mo` (`statement_id` varchar(36) NOT NULL,`account` VARCHAR(300) NOT NULL,`response_at` datetime(3) NULL,`stu` DECIMAL(23,3) NOT NULL,PRIMARY KEY (`statement_id`, `account_id`));
CREATE TABLE `statement_time` (`cluster` varchar(191) NOT NULL,`last_time` datetime(3) NOT NULL,PRIMARY KEY (`cluster`));
CREATE TABLE `stu` (`account` VARCHAR(300) NOT NULL,`start_time` datetime(3) NOT NULL,`end_time` datetime(3) NOT NULL,`stu` decimal(23,3) NOT NULL,`type` longtext NOT NULL);
CREATE TABLE `mo_mo`.`wb_version` (`id` varchar(128),`wb_id` varchar(128),`sql_content` MEDIUMTEXT,`version` varchar(255),`status` tinyint(4),`created_at` datetime,`updated_at` datetime,PRIMARY KEY (`id`),INDEX `wb_id_idx` (`wb_id`));
CREATE TABLE `mo_mo`.`wb` (`id` varchar(128),`account_id` varchar(128),`sql_user` varchar(128),`name` varchar(128),`created_at` datetime,`updated_at` datetime,PRIMARY KEY (`id`),UNIQUE INDEX `unq_account_name` (`account_id`,`sql_user`,`name`));
CREATE TABLE `moins` (`id` varchar(128) NOT NULL,`name` VARCHAR(255) NOT NULL,`account_name` varchar(128) NOT NULL,`provider` longtext NOT NULL,`provider_id` longtext,`region` longtext NOT NULL,`pty` longtext NOT NULL,`version` longtext,`status` longtext,`qt` longtext,`policy` longtext,`created_by` longtext,`created_at` datetime(3) NULL,PRIMARY KEY (`id`));
CREATE TABLE `operation` (`id` varchar(128) NOT NULL,`created_at` datetime(3) NULL,`statement_id` varchar(128) NOT NULL,`connection_id` bigint unsigned NOT NULL,`db` varchar(128) NOT NULL,`run_status` varchar(64) NOT NULL,`err_msg` text NOT NULL,`rows_affected` bigint,PRIMARY KEY (`id`),INDEX `idx_operation_created_at` (`created_at` asc));
CREATE TABLE `storage_time` (`cluster` varchar(191) NOT NULL,`last_time` datetime(3) NOT NULL,PRIMARY KEY (`cluster`));
CREATE TABLE `storage` (`collecttime` datetime NOT NULL,`value` double NOT NULL,`account` varchar(128) NOT NULL,`interval` int NOT NULL);

-- insert sql
INSERT INTO `mo_mo`.`operation` (`id`,`created_at`,`statement_id`,`connection_id`,`db`,`run_status`,`err_msg`,`rows_affected`) VALUES ('fef17f18-d337-4f81-b5f5-3888690ac3a0','2023-07-28 05:01:57.296','',0,'','','',0);

-- select sql
SELECT * FROM (select `statement`,system.statement_info.statement_id,IF(`status`='Running', TIMESTAMPDIFF(MICROSECOND,`request_at`,now())*1000, `duration`) AS `duration`,`status`,`query_type`,`request_at`,system.statement_info.response_at,`user`,`database`,`transaction_id`,`session_id`,`rows_read`,`bytes_scan`,`result_count` from system.statement_info left join mo_catalog.statement_mo ON system.statement_info.statement_id = mo_catalog.statement_mo.statement_id where 1=1 AND `request_at` >= '2023-07-28 04:09:33' AND system.statement_info.statement_id LIKE '%cd%' AND system.statement_info.account = 'c3daefe4_2197_4192_a10e_d0acc5d9da30' AND `user` IN ('ab') AND `session_id` LIKE '%ef%' AND `duration` > 1000000000 AND `statement` LIKE '%show%' ESCAPE '\\\\' )t ORDER BY request_at DESC LIMIT 20;
SELECT * FROM `mo_mo`.`moins` WHERE id = 'c3daefe4-2197-4192-a10e-d0acc5d9da30';
SELECT * FROM `mo_mo`.`operation` WHERE `operation`.`id` = 'fef17f18-d337-4f81-b5f5-3888690ac3a0' ORDER BY `operation`.`id` LIMIT 1;
SELECT `stat_ts`, sum(`stu`) as value FROM(SELECT concat(DATE_FORMAT(date_add(`start_time`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`start_time`,Interval 1 MINUTE)) / 1) as int),2,0),':00') AS stat_ts, SUM(`stu`) AS stu FROM mo_mo.stu WHERE `start_time` >= '2023-07-28 03:04:53' AND `start_time` <= '2023-07-28 03:34:53' AND account = '863f0767_0361_4a8c_a3e8_73b2be140a74' GROUP BY concat(DATE_FORMAT(date_add(`start_time`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`start_time`,Interval 1 MINUTE)) / 1) as int),2,0),':00') ORDER BY concat(DATE_FORMAT(date_add(`start_time`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`start_time`,Interval 1 MINUTE)) / 1) as int),2,0),':00') LIMIT 100)t GROUP BY `stat_ts`;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='information_schema' and att_relname='engines' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='information_schema' and att_relname='key_column_usage' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='information_schema' and att_relname='keywords' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='information_schema' and att_relname='parameters' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='mo_catalog' and att_relname='mo_columns' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='mo_sample_data_tpch_sf1' and att_relname='lineitem' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='mo_sample_data_tpch_sf1' and att_relname='supplier' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='mysql' and att_relname='columns_priv' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='mysql' and att_relname='db' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='mysql' and att_relname='procs_priv' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='mysql' and att_relname='tables_priv' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='mysql' and att_relname='user' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT attname AS name, mo_show_visible_bin(atttyp,3) AS data_type, replace(mo_table_col_max(att_database,att_relname,attname),'\\0', '') AS `maximum`,  mo_table_col_min(att_database,att_relname,attname) as minimum from mo_catalog.mo_columns where att_database='system_metrics' and att_relname='metric' and attname NOT IN  ('__mo_rowid', '__mo_cpkey_col', '__mo_fake_pk_col') ORDER BY attnum;
SELECT concat(DATE_FORMAT(date_add(`request_at`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`request_at`,Interval 1 MINUTE)) / 1) as int),2,0),':00') AS stat_ts, AVG(duration) AS value, `query_type` AS type FROM `system`.`statement_info` WHERE request_at >= '2023-07-28 04:20:43' AND request_at <= '2023-07-28 04:50:43' AND `query_type` in ('DQL','DDL','DML','DCL','TCL','Other') GROUP BY `query_type`, concat(DATE_FORMAT(date_add(`request_at`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`request_at`,Interval 1 MINUTE)) / 1) as int),2,0),':00') ORDER BY concat(DATE_FORMAT(date_add(`request_at`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`request_at`,Interval 1 MINUTE)) / 1) as int),2,0),':00')LIMIT 100;
SELECT count(*) FROM (select `statement`,system.statement_info.statement_id,IF(`status`='Running', TIMESTAMPDIFF(MICROSECOND,`request_at`,now())*1000, `duration`) AS `duration`,`status`,`query_type`,`request_at`,system.statement_info.response_at,`user`,`database`,`transaction_id`,`session_id`,`rows_read`,`bytes_scan`,`result_count` from system.statement_info left join mo_catalog.statement_mo ON system.statement_info.statement_id = mo_catalog.statement_mo.statement_id where 1=1 AND `request_at` >= '2023-07-28 04:09:33' AND system.statement_info.statement_id LIKE '%cd%' AND system.statement_info.account = 'c3daefe4_2197_4192_a10e_d0acc5d9da30' AND `user` IN ('ab') AND `session_id` LIKE '%ef%' AND `duration` > 1000000000 AND `statement` LIKE '%show%' ESCAPE '\\\\' )t;
SELECT count(*) FROM `mo_catalog`.`mo_database`;
SELECT count(*) FROM `system`.`statement_info` WHERE 1=1 AND `request_at` >= '2023-07-28 03:03:28' AND system.statement_info.account = '91731e77_49ea_4a8a_b1c7_d5512c7ae96e' AND sql_source_type IN ('ab','external_sql') ;

-- show sql
SHOW CREATE TABLE information_schema.engines;
SHOW CREATE TABLE information_schema.key_column_usage;
SHOW CREATE TABLE information_schema.keywords;
SHOW CREATE TABLE information_schema.parameters;
SHOW CREATE TABLE mo_catalog.mo_columns;
SHOW CREATE TABLE mo_sample_data_tpch_sf1.lineitem;
SHOW CREATE TABLE mo_sample_data_tpch_sf1.supplier;
SHOW CREATE TABLE mysql.columns_priv;
SHOW CREATE TABLE mysql.db;
SHOW CREATE TABLE mysql.procs_priv;
SHOW CREATE TABLE mysql.tables_priv;
SHOW CREATE TABLE mysql.user;
SHOW CREATE TABLE system_metrics.metric;
SHOW CREATE TABLE system_metrics.server_connections;
SHOW CREATE TABLE system_metrics.server_storage_usage;
SHOW CREATE TABLE system_metrics.sql_statement_errors;
SHOW CREATE TABLE system_metrics.sql_transaction_total;
SHOW GLOBAL VARIABLES LIKE 'save_query_result';
SHOW SUBSCRIPTIONS;
SHOW TABLES FROM mo_sample_data_tpch_sf1;

-- update sql
UPDATE `mo_mo`.`operation` SET `created_at`='2023-07-28 05:00:13.704',`statement_id`='a3055a4e-2d03-11ee-9e81-96ce077ba0f3',`connection_id`=11349,`db`='mo_sample_data_tpch_sf10',`run_status`='success' WHERE `id` = '3b30a3c1-4dca-4ded-9576-51a99e47b53a';


-- drop table
drop table if exists `mo_catalog`.`statement_mo`;
drop table if exists `statement_time`;
drop table if exists `stu`;
drop table if exists `mo_mo`.`wb_version`;
drop table if exists `mo_mo`.`wb`;
drop table if exists `moins`;
drop table if exists `operation`;
drop table if exists `storage_time`;
drop table if exists `storage`;

-- select sql
USE system_metrics;
SELECT `stat_ts`, SUM(`value`) as value, `type` FROM(SELECT concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') AS stat_ts, AVG(`value`)/15 AS value, `node`, `type` FROM sql_statement_total WHERE `collecttime` >= '2023-07-28 05:37:43' AND `collecttime` <= '2023-07-28 06:07:43' AND `type` in ('DQL','DDL','DML','DCL','TCL','Other') GROUP BY `node`,`type`, concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00')  ORDER BY concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') LIMIT 100)t GROUP BY `stat_ts`,`type`;
USE system_metrics;
SELECT `stat_ts`, SUM(`value`) as value, `type` FROM(SELECT concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') AS stat_ts, SUM(`value`) AS value, `node`, `type` FROM sql_statement_errors WHERE `collecttime` >= '2023-07-28 05:37:43' AND `collecttime` <= '2023-07-28 06:07:43' AND `type` in ('DQL','DDL','DML','DCL','TCL','Other') GROUP BY `node`,`type`, concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00')  ORDER BY concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') LIMIT 100)t GROUP BY `stat_ts`,`type`;
USE system_metrics;
SELECT `stat_ts`, SUM(`value`) as value, `type` FROM(SELECT concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') AS stat_ts, SUM(`value`) AS value, `node`, `type` FROM sql_statement_total WHERE `collecttime` >= '2023-07-28 05:37:43' AND `collecttime` <= '2023-07-28 06:07:43' AND `type` in ('DQL','DDL','DML','DCL','TCL','Other') GROUP BY `node`,`type`, concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00')  ORDER BY concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') LIMIT 100)t GROUP BY `stat_ts`,`type`;
USE system_metrics;
SELECT `stat_ts`, sum(`value`) as value FROM(SELECT concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') AS stat_ts, AVG(`value`) AS value, `node` FROM server_connections WHERE `collecttime` >= '2023-07-28 03:23:31' AND `collecttime` <= '2023-07-28 03:53:31' AND account = '2ef38bf3_b821_4cab_879e_3b788f1e922f'GROUP BY `node`, concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00')  ORDER BY concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') LIMIT 100)t GROUP BY `stat_ts`;
USE system_metrics;
SELECT `stat_ts`, sum(`value`) as value FROM(SELECT concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') AS stat_ts, AVG(`value`) AS value, `node` FROM server_storage_usage WHERE `collecttime` >= '2023-07-28 05:36:55' AND `collecttime` <= '2023-07-28 06:06:55' AND account = 'c3daefe4_2197_4192_a10e_d0acc5d9da30'GROUP BY `node`, concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00')  ORDER BY concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') LIMIT 100)t GROUP BY `stat_ts`;
USE system_metrics;
SELECT `stat_ts`, sum(`value`) as value FROM(SELECT concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') AS stat_ts, SUM(`value`) AS value, `node` FROM sql_transaction_errors WHERE `collecttime` >= '2023-07-28 05:37:43' AND `collecttime` <= '2023-07-28 06:07:43' GROUP BY `node`, concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00')  ORDER BY concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') LIMIT 100)t GROUP BY `stat_ts`;
USE system_metrics;
SELECT `stat_ts`, sum(`value`) as value FROM(SELECT concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') AS stat_ts, SUM(`value`) AS value, `node` FROM sql_transaction_total WHERE `collecttime` >= '2023-07-28 05:37:43' AND `collecttime` <= '2023-07-28 06:07:43' GROUP BY `node`, concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00')  ORDER BY concat(DATE_FORMAT(date_add(`collecttime`,Interval 1 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(1 * floor(minute(date_add(`collecttime`,Interval 1 MINUTE)) / 1) as int),2,0),':00') LIMIT 100)t GROUP BY `stat_ts`;

select count(*) from mo_catalog.mo_tables where reldatabase = 'mo_mo' and relname = 'operation' and relkind = 'r';
select count(*) from mo_catalog.mo_tables where reldatabase = 'mo_mo' and relname = 'wb' and relkind = 'r';

-- desc sql
DESC `information_schema`.`character_sets` ;
DESC `information_schema`.`engines` ;
DESC `information_schema`.`key_column_usage` ;
DESC `information_schema`.`keywords` ;
DESC `information_schema`.`parameters` ;
DESC `mo_catalog`.`mo_columns` ;
DESC `mo_sample_data_tpch_sf10`.`region` ;
DESC `mo_sample_data_tpch_sf1`.`customer` ;
DESC `mo_sample_data_tpch_sf1`.`lineitem` ;
DESC `mo_sample_data_tpch_sf1`.`nation` ;
DESC `mo_sample_data_tpch_sf1`.`orders` ;
DESC `mo_sample_data_tpch_sf1`.`supplier` ;
DESC `mysql`.`columns_priv` ;
DESC `mysql`.`db` ;
DESC `mysql`.`procs_priv` ;
DESC `mysql`.`tables_priv` ;
DESC `mysql`.`user` ;
DESC `system_metrics`.`metric` ;
DESC `system_metrics`.`server_connections` ;
DESC `system_metrics`.`server_storage_usage` ;
DESC `system_metrics`.`sql_statement_errors` ;
DESC `system_metrics`.`sql_transaction_total` ;

-- insert sql
INSERT INTO `mo_catalog`.`statement_mo` (`statement_id`,`account`,`response_at`,`stu`,`account_id`) VALUES ('057247c5-2d0c-11ee-9e81-96ce077ba0f3','91731e77_49ea_4a8a_b1c7_d5512c7ae96e','2023-07-28 06:00:20','0.0003963262009632',2);
INSERT INTO `mo_mo`.`statement_time` (`cluster`,`last_time`) VALUES ('ab','2023-07-28 04:50:00');
INSERT INTO `mo_mo`.`stu` (`account`,`start_time`,`end_time`,`stu`,`type`) VALUES ('2ef38bf3_b821_4cab_879e_3b788f1e922f','2023-07-28 07:19:00','2023-07-28 07:20:00','0.0002629868102331','ab');
INSERT INTO `mo_mo`.`stu` (`account`,`start_time`,`end_time`,`stu`,`type`) VALUES ('2ef38bf3_b821_4cab_879e_3b788f1e922f','2023-07-28 07:52:00','2023-07-28 07:53:00','0.0056437809323943','cd');
INSERT INTO `mo_mo`.`moins` (`id`,`name`,`account_name`,`provider`,`provider_id`,`region`,`pty`,`version`,`status`,`qt`,`policy`,`created_by`,`created_at`) VALUES ('c3daefe4-2197-4192-a10e-d0acc5d9da30','free0','c3daefe4_2197_4192_a10e_d0acc5d9da30','ab','1527','cd','ef','nightly-2257011e','','{\stu\:1000,\storage\:53687}','{\allow_list\:[\0.0.0.0/0\]}','154b8231-cc1d-4629-88c6-d007cf4f4af8','2023-07-28 06:06:03.608');
INSERT INTO `mo_mo`.`storage_time` (`cluster`,`last_time`) VALUES ('ab','2023-07-28 03:30:00');
INSERT INTO `mo_mo`.`storage` (`collecttime`,`value`,`account`,`interval`) VALUES ('2023-07-28 03:30:00',0.075000,'301b1fe7_0d51_404e_9e18_33fe2f1d29b2',10);
INSERT INTO `mo_mo`.`wb_version` (`id`,`wb_id`,`sql_content`,`version`,`status`,`created_at`,`updated_at`) VALUES ('6c1eede6-70d9-4185-82fe-f5341777d27d','ba8f2a64-3396-465c-a770-77600865d6c2','create database hx;','2023-07-28T05:01:41Z',1,'2023-07-28 05:01:41.047','2023-07-28 05:01:41.047');
INSERT INTO `mo_mo`.`wb` (`id`,`account_id`,`sql_user`,`name`,`created_at`,`updated_at`) VALUES ('30415473-5276-42c5-8302-4e9a7e605900','91731e77_49ea_4a8a_b1c7_d5512c7ae96e','admin','New sql wb 2','2023-07-28 06:21:56.89','2023-07-28 06:21:56.89');

-- select sql
SELECT * FROM `mo_catalog`.`mo_account` WHERE `mo_account`.`account_name` = 'c3daefe4_2197_4192_a10e_d0acc5d9da30' ORDER BY `mo_account`.`account_name` LIMIT 1;
SELECT * FROM `mo_mo`.`statement_time` WHERE cluster = 'ab' LIMIT 1;
SELECT * FROM `mo_mo`.`storage_time` WHERE cluster = 'ab' LIMIT 1;
SELECT * FROM `mo_mo`.`wb_version` WHERE wb_id = '9579cf34-2faf-4b9b-83dc-09638c39265a' AND id = '06945b61-372b-4289-964b-0a6b098e2c4e' ORDER BY updated_at DESC;
SELECT * FROM `mo_mo`.`wb` WHERE 1=1 AND account_id = '91731e77_49ea_4a8a_b1c7_d5512c7ae96e' AND sql_user = 'admin' AND name LIKE '%New sql wb %' ORDER BY updated_at DESC LIMIT 100;
SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE 'mo_mo%' ORDER BY SCHEMA_NAME='mo_mo' DESC,SCHEMA_NAME limit 1;
SELECT `collecttime`,`value`,`account` FROM `system_metrics`.`server_storage_usage` WHERE account != 'sys' and account != 'ob' and collectTime >= '2023-07-28 03:23:00' and collectTime < '2023-07-28 03:30:00';
SELECT `statement_id`,`stats`,`duration`,`sql_source_type`,`account`,`response_at` FROM `system`.`statement_info` WHERE status != 'Running' and account != 'sys' and account != 'ob' and response_at >= '2023-07-28 04:49:00' and response_at < '2023-07-28 04:50:00';
SELECT `value` FROM `system_metrics`.`server_storage_usage` WHERE account = '863f0767_0361_4a8c_a3e8_73b2be140a74' and collecttime > '2023-07-28 06:07:53' and collecttime <= '2023-07-28 06:10:53' ORDER BY collecttime DESC LIMIT 1;
SELECT count(*) FROM `mo_mo`.`moins` WHERE id = '2ef38bf3-b821-4cab-879e-3b788f1e922f';
SELECT count(*) FROM `mo_mo`.`wb_version` WHERE wb_id = 'd457fd59-a865-4a5b-80b0-6e19a6059f1f';
SELECT count(*) FROM `mo_mo`.`wb` WHERE account_id = '91731e77_49ea_4a8a_b1c7_d5512c7ae96e' AND sql_user = 'admin' AND id = '30415473-5276-42c5-8302-4e9a7e605900';
SELECT count(*) FROM information_schema.tables WHERE table_schema = '' AND table_name = 'moins' AND table_type = 'BASE TABLE';
SELECT datname AS name, IF (table_cnt IS NULL, 0, table_cnt) AS tables, role_name AS owner FROM (SELECT dat_id, datname, mo_database.created_time, IF(role_name IS NULL, '-', role_name) AS role_name FROM mo_catalog.mo_database LEFT JOIN mo_catalog.mo_role ON mo_database.owner = role_id) AS x LEFT JOIN(SELECT count(*) AS table_cnt, reldatabase_id FROM mo_catalog.mo_tables WHERE relkind IN ('r','v','e','cluster') GROUP BY reldatabase_id) AS y ON x.dat_id = y.reldatabase_id order by name;
SELECT mo_catalog.mo_database.datname,mo_catalog.mo_tables.relname,mo_catalog.mo_tables.relkind, if (role_name IS NULL,'-', role_name) AS `owner` FROM mo_catalog.mo_database LEFT JOIN mo_catalog.mo_tables ON mo_catalog.mo_database.datname = mo_catalog.mo_tables.reldatabase LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE (relname NOT LIKE "__mo_index_secondary_%" AND relname NOT LIKE "__mo_index_unique_%" OR relname IS NULL) ORDER BY reldatabase, relname;
SELECT relname AS `name`, IF (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('v') AND reldatabase='information_schema';
SELECT relname AS `name`, IF (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('v') AND reldatabase='mo_sample_data_tpch_sf1';
SELECT relname AS `name`, IF (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('v') AND reldatabase='mysql';
SELECT relname AS `name`, IF (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('v') AND reldatabase='system';
SELECT relname AS `name`, IF (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('v') AND reldatabase='system_metrics';
SELECT relname AS `name`, mo_table_rows(reldatabase, relname) AS `rows`, mo_table_size(reldatabase, relname) AS `size`, if (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('r','e','cluster') AND reldatabase='information_schema';
SELECT relname AS `name`, mo_table_rows(reldatabase, relname) AS `rows`, mo_table_size(reldatabase, relname) AS `size`, if (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('r','e','cluster') AND reldatabase='mo_sample_data_tpch_sf1';
SELECT relname AS `name`, mo_table_rows(reldatabase, relname) AS `rows`, mo_table_size(reldatabase, relname) AS `size`, if (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('r','e','cluster') AND reldatabase='mysql';
SELECT relname AS `name`, mo_table_rows(reldatabase, relname) AS `rows`, mo_table_size(reldatabase, relname) AS `size`, if (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('r','e','cluster') AND reldatabase='system';
SELECT relname AS `name`, mo_table_rows(reldatabase, relname) AS `rows`, mo_table_size(reldatabase, relname) AS `size`, if (role_name IS NULL, '-', role_name) AS `owner` FROM mo_catalog.mo_tables LEFT JOIN mo_catalog.mo_role ON mo_catalog.mo_tables.owner=role_id WHERE relkind IN ('r','e','cluster') AND reldatabase='system_metrics' order by name;
SELECT count(*) FROM `system`.`statement_info` WHERE status != 'Running' and account != 'sys' and account != 'ob' ORDER BY response_at asc LIMIT 1;
SELECT role_name AS owner FROM (SELECT dat_id, datname, mo_database.created_time, IF(role_name IS NULL, '-', role_name) AS role_name FROM mo_catalog.mo_database LEFT JOIN mo_catalog.mo_role ON mo_database.owner = role_id) AS x WHERE datname='information_schema';
SELECT role_name AS owner FROM (SELECT dat_id, datname, mo_database.created_time, IF(role_name IS NULL, '-', role_name) AS role_name FROM mo_catalog.mo_database LEFT JOIN mo_catalog.mo_role ON mo_database.owner = role_id) AS x WHERE datname='mo_sample_data_tpch_sf1';
SELECT role_name AS owner FROM (SELECT dat_id, datname, mo_database.created_time, IF(role_name IS NULL, '-', role_name) AS role_name FROM mo_catalog.mo_database LEFT JOIN mo_catalog.mo_role ON mo_database.owner = role_id) AS x WHERE datname='mysql';
SELECT role_name AS owner FROM (SELECT dat_id, datname, mo_database.created_time, IF(role_name IS NULL, '-', role_name) AS role_name FROM mo_catalog.mo_database LEFT JOIN mo_catalog.mo_role ON mo_database.owner = role_id) AS x WHERE datname='system';
SELECT role_name AS owner FROM (SELECT dat_id, datname, mo_database.created_time, IF(role_name IS NULL, '-', role_name) AS role_name FROM mo_catalog.mo_database LEFT JOIN mo_catalog.mo_role ON mo_database.owner = role_id) AS x WHERE datname='system_metrics';
SELECT sum(`value`*`interval`) FROM `mo_mo`.`storage` WHERE collecttime > '2023-07-28 00:00:00' AND collecttime <= '2023-07-28 03:54:39' AND account = '91731e77_49ea_4a8a_b1c7_d5512c7ae96e';

-- update sql
UPDATE `mo_mo`.`statement_time` SET `last_time`='2023-07-28 09:00:00' WHERE `cluster` = 'ab';
UPDATE `mo_mo`.`storage_time` SET `last_time`='2023-07-28 03:30:00' WHERE `cluster` = 'ab';
UPDATE `mo_mo`.`wb_version` SET `status`=2,`version`='2023-07-28T05:00:35Z',`updated_at`='2023-07-28 05:00:35.752' WHERE wb_id = 'ba8f2a64-3396-465c-a770-77600865d6c2' AND id = '7f660329-a51b-4b17-b981-fc9acd9b492a';
UPDATE `mo_mo`.`wb` SET `updated_at`='2023-07-28 06:02:41.452' WHERE id = '9579cf34-2faf-4b9b-83dc-09638c39265a';

alter account `2ef38bf3_b821_4cab_879e_3b788f1e922f` admin_name 'admin' identified by 'abcd';
delete from mo_mo.operation where created_at <= '2023-07-21 13:42:02';
select distinct IF(relkind = 'v', 0, mo_table_rows('information_schema','character_sets')) as `rows` from mo_catalog.mo_tables where reldatabase='information_schema' and relname='character_sets';
select distinct IF(relkind = 'v', 0, mo_table_rows('mo_sample_data_tpch_sf1','customer')) as `rows` from mo_catalog.mo_tables where reldatabase='mo_sample_data_tpch_sf1' and relname='customer';
select distinct IF(relkind = 'v', 0, mo_table_rows('mo_sample_data_tpch_sf1','lineitem')) as `rows` from mo_catalog.mo_tables where reldatabase='mo_sample_data_tpch_sf1' and relname='lineitem';
select distinct IF(relkind = 'v', 0, mo_table_rows('mo_sample_data_tpch_sf1','nation')) as `rows` from mo_catalog.mo_tables where reldatabase='mo_sample_data_tpch_sf1' and relname='nation';
select distinct IF(relkind = 'v', 0, mo_table_rows('mo_sample_data_tpch_sf1','orders')) as `rows` from mo_catalog.mo_tables where reldatabase='mo_sample_data_tpch_sf1' and relname='orders';
select distinct IF(relkind = 'v', 0, mo_table_rows('mo_sample_data_tpch_sf1','supplier')) as `rows` from mo_catalog.mo_tables where reldatabase='mo_sample_data_tpch_sf1' and relname='supplier';
select distinct IF(relkind = 'v', 0, mo_table_rows('mo_sample_data_tpch_sf10','region')) as `rows` from mo_catalog.mo_tables where reldatabase='mo_sample_data_tpch_sf10' and relname='region';
select sum(stu) from mo_mo.stu where account = '863f0767_0361_4a8c_a3e8_73b2be140a74' and start_time >= '2023-07-28 03:34:59' and start_time <= '2023-07-28 06:10:52';

-- drop database
drop database if exists mo_mo;
-- @session

-- drop account
drop account test_tenant_1;
