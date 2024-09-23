// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sysview

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
)

// `mysql` database system tables
// They are all Tenant level system tables
var (
	MysqlUserDDL = `CREATE TABLE mysql.user (
			Host char(255)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Select_priv varchar(10) NOT NULL DEFAULT 'N',
			Insert_priv varchar(10) NOT NULL DEFAULT 'N',
			Update_priv varchar(10) NOT NULL DEFAULT 'N',
			Delete_priv varchar(10) NOT NULL DEFAULT 'N',
			Create_priv varchar(10) NOT NULL DEFAULT 'N',
			Drop_priv varchar(10)  NOT NULL DEFAULT 'N',
			Reload_priv varchar(10)  NOT NULL DEFAULT 'N',
			Shutdown_priv varchar(10)  NOT NULL DEFAULT 'N',
			Process_priv varchar(10)  NOT NULL DEFAULT 'N',
			File_priv varchar(10)  NOT NULL DEFAULT 'N',
			Grant_priv varchar(10)  NOT NULL DEFAULT 'N',
			References_priv varchar(10)  NOT NULL DEFAULT 'N',
			Index_priv varchar(10)  NOT NULL DEFAULT 'N',
			Alter_priv varchar(10)  NOT NULL DEFAULT 'N',
			Show_db_priv varchar(10)  NOT NULL DEFAULT 'N',
			Super_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_tmp_table_priv varchar(10)  NOT NULL DEFAULT 'N',
			Lock_tables_priv varchar(10)  NOT NULL DEFAULT 'N',
			Execute_priv varchar(10)  NOT NULL DEFAULT 'N',
			Repl_slave_priv varchar(10)  NOT NULL DEFAULT 'N',
			Repl_client_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_view_priv varchar(10)  NOT NULL DEFAULT 'N',
			Show_view_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_routine_priv varchar(10)  NOT NULL DEFAULT 'N',
			Alter_routine_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_user_priv varchar(10)  NOT NULL DEFAULT 'N',
			Event_priv varchar(10)  NOT NULL DEFAULT 'N',
			Trigger_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_tablespace_priv varchar(10)  NOT NULL DEFAULT 'N',
			ssl_type varchar(10)  NOT NULL DEFAULT '',
			ssl_cipher blob NOT NULL,
			x509_issuer blob NOT NULL,
			x509_subject blob NOT NULL,
			max_questions int unsigned NOT NULL DEFAULT '0',
			max_updates int unsigned NOT NULL DEFAULT '0',
			max_connections int unsigned NOT NULL DEFAULT '0',
			max_user_connections int unsigned NOT NULL DEFAULT '0',
			plugin char(64)  NOT NULL DEFAULT 'caching_sha2_password',
			authentication_string text ,
			password_expired varchar(10)  NOT NULL DEFAULT 'N',
			password_last_changed timestamp NULL DEFAULT NULL,
			password_lifetime smallint unsigned DEFAULT NULL,
			account_locked varchar(10)  NOT NULL DEFAULT 'N',
			Create_role_priv varchar(10)  NOT NULL DEFAULT 'N',
			Drop_role_priv varchar(10)  NOT NULL DEFAULT 'N',
			Password_reuse_history smallint unsigned DEFAULT NULL,
			Password_reuse_time smallint unsigned DEFAULT NULL,
			Password_require_current varchar(10)  DEFAULT NULL,
			User_attributes json DEFAULT NULL,
			PRIMARY KEY (Host,User)
		  )`

	MysqlDbDDL = `CREATE TABLE mysql.db (
			Host char(255) NOT NULL DEFAULT '',
			Db char(64)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Select_priv varchar(10)  NOT NULL DEFAULT 'N',
			Insert_priv varchar(10)  NOT NULL DEFAULT 'N',
			Update_priv varchar(10)  NOT NULL DEFAULT 'N',
			Delete_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_priv varchar(10)  NOT NULL DEFAULT 'N',
			Drop_priv varchar(10)  NOT NULL DEFAULT 'N',
			Grant_priv varchar(10)  NOT NULL DEFAULT 'N',
			References_priv varchar(10)  NOT NULL DEFAULT 'N',
			Index_priv varchar(10)  NOT NULL DEFAULT 'N',
			Alter_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_tmp_table_priv varchar(10)  NOT NULL DEFAULT 'N',
			Lock_tables_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_view_priv varchar(10)  NOT NULL DEFAULT 'N',
			Show_view_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_routine_priv varchar(10)  NOT NULL DEFAULT 'N',
			Alter_routine_priv varchar(10)  NOT NULL DEFAULT 'N',
			Execute_priv varchar(10)  NOT NULL DEFAULT 'N',
			Event_priv varchar(10)  NOT NULL DEFAULT 'N',
			Trigger_priv varchar(10)  NOT NULL DEFAULT 'N',
			PRIMARY KEY (Host,Db,User),
			KEY User (User)
		  )`

	MysqlProcsPrivDDL = `CREATE TABLE mysql.procs_priv (
			Host char(255)  NOT NULL DEFAULT '',
			Db char(64)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Routine_name char(64)  NOT NULL DEFAULT '',
			Routine_type varchar(10)  NOT NULL,
			Grantor varchar(288)  NOT NULL DEFAULT '',
			Proc_priv varchar(10)  NOT NULL DEFAULT '',
			Timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (Host,Db,User,Routine_name,Routine_type),
			KEY Grantor (Grantor)
		  )`

	MysqlColumnsPrivDDL = `CREATE TABLE mysql.columns_priv (
			Host char(255)  NOT NULL DEFAULT '',
			Db char(64)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Table_name char(64)  NOT NULL DEFAULT '',
			Column_name char(64)  NOT NULL DEFAULT '',
			Timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			Column_priv varchar(10) NOT NULL DEFAULT '',
			PRIMARY KEY (Host,Db,User,Table_name,Column_name)
		  )`

	MysqlTablesPrivDDL = `CREATE TABLE mysql.tables_priv (
			Host char(255)  NOT NULL DEFAULT '',
			Db char(64)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Table_name char(64)  NOT NULL DEFAULT '',
			Grantor varchar(288)  NOT NULL DEFAULT '',
			Timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			Table_priv varchar(10) NOT NULL DEFAULT '',
			Column_priv varchar(10) NOT NULL DEFAULT '',
			PRIMARY KEY (Host,Db,User,Table_name),
			KEY Grantor (Grantor)
		  )`

	MysqlRoleEdgesDDL = `CREATE TABLE mysql.role_edges (
			FROM_HOST char(255) NOT NULL DEFAULT '',
			FROM_USER char(32) NOT NULL DEFAULT '',
			TO_HOST char(255) NOT NULL DEFAULT '',
			TO_USER char(32) NOT NULL DEFAULT '',
			WITH_ADMIN_OPTION enum('N','Y') NOT NULL DEFAULT 'N',
			PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
		)`
)

// `information_schema` database
// They are all Tenant level system tables/system views
var (
	InformationSchemaKeyColumnUsageDDL = "CREATE TABLE information_schema.KEY_COLUMN_USAGE (" +
		"CONSTRAINT_CATALOG varchar(64)," +
		"CONSTRAINT_SCHEMA varchar(64)," +
		"CONSTRAINT_NAME varchar(64)," +
		"TABLE_CATALOG varchar(64)," +
		"TABLE_SCHEMA varchar(64)," +
		"TABLE_NAME varchar(64)," +
		"COLUMN_NAME varchar(64)," +
		"ORDINAL_POSITION int unsigned," +
		"POSITION_IN_UNIQUE_CONSTRAINT int unsigned," +
		"REFERENCED_TABLE_SCHEMA varchar(64)," +
		"REFERENCED_TABLE_NAME varchar(64)," +
		"REFERENCED_COLUMN_NAME varchar(64)" +
		")"

	InformationSchemaColumnsDDL = fmt.Sprintf("CREATE VIEW information_schema.COLUMNS AS select "+
		"'def' as TABLE_CATALOG,"+
		"mc.att_database as TABLE_SCHEMA,"+
		"mc.att_relname AS TABLE_NAME,"+
		"mc.attname AS COLUMN_NAME,"+
		"mc.attnum AS ORDINAL_POSITION,"+
		"mo_show_visible_bin(mc.att_default,1) as COLUMN_DEFAULT,"+
		"(case when mc.attnotnull != 0 then 'NO' else 'YES' end) as IS_NULLABLE,"+
		"mo_show_visible_bin(mc.atttyp,2) as DATA_TYPE,"+
		"internal_char_length(mc.atttyp) AS CHARACTER_MAXIMUM_LENGTH,"+
		"internal_char_size(mc.atttyp) AS CHARACTER_OCTET_LENGTH,"+
		"internal_numeric_precision(mc.atttyp) AS NUMERIC_PRECISION,"+
		"internal_numeric_scale(mc.atttyp) AS NUMERIC_SCALE,"+
		"internal_datetime_scale(mc.atttyp) AS DATETIME_PRECISION,"+
		"(case internal_column_character_set(mc.atttyp) WHEN 0 then 'utf8' WHEN 1 then 'utf8' else NULL end) AS CHARACTER_SET_NAME,"+
		"(case internal_column_character_set(mc.atttyp) WHEN 0 then 'utf8_bin' WHEN 1 then 'utf8_bin' else NULL end) AS COLLATION_NAME,"+
		"mo_show_visible_bin(mc.atttyp,3) as COLUMN_TYPE,"+
		"case when mc.att_constraint_type = 'p' then 'PRI' when mo_show_col_unique(mt.`constraint`, mc.attname) then 'UNI' else '' end as COLUMN_KEY,"+
		"case when mc.att_is_auto_increment = 1 then 'auto_increment' else '' end as EXTRA,"+
		"'select,insert,update,references' as `PRIVILEGES`,"+
		"mc.att_comment as COLUMN_COMMENT,"+
		"cast('' as varchar(500)) as GENERATION_EXPRESSION,"+
		"if(true, NULL, 0) as SRS_ID "+
		"from mo_catalog.mo_columns mc join mo_catalog.mo_tables mt ON mc.account_id = mt.account_id AND mc.att_database = mt.reldatabase AND mc.att_relname = mt.relname "+
		"where mc.account_id = current_account_id() "+
		"and mc.att_relname!='%s' and mc.att_relname not like '%s' and mc.attname != '%s' and mc.att_relname not like '%s'",
		catalog.MOAutoIncrTable, catalog.PrefixPriColName+"%", catalog.Row_ID, catalog.PartitionSubTableWildcard)

	InformationSchemaProfilingDDL = "CREATE TABLE information_schema.PROFILING (" +
		"QUERY_ID int NOT NULL DEFAULT '0'," +
		"SEQ int NOT NULL DEFAULT '0'," +
		"STATE varchar(30) NOT NULL DEFAULT ''," +
		"DURATION decimal(9,6) NOT NULL DEFAULT '0.000000'," +
		"CPU_USER decimal(9,6) DEFAULT NULL," +
		"CPU_SYSTEM decimal(9,6) DEFAULT NULL," +
		"CONTEXT_VOLUNTARY int DEFAULT NULL," +
		"CONTEXT_INVOLUNTARY int DEFAULT NULL," +
		"BLOCK_OPS_IN int DEFAULT NULL," +
		"BLOCK_OPS_OUT int DEFAULT NULL," +
		"MESSAGES_SENT int DEFAULT NULL," +
		"MESSAGES_RECEIVED int DEFAULT NULL," +
		"PAGE_FAULTS_MAJOR int DEFAULT NULL," +
		"PAGE_FAULTS_MINOR int DEFAULT NULL," +
		"SWAPS int DEFAULT NULL," +
		"SOURCE_FUNCTION varchar(30) DEFAULT NULL," +
		"SOURCE_FILE varchar(20) DEFAULT NULL," +
		"SOURCE_LINE int DEFAULT NULL" +
		")"

	InformationSchemaProcesslistDDL = fmt.Sprintf("CREATE VIEW %s.PROCESSLIST AS "+
		"select node_id, conn_id, session_id, account, user, host, db, "+
		"session_start, command, info, txn_id, statement_id, statement_type, "+
		"query_type, sql_source_type, query_start, client_host, role, proxy_host "+
		"from PROCESSLIST() A", InformationDBConst)

	InformationSchemaUserPrivilegesDDL = "CREATE TABLE information_schema.USER_PRIVILEGES (" +
		"GRANTEE varchar(292) NOT NULL DEFAULT ''," +
		"TABLE_CATALOG varchar(512) NOT NULL DEFAULT ''," +
		"PRIVILEGE_TYPE varchar(64) NOT NULL DEFAULT ''," +
		"IS_GRANTABLE varchar(3) NOT NULL DEFAULT ''" +
		")"

	InformationSchemaSchemataDDL = "CREATE VIEW information_schema.SCHEMATA AS SELECT " +
		"dat_catalog_name AS CATALOG_NAME," +
		"datname AS SCHEMA_NAME," +
		"'utf8mb4' AS DEFAULT_CHARACTER_SET_NAME," +
		"'utf8mb4_0900_ai_ci' AS DEFAULT_COLLATION_NAME," +
		"if(true, NULL, '') AS SQL_PATH," +
		"cast('NO' as varchar(3)) AS DEFAULT_ENCRYPTION " +
		"FROM mo_catalog.mo_database where account_id = current_account_id() or (account_id = 0 and datname in ('mo_catalog'))"

	InformationSchemaCharacterSetsDDL = "CREATE TABLE information_schema.CHARACTER_SETS (" +
		"CHARACTER_SET_NAME varchar(64)," +
		"DEFAULT_COLLATE_NAME varchar(64)," +
		"DESCRIPTION varchar(2048)," +
		"MAXLEN int unsigned" +
		")"

	InformationSchemaTriggersDDL = "CREATE TABLE information_schema.TRIGGERS (" +
		"TRIGGER_CATALOG varchar(64)," +
		"TRIGGER_SCHEMA varchar(64)," +
		"TRIGGER_NAME varchar(64)," +
		"EVENT_MANIPULATION varchar(10)," +
		"EVENT_OBJECT_CATALOG varchar(64)," +
		"EVENT_OBJECT_SCHEMA varchar(64)," +
		"EVENT_OBJECT_TABLE varchar(64)," +
		"ACTION_ORDER int unsigned," +
		"ACTION_CONDITION binary(0)," +
		"ACTION_STATEMENT longtext," +
		"ACTION_ORIENTATION varchar(3)," +
		"ACTION_TIMING varchar(10)," +
		"ACTION_REFERENCE_OLD_TABLE binary(0)," +
		"ACTION_REFERENCE_NEW_TABLE binary(0)," +
		"ACTION_REFERENCE_OLD_ROW varchar(3)," +
		"ACTION_REFERENCE_NEW_ROW varchar(3)," +
		"CREATED timestamp(2)," +
		"SQL_MODE varchar(10)," +
		"DEFINER varchar(288)," +
		"CHARACTER_SET_CLIENT varchar(64)," +
		"COLLATION_CONNECTION varchar(64)," +
		"DATABASE_COLLATION varchar(64)" +
		")"

	InformationSchemaTablesDDL = fmt.Sprintf("CREATE VIEW information_schema.TABLES AS "+
		"SELECT 'def' AS TABLE_CATALOG,"+
		"reldatabase AS TABLE_SCHEMA,"+
		"relname AS TABLE_NAME,"+
		"(case when relkind = 'v' and (reldatabase='mo_catalog' or reldatabase='information_schema') then 'SYSTEM VIEW' "+
		"when relkind = 'v'  then 'VIEW' "+
		"when relkind = 'e' then 'EXTERNAL TABLE' "+
		"when relkind = 'r' then 'BASE TABLE' "+
		"else 'INTERNAL TABLE' end) AS TABLE_TYPE,"+
		"if(relkind = 'r','Tae',NULL) AS ENGINE,"+
		"if(relkind = 'v',NULL,10) AS VERSION,"+
		"'Compressed' AS ROW_FORMAT,"+
		"if(relkind = 'v', NULL, 0) AS TABLE_ROWS,"+
		"if(relkind = 'v', NULL, 0) AS AVG_ROW_LENGTH,"+
		"if(relkind = 'v', NULL, 0) AS DATA_LENGTH,"+
		"if(relkind = 'v', NULL, 0) AS MAX_DATA_LENGTH,"+
		"if(relkind = 'v', NULL, 0) AS INDEX_LENGTH,"+
		"if(relkind = 'v', NULL, 0) AS DATA_FREE,"+
		"if(relkind = 'v', NULL, internal_auto_increment(reldatabase, relname)) AS `AUTO_INCREMENT`,"+
		"created_time AS CREATE_TIME,"+
		"if(relkind = 'v', NULL, created_time) AS UPDATE_TIME,"+
		"if(relkind = 'v', NULL, created_time) AS CHECK_TIME,"+
		"'utf8mb4_0900_ai_ci' AS TABLE_COLLATION,"+
		"if(relkind = 'v', NULL, 0) AS CHECKSUM,"+
		"if(relkind = 'v', NULL, if(partitioned = 0, '', cast('partitioned' as varchar(256)))) AS CREATE_OPTIONS,"+
		"cast(rel_comment as text) AS TABLE_COMMENT "+
		"FROM mo_catalog.mo_tables tbl "+
		"WHERE tbl.account_id = current_account_id() and tbl.relname not like '%s' and tbl.relkind != '%s'", catalog.IndexTableNamePrefix+"%", catalog.SystemPartitionRel)

	InformationSchemaPartitionsDDL = "CREATE VIEW information_schema.`PARTITIONS` AS " +
		"SELECT " +
		"'def' AS `TABLE_CATALOG`," +
		"`tbl`.`reldatabase` AS `TABLE_SCHEMA`," +
		"`tbl`.`relname` AS `TABLE_NAME`," +
		"`part`.`name` AS `PARTITION_NAME`," +
		"NULL AS `SUBPARTITION_NAME`," +
		"`part`.`number` AS `PARTITION_ORDINAL_POSITION`," +
		"NULL AS `SUBPARTITION_ORDINAL_POSITION`," +
		"(case `part`.`partition_type` when 'HASH' then 'HASH' " +
		"when 'RANGE' then 'RANGE' " +
		"when 'LIST' then 'LIST' " +
		"when 'AUTO' then 'AUTO' " +
		"when 'KEY_51' then 'KEY' " +
		"when 'KEY_55' then 'KEY' " +
		"when 'LINEAR_KEY_51' then 'LINEAR KEY' " +
		"when 'LINEAR_KEY_55' then 'LINEAR KEY' " +
		"when 'LINEAR_HASH' then 'LINEAR HASH' " +
		"when 'RANGE_COLUMNS' then 'RANGE COLUMNS' " +
		"when 'LIST_COLUMNS' then 'LIST COLUMNS' else NULL end) AS `PARTITION_METHOD`," +
		"NULL AS `SUBPARTITION_METHOD`," +
		"`part`.`partition_expression` AS `PARTITION_EXPRESSION`," +
		"NULL AS `SUBPARTITION_EXPRESSION`," +
		"`part`.`description_utf8` AS `PARTITION_DESCRIPTION`," +
		"mo_table_rows(`tbl`.`reldatabase`, `part`.`partition_table_name`) AS `TABLE_ROWS`," +
		"0 AS `AVG_ROW_LENGTH`," +
		"mo_table_size(`tbl`.`reldatabase`, `part`.`partition_table_name`) AS `DATA_LENGTH`," +
		"0 AS `MAX_DATA_LENGTH`," +
		"0 AS `INDEX_LENGTH`," +
		"0 AS `DATA_FREE`," +
		"`tbl`.`created_time` AS `CREATE_TIME`," +
		"NULL AS `UPDATE_TIME`," +
		"NULL AS `CHECK_TIME`," +
		"NULL AS `CHECKSUM`," +
		"ifnull(`part`.`comment`,'')  AS `PARTITION_COMMENT`," +
		"'default' AS `NODEGROUP`," +
		"NULL AS `TABLESPACE_NAME` " +
		"FROM `mo_catalog`.`mo_tables` `tbl` LEFT JOIN `mo_catalog`.`mo_table_partitions` `part` " +
		"ON `part`.`table_id` = `tbl`.`rel_id` " +
		"WHERE `tbl`.`account_id` = current_account_id() and `tbl`.`partitioned` = 1"

	InformationSchemaViewsDDL = "CREATE VIEW information_schema.VIEWS AS " +
		"SELECT 'def' AS `TABLE_CATALOG`," +
		"tbl.reldatabase AS `TABLE_SCHEMA`," +
		"tbl.relname AS `TABLE_NAME`," +
		"tbl.rel_createsql AS `VIEW_DEFINITION`," +
		"'NONE' AS `CHECK_OPTION`," +
		"'YES' AS `IS_UPDATABLE`," +
		"usr.user_name + '@' + usr.user_host AS `DEFINER`," +
		"'DEFINER' AS `SECURITY_TYPE`," +
		"'utf8mb4' AS `CHARACTER_SET_CLIENT`," +
		"'utf8mb4_0900_ai_ci' AS `COLLATION_CONNECTION` " +
		"FROM mo_catalog.mo_tables tbl LEFT JOIN mo_catalog.mo_user usr ON tbl.creator = usr.user_id " +
		"WHERE tbl.account_id = current_account_id() and tbl.relkind = 'v' and tbl.reldatabase != 'information_schema'"

	InformationSchemaStatisticsDDL = "CREATE VIEW information_schema.`STATISTICS` AS " +
		"select 'def' AS `TABLE_CATALOG`," +
		"`tbl`.`reldatabase` AS `TABLE_SCHEMA`," +
		"`tbl`.`relname` AS `TABLE_NAME`," +
		"if(((`idx`.`type` = 'PRIMARY') or (`idx`.`type` = 'UNIQUE')),0,1) AS `NON_UNIQUE`," +
		"`tbl`.`reldatabase` AS `INDEX_SCHEMA`," +
		"`idx`.`name` AS `INDEX_NAME`," +
		"`idx`.`ordinal_position` AS `SEQ_IN_INDEX`," +
		"`idx`.`column_name` AS `COLUMN_NAME`," +
		"'A' AS `COLLATION`," +
		"0 AS `CARDINALITY`," +
		"NULL AS `SUB_PART`," +
		"NULL AS `PACKED`," +
		"if((`tcl`.`attnotnull` = 0),'YES','') AS `NULLABLE`," +
		"NULL AS `INDEX_TYPE`," +
		"if(((`idx`.`type` = 'PRIMARY') or (`idx`.`type` = 'UNIQUE')),'','') AS `COMMENT`," +
		"`idx`.`comment` AS `INDEX_COMMENT`," +
		"if(`idx`.`is_visible`,'YES','NO') AS `IS_VISIBLE`," +
		"NULL AS `EXPRESSION`" +
		"from (`mo_catalog`.`mo_indexes` `idx` " +
		"join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`))" +
		"join `mo_catalog`.`mo_columns` `tcl` on (`idx`.`table_id` = `tcl`.`att_relname_id` and `idx`.`column_name` = `tcl`.`attname`)"

	InformationSchemaReferentialConstraintsDDL = "CREATE VIEW information_schema.REFERENTIAL_CONSTRAINTS AS " +
		"SELECT DISTINCT " +
		"'def' AS CONSTRAINT_CATALOG, " +
		"fk.db_name AS CONSTRAINT_SCHEMA, " +
		"fk.constraint_name AS CONSTRAINT_NAME, " +
		"'def' AS UNIQUE_CONSTRAINT_CATALOG, " +
		"fk.refer_db_name AS UNIQUE_CONSTRAINT_SCHEMA, " +
		"idx.type AS UNIQUE_CONSTRAINT_NAME," +
		"'NONE' AS MATCH_OPTION, " +
		"fk.on_update AS UPDATE_RULE, " +
		"fk.on_delete AS DELETE_RULE, " +
		"fk.table_name AS TABLE_NAME, " +
		"fk.refer_table_name AS REFERENCED_TABLE_NAME " +
		"FROM mo_catalog.mo_foreign_keys fk " +
		"JOIN mo_catalog.mo_indexes idx ON (fk.refer_column_name = idx.column_name)"

	InformationSchemaEnginesDDL = "CREATE TABLE information_schema.ENGINES (" +
		"ENGINE varchar(64)," +
		"SUPPORT varchar(8)," +
		"COMMENT varchar(160)," +
		"TRANSACTIONS varchar(3)," +
		"XA varchar(3)," +
		"SAVEPOINTS varchar(3)" +
		")"

	InformationSchemaRoutinesDDL = "CREATE TABLE information_schema.ROUTINES (" +
		"SPECIFIC_NAME varchar(64)," +
		"ROUTINE_CATALOG varchar(64)," +
		"ROUTINE_SCHEMA varchar(64)," +
		"ROUTINE_NAME varchar(64)," +
		"ROUTINE_TYPE varchar(10)," +
		"DATA_TYPE  longtext," +
		"CHARACTER_MAXIMUM_LENGTH bigint," +
		"CHARACTER_OCTET_LENGTH bigint," +
		"NUMERIC_PRECISION int unsigned," +
		"NUMERIC_SCALE int unsigned," +
		"DATETIME_PRECISION int unsigned," +
		"CHARACTER_SET_NAME varchar(64)," +
		"COLLATION_NAME varchar(64)," +
		"DTD_IDENTIFIER longtext," +
		"ROUTINE_BODY varchar(3)," +
		"ROUTINE_DEFINITION longtext," +
		"EXTERNAL_NAME binary(0)," +
		"EXTERNAL_LANGUAGE varchar(64)," +
		"PARAMETER_STYLE varchar(3)," +
		"IS_DETERMINISTIC varchar(3)," +
		"SQL_DATA_ACCESS varchar(10)," +
		"SQL_PATH varchar(1000)," +
		"SECURITY_TYPE varchar(10)," +
		"CREATED timestamp," +
		"LAST_ALTERED timestamp," +
		"SQL_MODE varchar(1000)," +
		"ROUTINE_COMMENT text," +
		"DEFINER varchar(288)," +
		"CHARACTER_SET_CLIENT varchar(64)," +
		"COLLATION_CONNECTION varchar(64)," +
		"DATABASE_COLLATION  varchar(64)" +
		")"

	InformationSchemaParametersDDL = "CREATE TABLE information_schema.PARAMETERS (" +
		"SPECIFIC_CATALOG varchar(64)," +
		"SPECIFIC_SCHEMA varchar(64)," +
		"SPECIFIC_NAME varchar(64)," +
		"ORDINAL_POSITION bigint unsigned," +
		"PARAMETER_MODE varchar(5)," +
		"PARAMETER_NAME varchar(64)," +
		"DATA_TYPE longtext," +
		"CHARACTER_MAXIMUM_LENGTH bigint," +
		"CHARACTER_OCTET_LENGTH bigint," +
		"NUMERIC_PRECISION int unsigned," +
		"NUMERIC_SCALE bigint," +
		"DATETIME_PRECISION int unsigned," +
		"CHARACTER_SET_NAME varchar(64)," +
		"COLLATION_NAME varchar(64)," +
		"DTD_IDENTIFIER mediumtext," +
		"ROUTINE_TYPE  varchar(64)" +
		")"

	InformationSchemaKeywordsDDL = "CREATE TABLE information_schema.KEYWORDS (" +
		"WORD varchar(64)," +
		"RESERVED int unsigned" +
		")"

	InformationSchemaSchemaPrivilegesDDL = "CREATE TABLE information_schema.`SCHEMA_PRIVILEGES` (" +
		"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
		"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
		"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
		"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
		"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
		")"

	InformationSchemaTablePrivilegesDDL = "CREATE TABLE information_schema.`TABLE_PRIVILEGES` (" +
		"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
		"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
		"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
		"`TABLE_NAME` varchar(64) NOT NULL DEFAULT ''," +
		"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
		"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
		")"

	InformationSchemaColumnPrivilegesDDL = "CREATE TABLE information_schema.`COLUMN_PRIVILEGES` (" +
		"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
		"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
		"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
		"`TABLE_NAME` varchar(64) NOT NULL DEFAULT ''," +
		"`COLUMN_NAME` varchar(64) NOT NULL DEFAULT ''," +
		"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
		"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
		")"

	InformationSchemaCollationsDDL = "CREATE TABLE information_schema.COLLATIONS (" +
		"COLLATION_NAME varchar(64) NOT NULL," +
		"CHARACTER_SET_NAME varchar(64) NOT NULL," +
		"ID bigint unsigned NOT NULL DEFAULT 0," +
		"IS_DEFAULT varchar(3) NOT NULL DEFAULT ''," +
		"IS_COMPILED varchar(3) NOT NULL DEFAULT ''," +
		"SORTLEN int unsigned NOT NULL," +
		"PAD_ATTRIBUTE enum('PAD SPACE','NO PAD') NOT NULL" +
		")"

	InformationSchemaTableConstraintsDDL = "CREATE TABLE information_schema.TABLE_CONSTRAINTS (" +
		"CONSTRAINT_CATALOG varchar(64)," +
		"CONSTRAINT_SCHEMA varchar(64)," +
		"CONSTRAINT_NAME varchar(64)," +
		"TABLE_SCHEMA varchar(64)," +
		"TABLE_NAME varchar(64)," +
		"CONSTRAINT_TYPE varchar(11) NOT NULL DEFAULT ''," +
		"ENFORCED varchar(3) NOT NULL DEFAULT ''" +
		")"

	InformationSchemaEventsDDL = "CREATE TABLE information_schema.EVENTS (" +
		"EVENT_CATALOG varchar(64)," +
		"EVENT_SCHEMA varchar(64)," +
		"EVENT_NAME varchar(64) NOT NULL," +
		"`DEFINER` varchar(288) NOT NULL," +
		"TIME_ZONE varchar(64) NOT NULL," +
		"EVENT_BODY varchar(3) NOT NULL DEFAULT ''," +
		"EVENT_DEFINITION longtext NOT NULL," +
		"EVENT_TYPE varchar(9) NOT NULL DEFAULT ''," +
		"EXECUTE_AT datetime," +
		"INTERVAL_VALUE varchar(256)," +
		"INTERVAL_FIELD enum('YEAR','QUARTER','MONTH','DAY','HOUR','MINUTE','WEEK','SECOND','MICROSECOND','YEAR_MONTH','DAY_HOUR','DAY_MINUTE','DAY_SECOND','HOUR_MINUTE','HOUR_SECOND','MINUTE_SECOND','DAY_MICROSECOND','HOUR_MICROSECOND','MINUTE_MICROSECOND','SECOND_MICROSECOND')," +
		"SQL_MODE varchar(64) NOT NULL," +
		"STARTS datetime," +
		"ENDS datetime," +
		"STATUS varchar(21) NOT NULL DEFAULT ''," +
		"ON_COMPLETION varchar(12) NOT NULL DEFAULT ''," +
		"CREATED timestamp NOT NULL," +
		"LAST_ALTERED timestamp NOT NULL," +
		"LAST_EXECUTED datetime," +
		"EVENT_COMMENT varchar(2048) NOT NULL," +
		"ORIGINATOR int unsigned NOT NULL," +
		"CHARACTER_SET_CLIENT varchar(64) NOT NULL," +
		"COLLATION_CONNECTION varchar(64) NOT NULL," +
		"DATABASE_COLLATION varchar(64) NOT NULL" +
		")"

	InformationSchemaFilesDDL = "CREATE TABLE information_schema.FILES (" +
		"FILE_ID  bigint NOT NULL," +
		"FILE_NAME  text NOT NULL," +
		"FILE_TYPE  varchar(256)," +
		"TABLESPACE_NAME  varchar(268) NOT NULL," +
		"TABLE_CATALOG  char(0) NOT NULL," +
		"TABLE_SCHEMA  binary(0)," +
		"TABLE_NAME  binary(0)," +
		"LOGFILE_GROUP_NAME  varchar(256)," +
		"LOGFILE_GROUP_NUMBER  bigint," +
		"ENGINE  varchar(64) NOT NULL," +
		"FULLTEXT_KEYS  binary(0)," +
		"DELETED_ROWS  binary(0)," +
		"UPDATE_COUNT  binary(0)," +
		"FREE_EXTENTS  bigint," +
		"TOTAL_EXTENTS  bigint," +
		"EXTENT_SIZE  bigint," +
		"INITIAL_SIZE  bigint," +
		"MAXIMUM_SIZE  bigint," +
		"AUTOEXTEND_SIZE  bigint," +
		"CREATION_TIME  binary(0)," +
		"LAST_UPDATE_TIME  binary(0)," +
		"LAST_ACCESS_TIME  binary(0)," +
		"RECOVER_TIME  binary(0)," +
		"TRANSACTION_COUNTER  binary(0)," +
		"VERSION  bigint," +
		"ROW_FORMAT  varchar(256)," +
		"TABLE_ROWS  binary(0)," +
		"AVG_ROW_LENGTH  binary(0)," +
		"DATA_LENGTH  binary(0)," +
		"MAX_DATA_LENGTH  binary(0)," +
		"INDEX_LENGTH  binary(0)," +
		"DATA_FREE  bigint," +
		"CREATE_TIME  binary(0)," +
		"UPDATE_TIME  binary(0)," +
		"CHECK_TIME  binary(0)," +
		"CHECKSUM  binary(0)," +
		"STATUS  varchar(256)," +
		"EXTRA  varchar(256)" +
		")"
)
