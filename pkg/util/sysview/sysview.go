// Copyright 2022 Matrix Origin
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
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const (
	MysqlDBConst       = "mysql"
	InformationDBConst = "information_schema"
	sqlCreateDBConst   = "create database if not exists "
	sqlUseDbConst      = "use "
)

var (
	InitMysqlSysTables = []string{
		`CREATE TABLE IF NOT EXISTS user (
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
		  );`,
		`CREATE TABLE IF NOT EXISTS db (
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
		  );`,
		`CREATE TABLE IF NOT EXISTS procs_priv (
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
		  );`,
		`CREATE TABLE IF NOT EXISTS columns_priv (
			Host char(255)  NOT NULL DEFAULT '',
			Db char(64)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Table_name char(64)  NOT NULL DEFAULT '',
			Column_name char(64)  NOT NULL DEFAULT '',
			Timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			Column_priv varchar(10) NOT NULL DEFAULT '',
			PRIMARY KEY (Host,Db,User,Table_name,Column_name)
		  );`,
		`CREATE TABLE IF NOT EXISTS tables_priv (
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
		  );`,
		`CREATE TABLE IF NOT EXISTS role_edges (
			FROM_HOST char(255) NOT NULL DEFAULT '',
			FROM_USER char(32) NOT NULL DEFAULT '',
			TO_HOST char(255) NOT NULL DEFAULT '',
			TO_USER char(32) NOT NULL DEFAULT '',
			WITH_ADMIN_OPTION enum('N','Y') NOT NULL DEFAULT 'N',
			PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
		);`,
	}
	InitInformationSchemaSysTables = []string{
		"CREATE TABLE IF NOT EXISTS KEY_COLUMN_USAGE(" +
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
			");",
		fmt.Sprintf("CREATE VIEW information_schema.COLUMNS AS select "+
			"'def' as TABLE_CATALOG,"+
			"att_database as TABLE_SCHEMA,"+
			"att_relname AS TABLE_NAME,"+
			"attname AS COLUMN_NAME,"+
			"attnum AS ORDINAL_POSITION,"+
			"mo_show_visible_bin(att_default,1) as COLUMN_DEFAULT,"+
			"(case when attnotnull != 0 then 'NO' else 'YES' end) as IS_NULLABLE,"+
			"mo_show_visible_bin(atttyp,2) as DATA_TYPE,"+
			"internal_char_length(atttyp) AS CHARACTER_MAXIMUM_LENGTH,"+
			"internal_char_size(atttyp) AS CHARACTER_OCTET_LENGTH,"+
			"internal_numeric_precision(atttyp) AS NUMERIC_PRECISION,"+
			"internal_numeric_scale(atttyp) AS NUMERIC_SCALE,"+
			"internal_datetime_scale(atttyp) AS DATETIME_PRECISION,"+
			"(case internal_column_character_set(atttyp) WHEN 0 then 'utf8' WHEN 1 then 'utf8' else NULL end) AS CHARACTER_SET_NAME,"+
			"(case internal_column_character_set(atttyp) WHEN 0 then 'utf8_bin' WHEN 1 then 'utf8_bin' else NULL end) AS COLLATION_NAME,"+
			"mo_show_visible_bin(atttyp,3) as COLUMN_TYPE,"+
			"case when att_constraint_type = 'p' then 'PRI' else '' end as COLUMN_KEY,"+
			"case when att_is_auto_increment = 1 then 'auto_increment' else '' end as EXTRA,"+
			"'select,insert,update,references' as `PRIVILEGES`,"+
			"att_comment as COLUMN_COMMENT,"+
			"cast('' as varchar(500)) as GENERATION_EXPRESSION,"+
			"if(true, NULL, 0) as SRS_ID "+
			"from mo_catalog.mo_columns where att_relname!='%s' and att_relname not like '%s' and attname != '%s'", catalog.MOAutoIncrTable, catalog.PrefixPriColName+"%", catalog.Row_ID),
		"CREATE TABLE IF NOT EXISTS PROFILING (" +
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
			");",
		"CREATE VIEW IF NOT EXISTS `PROCESSLIST` AS SELECT * FROM PROCESSLIST() A;",
		"CREATE TABLE IF NOT EXISTS USER_PRIVILEGES (" +
			"GRANTEE varchar(292) NOT NULL DEFAULT ''," +
			"TABLE_CATALOG varchar(512) NOT NULL DEFAULT ''," +
			"PRIVILEGE_TYPE varchar(64) NOT NULL DEFAULT ''," +
			"IS_GRANTABLE varchar(3) NOT NULL DEFAULT ''" +
			");",

		"CREATE VIEW SCHEMATA AS SELECT " +
			"dat_catalog_name AS CATALOG_NAME," +
			"datname AS SCHEMA_NAME," +
			"'utf8mb4' AS DEFAULT_CHARACTER_SET_NAME," +
			"'utf8mb4_0900_ai_ci' AS DEFAULT_COLLATION_NAME," +
			"if(true, NULL, '') AS SQL_PATH," +
			"cast('NO' as varchar(3)) AS DEFAULT_ENCRYPTION " +
			"FROM mo_catalog.mo_database where account_id = current_account_id() or (account_id = 0 and datname in ('mo_catalog'))",
		"CREATE TABLE IF NOT EXISTS CHARACTER_SETS(" +
			"CHARACTER_SET_NAME varchar(64)," +
			"DEFAULT_COLLATE_NAME varchar(64)," +
			"DESCRIPTION varchar(2048)," +
			"MAXLEN int unsigned" +
			");",
		"CREATE TABLE IF NOT EXISTS TRIGGERS(" +
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
			");",

		fmt.Sprintf("CREATE VIEW IF NOT EXISTS TABLES AS "+
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
			"WHERE tbl.relname not like '%s' and tbl.relkind != '%s';", catalog.IndexTableNamePrefix+"%", catalog.SystemPartitionRel),

		"CREATE VIEW IF NOT EXISTS `PARTITIONS` AS " +
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
			"WHERE `tbl`.`partitioned` = 1;",

		"CREATE VIEW IF NOT EXISTS VIEWS AS " +
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
			"WHERE tbl.relkind = 'v' and tbl.reldatabase != 'information_schema'",

		"CREATE VIEW IF NOT EXISTS `STATISTICS` AS " +
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
			"join `mo_catalog`.`mo_columns` `tcl` on (`idx`.`table_id` = `tcl`.`att_relname_id` and `idx`.`column_name` = `tcl`.`attname`)",

		"CREATE TABLE IF NOT EXISTS ENGINES (" +
			"ENGINE varchar(64)," +
			"SUPPORT varchar(8)," +
			"COMMENT varchar(160)," +
			"TRANSACTIONS varchar(3)," +
			"XA varchar(3)," +
			"SAVEPOINTS varchar(3)" +
			");",
		"CREATE TABLE IF NOT EXISTS ROUTINES (" +
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
			");",
		"CREATE TABLE IF NOT EXISTS PARAMETERS(" +
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
			");",
		"CREATE TABLE IF NOT EXISTS KEYWORDS (" +
			"WORD varchar(64)," +
			"RESERVED int unsigned" +
			");",
		"CREATE TABLE IF NOT EXISTS `SCHEMA_PRIVILEGES` (" +
			"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
			"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
			"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
			"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
			"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
			");",
		"CREATE TABLE IF NOT EXISTS `TABLE_PRIVILEGES` (" +
			"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
			"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
			"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
			"`TABLE_NAME` varchar(64) NOT NULL DEFAULT ''," +
			"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
			"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
			");",
		"CREATE TABLE IF NOT EXISTS `COLUMN_PRIVILEGES` (" +
			"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
			"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
			"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
			"`TABLE_NAME` varchar(64) NOT NULL DEFAULT ''," +
			"`COLUMN_NAME` varchar(64) NOT NULL DEFAULT ''," +
			"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
			"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
			");",
		informationSchemaKeywordsData,
	}
)

func InitSchema(ctx context.Context, ieFactory func() ie.InternalExecutor) error {
	ctx = defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
	initMysqlTables(ctx, ieFactory)
	initInformationSchemaTables(ctx, ieFactory)
	return nil
}

func initMysqlTables(ctx context.Context, ieFactory func() ie.InternalExecutor) {
	exec := ieFactory()
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(MysqlDBConst).Internal(true).Finish())
	mustExec := func(sql string) {
		if err := exec.Exec(ctx, sql, ie.NewOptsBuilder().Finish()); err != nil {
			panic(fmt.Sprintf("[Mysql] init mysql tables error: %v, sql: %s", err, sql))
		}
	}
	mustExec(sqlCreateDBConst + MysqlDBConst)
	mustExec(sqlUseDbConst + MysqlDBConst)
	var createCost time.Duration
	defer func() {
		logutil.Debugf("[Mysql] init mysql tables: create cost %d ms", createCost.Milliseconds())
	}()
	instant := time.Now()

	for _, sql := range InitMysqlSysTables {
		mustExec(sql)
	}
	createCost = time.Since(instant)
}

func initInformationSchemaTables(ctx context.Context, ieFactory func() ie.InternalExecutor) {
	exec := ieFactory()
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(InformationDBConst).Internal(true).Finish())
	mustExec := func(sql string) {
		if err := exec.Exec(ctx, sql, ie.NewOptsBuilder().Finish()); err != nil {
			panic(fmt.Sprintf("[information_schema] init information_schema tables error: %v, sql: %s", err, sql))
		}
	}
	mustExec(sqlCreateDBConst + InformationDBConst)
	mustExec(sqlUseDbConst + InformationDBConst)
	var createCost time.Duration
	defer func() {
		logutil.Debugf("[information_schema] init information_schema tables: create cost %d ms", createCost.Milliseconds())
	}()
	instant := time.Now()

	for _, sql := range InitInformationSchemaSysTables {
		mustExec(sql)
	}
	createCost = time.Since(instant)
}
