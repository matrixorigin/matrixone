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

package v1_2_0

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/util/sysview"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upg_mo_indexes_add_IndexAlgoName,
	upg_mo_indexes_add_IndexAlgoTableType,
	upg_mo_indexes_add_IndexAlgoParams,
	upg_mo_foreign_keys,
	upg_system_metrics_sql_statement_duration_total,
	upg_mo_snapshots,
	upg_sql_statement_cu,
	upg_mysql_role_edges,
	upg_information_schema_schema_privileges,
	upg_information_schema_table_privileges,
	upg_information_schema_column_privileges,
	upg_information_schema_collations,
	upg_information_schema_table_constraints,
	upg_information_schema_events,
	upg_information_schema_tables,
	upg_information_schema_processlist,
	upg_information_schema_referenctial_constraints,
	upg_information_schema_columns,
	upg_information_schema_views,
	upg_information_schema_partitions,
}

var UpgPrepareEntres = []versions.UpgradeEntry{
	upg_mo_indexes_add_IndexAlgoName,
	upg_mo_indexes_add_IndexAlgoTableType,
	upg_mo_indexes_add_IndexAlgoParams,
	upg_mo_foreign_keys,
}

var createFrameworkDepsEntres = []versions.UpgradeEntry{
	upg_mo_indexes_add_IndexAlgoName,
	upg_mo_indexes_add_IndexAlgoTableType,
	upg_mo_indexes_add_IndexAlgoParams,
	upg_mo_foreign_keys,
}

// MOForeignKeys = "mo_foreign_keys"
var upg_mo_foreign_keys = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MOForeignKeys,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
				constraint_name varchar(5000) not null,
				constraint_id BIGINT UNSIGNED not null,
				db_name varchar(5000) not null,
				db_id BIGINT UNSIGNED not null,
				table_name varchar(5000) not null,
				table_id BIGINT UNSIGNED not null,
				column_name varchar(256) not null,
				column_id BIGINT UNSIGNED not null,
				refer_db_name varchar(5000) not null,
				refer_db_id BIGINT UNSIGNED not null,
				refer_table_name varchar(5000) not null,
				refer_table_id BIGINT UNSIGNED not null,
				refer_column_name varchar(256) not null,
				refer_column_id BIGINT UNSIGNED not null,
				on_delete varchar(128) not null,
				on_update varchar(128) not null,
				primary key(
					constraint_name,
					constraint_id,
					db_name,
					db_id,
					table_name,
					table_id,
					column_name,
					column_id,
					refer_db_name,
					refer_db_id,
					refer_table_name,
					refer_table_id,
					refer_column_name,
					refer_column_id)
			);`, catalog.MO_CATALOG, catalog.MOForeignKeys),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MOForeignKeys)
		if err != nil {
			return false, err
		}

		if isExist {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_indexes_add_IndexAlgoName = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_INDEXES,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    fmt.Sprintf(`alter table %s.%s add column %s varchar(11) after type;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoName),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoName)
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_indexes_add_IndexAlgoTableType = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_INDEXES,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    fmt.Sprintf(`alter table %s.%s add column %s varchar(11) after %s;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoTableType, catalog.IndexAlgoName),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoTableType)
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_indexes_add_IndexAlgoParams = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_INDEXES,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    fmt.Sprintf(`alter table %s.%s add column %s varchar(2048) after %s;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoParams, catalog.IndexAlgoTableType),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoParams)
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_system_metrics_sql_statement_duration_total = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: "sql_statement_duration_total",
	UpgType:   versions.CREATE_VIEW,
	UpgSql: fmt.Sprintf("CREATE VIEW IF NOT EXISTS `%s`.`%s` as "+
		"SELECT `collecttime`, `value`, `node`, `role`, `account`, `type` "+
		"from `system_metrics`.`metric` "+
		"where `metric_name` = 'sql_statement_duration_total'",
		catalog.MO_SYSTEM_METRICS, "sql_statement_duration_total"),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return false, nil
	},
}

var upg_mo_snapshots = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_SNAPSHOTS,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`CREATE TABLE %s.%s (
			snapshot_id uuid unique key,
			sname varchar(64) primary key,
			ts timestamp,
			level enum('cluster','account','database','table'),
	        account_name varchar(300),
			database_name varchar(5000),
			table_name  varchar(5000),
			obj_id bigint unsigned
			);`, catalog.MO_CATALOG, catalog.MO_SNAPSHOTS),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_SNAPSHOTS)
		if err != nil {
			return false, err
		}

		if isExist {
			return true, nil
		}
		return false, nil
	},
}

var upg_sql_statement_cu = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: catalog.MO_SQL_STMT_CU,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`CREATE TABLE %s.%s (
		account VARCHAR(1024) DEFAULT 'sys' COMMENT 'account name',
		collecttime DATETIME NOT NULL COMMENT 'metric data collect time',
		value DOUBLE DEFAULT '0.0' COMMENT 'metric value',
		node VARCHAR(1024) DEFAULT 'monolithic' COMMENT 'mo node uuid',
		role VARCHAR(1024) DEFAULT 'monolithic' COMMENT 'mo node role, like: CN, DN, LOG',
		sql_source_type VARCHAR(1024) NOT NULL COMMENT 'sql_source_type, val like: external_sql, cloud_nonuser_sql, cloud_user_sql, internal_sql, ...'
		) CLUSTER BY (account, collecttime);`, catalog.MO_SYSTEM_METRICS, catalog.MO_SQL_STMT_CU),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM_METRICS, catalog.MO_SQL_STMT_CU)
		if err != nil {
			return false, err
		}

		if isExist {
			return true, nil
		}
		return false, nil
	},
}

// ------------------------------------------------------------------------------------------------------------------
var upg_mysql_role_edges = versions.UpgradeEntry{
	Schema:    sysview.MysqlDBConst,
	TableName: "role_edges",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: `CREATE TABLE IF NOT EXISTS mysql.role_edges (
			FROM_HOST char(255) NOT NULL DEFAULT '',
			FROM_USER char(32) NOT NULL DEFAULT '',
			TO_HOST char(255) NOT NULL DEFAULT '',
			TO_USER char(32) NOT NULL DEFAULT '',
			WITH_ADMIN_OPTION enum('N','Y') NOT NULL DEFAULT 'N',
			PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
		);`,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.MysqlDBConst, "role_edges")
	},
}

var upg_information_schema_schema_privileges = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "schema_privileges",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: "CREATE TABLE IF NOT EXISTS `information_schema`.`schema_privileges` (" +
		"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
		"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
		"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
		"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
		"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
		");",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "schema_privileges")
	},
}

var upg_information_schema_table_privileges = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "table_privileges",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: "CREATE TABLE IF NOT EXISTS `information_schema`.`table_privileges` (" +
		"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
		"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
		"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
		"`TABLE_NAME` varchar(64) NOT NULL DEFAULT ''," +
		"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
		"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
		");",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "table_privileges")
	},
}

var upg_information_schema_column_privileges = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "column_privileges",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: "CREATE TABLE IF NOT EXISTS `information_schema`.`column_privileges` (" +
		"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
		"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
		"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
		"`TABLE_NAME` varchar(64) NOT NULL DEFAULT ''," +
		"`COLUMN_NAME` varchar(64) NOT NULL DEFAULT ''," +
		"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
		"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
		");",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "column_privileges")
	},
}

var upg_information_schema_collations = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "collations",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: "CREATE TABLE IF NOT EXISTS information_schema.collations (" +
		"COLLATION_NAME varchar(64) NOT NULL," +
		"CHARACTER_SET_NAME varchar(64) NOT NULL," +
		"ID bigint unsigned NOT NULL DEFAULT 0," +
		"IS_DEFAULT varchar(3) NOT NULL DEFAULT ''," +
		"IS_COMPILED varchar(3) NOT NULL DEFAULT ''," +
		"SORTLEN int unsigned NOT NULL," +
		"PAD_ATTRIBUTE enum('PAD SPACE','NO PAD') NOT NULL" +
		");",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "collations")
	},
}

var upg_information_schema_table_constraints = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "table_constraints",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: "CREATE TABLE IF NOT EXISTS information_schema.table_constraints (" +
		"CONSTRAINT_CATALOG varchar(64)," +
		"CONSTRAINT_SCHEMA varchar(64)," +
		"CONSTRAINT_NAME varchar(64)," +
		"TABLE_SCHEMA varchar(64)," +
		"TABLE_NAME varchar(64)," +
		"CONSTRAINT_TYPE varchar(11) NOT NULL DEFAULT ''," +
		"ENFORCED varchar(3) NOT NULL DEFAULT ''" +
		");",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "table_constraints")
	},
}

var upg_information_schema_events = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "events",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: "CREATE TABLE IF NOT EXISTS information_schema.events (" +
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
		");",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "events")
	},
}

var upg_information_schema_tables = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "TABLES",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql: fmt.Sprintf("CREATE VIEW IF NOT EXISTS information_schema.TABLES AS "+
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
		"WHERE tbl.account_id = current_account_id() and tbl.relname not like '%s' and tbl.relkind != '%s';", catalog.IndexTableNamePrefix+"%", catalog.SystemPartitionRel),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "TABLES")
		if err != nil {
			return false, err
		}

		if exists && viewDef == fmt.Sprintf("CREATE VIEW IF NOT EXISTS information_schema.TABLES AS "+
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
			"WHERE tbl.account_id = current_account_id() and tbl.relname not like '%s' and tbl.relkind != '%s';", catalog.IndexTableNamePrefix+"%", catalog.SystemPartitionRel) {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "TABLES"),
}

var upg_information_schema_processlist = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "processlist",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql: fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.PROCESSLIST AS "+
		"select node_id, conn_id, session_id, account, user, host, db, "+
		"session_start, command, info, txn_id, statement_id, statement_type, "+
		"query_type, sql_source_type, query_start, client_host, role, proxy_host "+
		"from PROCESSLIST() A", sysview.InformationDBConst),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "processlist")
		if err != nil {
			return false, err
		}

		if exists && viewDef == fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.PROCESSLIST AS "+
			"select node_id, conn_id, session_id, account, user, host, db, "+
			"session_start, command, info, txn_id, statement_id, statement_type, "+
			"query_type, sql_source_type, query_start, client_host, role, proxy_host "+
			"from PROCESSLIST() A", sysview.InformationDBConst) {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "processlist"),
}

var upg_information_schema_referenctial_constraints = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "REFERENTIAL_CONSTRAINTS",
	UpgType:   versions.CREATE_VIEW,
	UpgSql: fmt.Sprintf("CREATE VIEW %s.REFERENTIAL_CONSTRAINTS AS "+
		"SELECT DISTINCT "+
		"'def' AS CONSTRAINT_CATALOG, "+
		"fk.db_name AS CONSTRAINT_SCHEMA, "+
		"fk.constraint_name AS CONSTRAINT_NAME, "+
		"'def' AS UNIQUE_CONSTRAINT_CATALOG, "+
		"fk.refer_db_name AS UNIQUE_CONSTRAINT_SCHEMA, "+
		"idx.type AS UNIQUE_CONSTRAINT_NAME,"+
		"'NONE' AS MATCH_OPTION, "+
		"fk.on_update AS UPDATE_RULE, "+
		"fk.on_delete AS DELETE_RULE, "+
		"fk.table_name AS TABLE_NAME, "+
		"fk.refer_table_name AS REFERENCED_TABLE_NAME "+
		"FROM mo_catalog.mo_foreign_keys fk "+
		"JOIN mo_catalog.mo_indexes idx ON (fk.refer_column_name = idx.column_name)", sysview.InformationDBConst),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "REFERENTIAL_CONSTRAINTS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == fmt.Sprintf("CREATE VIEW %s.REFERENTIAL_CONSTRAINTS AS "+
			"SELECT DISTINCT "+
			"'def' AS CONSTRAINT_CATALOG, "+
			"fk.db_name AS CONSTRAINT_SCHEMA, "+
			"fk.constraint_name AS CONSTRAINT_NAME, "+
			"'def' AS UNIQUE_CONSTRAINT_CATALOG, "+
			"fk.refer_db_name AS UNIQUE_CONSTRAINT_SCHEMA, "+
			"idx.type AS UNIQUE_CONSTRAINT_NAME,"+
			"'NONE' AS MATCH_OPTION, "+
			"fk.on_update AS UPDATE_RULE, "+
			"fk.on_delete AS DELETE_RULE, "+
			"fk.table_name AS TABLE_NAME, "+
			"fk.refer_table_name AS REFERENCED_TABLE_NAME "+
			"FROM mo_catalog.mo_foreign_keys fk "+
			"JOIN mo_catalog.mo_indexes idx ON (fk.refer_column_name = idx.column_name)", sysview.InformationDBConst) {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "REFERENTIAL_CONSTRAINTS"),
}

var upg_information_schema_columns = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "COLUMNS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql: fmt.Sprintf("CREATE VIEW information_schema.COLUMNS AS select "+
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
		"from mo_catalog.mo_columns "+
		"where account_id = current_account_id() and att_relname!='%s' and att_relname not like '%s' and attname != '%s'", catalog.MOAutoIncrTable, catalog.PrefixPriColName+"%", catalog.Row_ID),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "COLUMNS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == fmt.Sprintf("CREATE VIEW information_schema.COLUMNS AS select "+
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
			"from mo_catalog.mo_columns "+
			"where account_id = current_account_id() and att_relname!='%s' and att_relname not like '%s' and attname != '%s'", catalog.MOAutoIncrTable, catalog.PrefixPriColName+"%", catalog.Row_ID) {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "COLUMNS"),
}

var upg_information_schema_views = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "VIEWS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql: "CREATE VIEW IF NOT EXISTS information_schema.VIEWS AS " +
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
		"WHERE tbl.account_id = current_account_id() and tbl.relkind = 'v' and tbl.reldatabase != 'information_schema'",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "VIEWS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == "CREATE VIEW IF NOT EXISTS information_schema.VIEWS AS "+
			"SELECT 'def' AS `TABLE_CATALOG`,"+
			"tbl.reldatabase AS `TABLE_SCHEMA`,"+
			"tbl.relname AS `TABLE_NAME`,"+
			"tbl.rel_createsql AS `VIEW_DEFINITION`,"+
			"'NONE' AS `CHECK_OPTION`,"+
			"'YES' AS `IS_UPDATABLE`,"+
			"usr.user_name + '@' + usr.user_host AS `DEFINER`,"+
			"'DEFINER' AS `SECURITY_TYPE`,"+
			"'utf8mb4' AS `CHARACTER_SET_CLIENT`,"+
			"'utf8mb4_0900_ai_ci' AS `COLLATION_CONNECTION` "+
			"FROM mo_catalog.mo_tables tbl LEFT JOIN mo_catalog.mo_user usr ON tbl.creator = usr.user_id "+
			"WHERE tbl.account_id = current_account_id() and tbl.relkind = 'v' and tbl.reldatabase != 'information_schema'" {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "VIEWS"),
}

var upg_information_schema_partitions = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "PARTITIONS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql: "CREATE VIEW IF NOT EXISTS information_schema.`PARTITIONS` AS " +
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
		"WHERE `tbl`.`account_id` = current_account_id() and `tbl`.`partitioned` = 1;",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "PARTITIONS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == "CREATE VIEW IF NOT EXISTS information_schema.`PARTITIONS` AS "+
			"SELECT "+
			"'def' AS `TABLE_CATALOG`,"+
			"`tbl`.`reldatabase` AS `TABLE_SCHEMA`,"+
			"`tbl`.`relname` AS `TABLE_NAME`,"+
			"`part`.`name` AS `PARTITION_NAME`,"+
			"NULL AS `SUBPARTITION_NAME`,"+
			"`part`.`number` AS `PARTITION_ORDINAL_POSITION`,"+
			"NULL AS `SUBPARTITION_ORDINAL_POSITION`,"+
			"(case `part`.`partition_type` when 'HASH' then 'HASH' "+
			"when 'RANGE' then 'RANGE' "+
			"when 'LIST' then 'LIST' "+
			"when 'AUTO' then 'AUTO' "+
			"when 'KEY_51' then 'KEY' "+
			"when 'KEY_55' then 'KEY' "+
			"when 'LINEAR_KEY_51' then 'LINEAR KEY' "+
			"when 'LINEAR_KEY_55' then 'LINEAR KEY' "+
			"when 'LINEAR_HASH' then 'LINEAR HASH' "+
			"when 'RANGE_COLUMNS' then 'RANGE COLUMNS' "+
			"when 'LIST_COLUMNS' then 'LIST COLUMNS' else NULL end) AS `PARTITION_METHOD`,"+
			"NULL AS `SUBPARTITION_METHOD`,"+
			"`part`.`partition_expression` AS `PARTITION_EXPRESSION`,"+
			"NULL AS `SUBPARTITION_EXPRESSION`,"+
			"`part`.`description_utf8` AS `PARTITION_DESCRIPTION`,"+
			"mo_table_rows(`tbl`.`reldatabase`, `part`.`partition_table_name`) AS `TABLE_ROWS`,"+
			"0 AS `AVG_ROW_LENGTH`,"+
			"mo_table_size(`tbl`.`reldatabase`, `part`.`partition_table_name`) AS `DATA_LENGTH`,"+
			"0 AS `MAX_DATA_LENGTH`,"+
			"0 AS `INDEX_LENGTH`,"+
			"0 AS `DATA_FREE`,"+
			"`tbl`.`created_time` AS `CREATE_TIME`,"+
			"NULL AS `UPDATE_TIME`,"+
			"NULL AS `CHECK_TIME`,"+
			"NULL AS `CHECKSUM`,"+
			"ifnull(`part`.`comment`,'')  AS `PARTITION_COMMENT`,"+
			"'default' AS `NODEGROUP`,"+
			"NULL AS `TABLESPACE_NAME` "+
			"FROM `mo_catalog`.`mo_tables` `tbl` LEFT JOIN `mo_catalog`.`mo_table_partitions` `part` "+
			"ON `part`.`table_id` = `tbl`.`rel_id` "+
			"WHERE `tbl`.`account_id` = current_account_id() and `tbl`.`partitioned` = 1;" {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "PARTITIONS"),
}
