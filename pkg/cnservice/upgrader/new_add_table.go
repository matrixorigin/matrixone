// Copyright 2021 - 2023 Matrix Origin
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

package upgrader

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

var (
	// mo_table_partitions;
	MoTablePartitionsTable = &table.Table{
		Account:  table.AccountAll,
		Database: catalog.MO_CATALOG,
		Table:    catalog.MO_TABLE_PARTITIONS,
		CreateTableSql: fmt.Sprintf(`CREATE TABLE %s.%s (
			  table_id bigint unsigned NOT NULL,
			  database_id bigint unsigned not null,
			  number smallint unsigned NOT NULL,
			  name varchar(64) NOT NULL,
    		  partition_type varchar(50) NOT NULL,
              partition_expression varchar(2048) NULL,
			  description_utf8 text,
			  comment varchar(2048) NOT NULL,
			  options text,
			  partition_table_name varchar(1024) NOT NULL,
    		  PRIMARY KEY table_id (table_id, name)
			);`, catalog.MO_CATALOG, catalog.MO_TABLE_PARTITIONS),
	}

	SysDaemonTaskTable = &table.Table{
		Account:  table.AccountSys,
		Database: catalog.MOTaskDB,
		Table:    catalog.MOSysDaemonTask,
		CreateTableSql: fmt.Sprintf(`create table %s.%s(
			task_id                     int primary key auto_increment,
			task_metadata_id            varchar(50),
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option        varchar(1000),
			account_id                  int unsigned not null,
			account                     varchar(128) not null,
			task_type                   varchar(64) not null,
			task_runner                 varchar(64),
			task_status                 int not null,
			last_heartbeat              timestamp,
			create_at                   timestamp not null,
			update_at                   timestamp not null,
			end_at                      timestamp,
			last_run                    timestamp,
			details                     blob)`,
			catalog.MOTaskDB, catalog.MOSysDaemonTask),
	}

	MoStagesTable = &table.Table{
		Account:  table.AccountAll,
		Database: catalog.MO_CATALOG,
		Table:    catalog.MO_STAGES,
		CreateTableSql: fmt.Sprintf(`CREATE TABLE %s.%s (
			stage_id int unsigned auto_increment,
			stage_name varchar(64),
			url text,
			stage_credentials text,
			stage_status varchar(64),
			created_time timestamp,
			comment text,
			primary key(stage_id)
		  );`, catalog.MO_CATALOG, catalog.MO_STAGES),
	}

	MoForeignKeys = &table.Table{
		Account:  table.AccountAll,
		Database: catalog.MO_CATALOG,
		Table:    catalog.MOForeignKeys,
		CreateTableSql: fmt.Sprintf(`create table %s.%s(
			constraint_name varchar(5000) not null,
			constraint_id BIGINT UNSIGNED not null default 0,
			db_name varchar(5000) not null,
			db_id BIGINT UNSIGNED not null default 0,
			table_name varchar(5000) not null,
			table_id BIGINT UNSIGNED not null default 0,
			column_name varchar(256) not null,
			column_id BIGINT UNSIGNED not null default 0,
			refer_db_name varchar(5000) not null,
			refer_db_id BIGINT UNSIGNED not null default 0,
			refer_table_name varchar(5000) not null,
			refer_table_id BIGINT UNSIGNED not null default 0,
			refer_column_name varchar(256) not null,
			refer_column_id BIGINT UNSIGNED not null default 0,
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
	}

	SqlStatementCUTable = &table.Table{
		Account:  table.AccountAll,
		Database: catalog.MO_SYSTEM_METRICS,
		Table:    catalog.MO_SQL_STMT_CU,
		CreateTableSql: fmt.Sprintf(`CREATE TABLE %s.%s (
account VARCHAR(1024) DEFAULT 'sys' COMMENT 'account name',
collecttime DATETIME NOT NULL COMMENT 'metric data collect time',
value DOUBLE DEFAULT '0.0' COMMENT 'metric value',
node VARCHAR(1024) DEFAULT 'monolithic' COMMENT 'mo node uuid',
role VARCHAR(1024) DEFAULT 'monolithic' COMMENT 'mo node role, like: CN, DN, LOG',
sql_source_type VARCHAR(1024) NOT NULL COMMENT 'sql_source_type, val like: external_sql, cloud_nonuser_sql, cloud_user_sql, internal_sql, ...'
) CLUSTER BY (account, collecttime);`, catalog.MO_SYSTEM_METRICS, catalog.MO_SQL_STMT_CU),
	}

	MysqlRoleEdgesTable = &table.Table{
		Account:  table.AccountAll,
		Database: sysview.MysqlDBConst,
		Table:    "role_edges",
		CreateTableSql: `CREATE TABLE IF NOT EXISTS mysql.role_edges (
			FROM_HOST char(255) NOT NULL DEFAULT '',
			FROM_USER char(32) NOT NULL DEFAULT '',
			TO_HOST char(255) NOT NULL DEFAULT '',
			TO_USER char(32) NOT NULL DEFAULT '',
			WITH_ADMIN_OPTION enum('N','Y') NOT NULL DEFAULT 'N',
			PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
		);`,
	}

	InfoSchemaSchemaPrivilegesTable = &table.Table{
		Account:  table.AccountAll,
		Database: sysview.InformationDBConst,
		Table:    "SCHEMA_PRIVILEGES",
		CreateTableSql: "CREATE TABLE IF NOT EXISTS `information_schema`.`SCHEMA_PRIVILEGES` (" +
			"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
			"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
			"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
			"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
			"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
			");",
	}

	InfoSchemaTablePrivilegesTable = &table.Table{
		Account:  table.AccountAll,
		Database: sysview.InformationDBConst,
		Table:    "TABLE_PRIVILEGES",
		CreateTableSql: "CREATE TABLE IF NOT EXISTS `information_schema`.`TABLE_PRIVILEGES` (" +
			"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
			"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
			"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
			"`TABLE_NAME` varchar(64) NOT NULL DEFAULT ''," +
			"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
			"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
			");",
	}

	InfoSchemaColumnPrivilegesTable = &table.Table{
		Account:  table.AccountAll,
		Database: sysview.InformationDBConst,
		Table:    "COLUMN_PRIVILEGES",
		CreateTableSql: "CREATE TABLE IF NOT EXISTS `information_schema`.`COLUMN_PRIVILEGES` (" +
			"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
			"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
			"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
			"`TABLE_NAME` varchar(64) NOT NULL DEFAULT ''," +
			"`COLUMN_NAME` varchar(64) NOT NULL DEFAULT ''," +
			"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
			"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
			");",
	}

	InfoSchemaCollationsTable = &table.Table{
		Account:  table.AccountAll,
		Database: sysview.InformationDBConst,
		Table:    "COLLATIONS",
		CreateTableSql: "CREATE TABLE IF NOT EXISTS information_schema.COLLATIONS (" +
			"COLLATION_NAME varchar(64) NOT NULL," +
			"CHARACTER_SET_NAME varchar(64) NOT NULL," +
			"ID bigint unsigned NOT NULL DEFAULT 0," +
			"IS_DEFAULT varchar(3) NOT NULL DEFAULT ''," +
			"IS_COMPILED varchar(3) NOT NULL DEFAULT ''," +
			"SORTLEN int unsigned NOT NULL," +
			"PAD_ATTRIBUTE enum('PAD SPACE','NO PAD') NOT NULL" +
			");",
	}

	InfoSchemaTableConstraintsTable = &table.Table{
		Account:  table.AccountAll,
		Database: sysview.InformationDBConst,
		Table:    "TABLE_CONSTRAINTS",
		CreateTableSql: "CREATE TABLE IF NOT EXISTS information_schema.TABLE_CONSTRAINTS (" +
			"CONSTRAINT_CATALOG varchar(64)," +
			"CONSTRAINT_SCHEMA varchar(64)," +
			"CONSTRAINT_NAME varchar(64)," +
			"TABLE_SCHEMA varchar(64)," +
			"TABLE_NAME varchar(64)," +
			"CONSTRAINT_TYPE varchar(11) NOT NULL DEFAULT ''," +
			"ENFORCED varchar(3) NOT NULL DEFAULT ''" +
			");",
	}

	InfoSchemaEventsTable = &table.Table{
		Account:  table.AccountAll,
		Database: sysview.InformationDBConst,
		Table:    "EVENTS",
		CreateTableSql: "CREATE TABLE IF NOT EXISTS information_schema.EVENTS (" +
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
	}
)

var needUpgradeNewTable = []*table.Table{
	MoTablePartitionsTable,
	SysDaemonTaskTable,
	MoStagesTable,
	MoForeignKeys,
	SqlStatementCUTable,
	MysqlRoleEdgesTable,
	InfoSchemaSchemaPrivilegesTable,
	InfoSchemaTablePrivilegesTable,
	InfoSchemaColumnPrivilegesTable,
	InfoSchemaCollationsTable,
	InfoSchemaTableConstraintsTable,
	InfoSchemaEventsTable,
}

var PARTITIONSView = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "PARTITIONS",
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `information_schema`.`PARTITIONS` AS " +
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
}

var STATISTICSView = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "STATISTICS",
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `information_schema`.`STATISTICS` AS " +
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
}

var processlistView = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "processlist",
	Columns: []table.Column{
		table.StringColumn("account", "the account name"),
		table.StringColumn("client_host", "the ip:port of the client"),
		table.StringColumn("proxy_host", "the ip:port on the proxy connection"),
		table.StringColumn("command", "the COMMAND send by client"),
		table.UInt64Column("conn_id", "the connection id of the tcp between client"),
		table.StringColumn("db", "the database be used"),
		table.StringColumn("host", "the ip:port of the mo-server"),
		table.StringColumn("info", "the sql"),
		table.StringColumn("node_id", "the id of the cn"),
		table.StringColumn("query_start", "the start time of the statement"),
		table.StringColumn("query_type", "the kind of the statement. DQL,TCL,etc"),
		table.StringColumn("role", "the role of the user"),
		table.StringColumn("session_id", "the id of the session"),
		table.StringColumn("session_start", "the start time of the session"),
		table.StringColumn("sql_source_type", "where does the sql come from. internal,external, etc"),
		table.StringColumn("statement_id", "the id of the statement"),
		table.StringColumn("statement_type", "the type of the statement.Select,Delete,Insert,etc"),
		table.StringColumn("txn_id", "the id of the transaction"),
		table.StringColumn("user", "the user name"),
	},
	CreateViewSql: fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.PROCESSLIST AS "+
		"select node_id, conn_id, session_id, account, user, host, db, "+
		"session_start, command, info, txn_id, statement_id, statement_type, "+
		"query_type, sql_source_type, query_start, client_host, role, proxy_host "+
		"from PROCESSLIST() A", sysview.InformationDBConst),
	//actually drop table here
	CreateTableSql: "drop view if exists `information_schema`.`PROCESSLIST`;",
}

var MoSessionsView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_CATALOG,
	Table:    "mo_sessions",
	Columns: []table.Column{
		table.StringColumn("account", "the account name"),
		table.StringColumn("client_host", "the ip:port of the client"),
		table.StringColumn("command", "the COMMAND send by client"),
		table.UInt64Column("conn_id", "the connection id of the tcp between client"),
		table.StringColumn("db", "the database be used"),
		table.StringColumn("host", "the ip:port of the mo-server"),
		table.StringColumn("info", "the sql"),
		table.StringColumn("node_id", "the id of the cn"),
		table.StringColumn("query_start", "the start time of the statement"),
		table.StringColumn("query_type", "the kind of the statement. DQL,TCL,etc"),
		table.StringColumn("role", "the role of the user"),
		table.StringColumn("session_id", "the id of the session"),
		table.StringColumn("session_start", "the start time of the session"),
		table.StringColumn("sql_source_type", "where does the sql come from. internal,external, etc"),
		table.StringColumn("statement_id", "the id of the statement"),
		table.StringColumn("statement_type", "the type of the statement.Select,Delete,Insert,etc"),
		table.StringColumn("txn_id", "the id of the transaction"),
		table.StringColumn("user", "the user name"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `mo_catalog`.`mo_sessions` AS SELECT * FROM mo_sessions() AS mo_sessions_tmp;",
	//actually drop view here
	CreateTableSql: "drop view if exists `mo_catalog`.`mo_sessions`;",
}

var MoConfigurationsView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_CATALOG,
	Table:    "mo_configurations",
	Columns: []table.Column{
		table.StringColumn("node_type", "the type of the node. cn,tn,log,proxy."),
		table.StringColumn("node_id", "the id of the node"),
		table.StringColumn("name", "the name of the configuration item"),
		table.UInt64Column("current_value", "the current value of the configuration item"),
		table.StringColumn("default_value", "the default value of the configuration item"),
		table.StringColumn("internal", "the configuration item is internal or external"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `mo_catalog`.`mo_configurations` AS SELECT * FROM mo_configurations() AS mo_configurations_tmp;",
	//actually drop view here
	CreateTableSql: "drop view if exists `mo_catalog`.`mo_configurations`;",
}

var MoLocksView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_CATALOG,
	Table:    "mo_locks",
	Columns: []table.Column{
		table.StringColumn("cn_id", "the cn id which cn lock stays on"),
		table.StringColumn("txn_id", "the txn id which txn holds the lock"),
		table.StringColumn("table_id", "the table that the lock is on"),
		table.StringColumn("lock_key", "point or range"),
		table.StringColumn("lock_content", "the content the clock is on"),
		table.StringColumn("lock_mode", "shared or exclusive"),
		table.StringColumn("lock_status", "acquired or wait"),
		table.StringColumn("lock_wait", "the txn that waits on the lock"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `mo_catalog`.`mo_locks` AS SELECT * FROM mo_locks() AS mo_locks_tmp;",
	//actually drop view here
	CreateTableSql: "drop view if exists `mo_catalog`.`mo_locks`;",
}

var MoVariablesView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_CATALOG,
	Table:    "mo_variables",
	Columns: []table.Column{
		table.StringColumn("configuration_id", "the id of configuration"),
		table.StringColumn("account_id", ""),
		table.StringColumn("account_name", ""),
		table.StringColumn("dat_name", "database name"),
		table.StringColumn("variable_name", "the name of variable"),
		table.StringColumn("variable_value", "the value of variable"),
		table.StringColumn("system_variables", "is system variable or not"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `mo_catalog`.`mo_variables` AS SELECT * FROM mo_catalog.mo_mysql_compatibility_mode;",
	//actually drop view here
	CreateTableSql: "drop view if exists `mo_catalog`.`mo_variables`;",
}

var SqlStatementHotspotView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_SYSTEM,
	Table:    motrace.SqlStatementHotspotTbl,
	Columns:  []table.Column{},
	// CreateViewSql get sql from original View define.
	CreateViewSql: motrace.SqlStatementHotspotView.ToCreateSql(context.Background(), true),
	//actually drop view here
	CreateTableSql: "DROP VIEW IF EXISTS `system`.`sql_statement_hotspot`;",
}

var MoTransactionsView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_CATALOG,
	Table:    "mo_transactions",
	Columns: []table.Column{
		table.StringColumn("cn_id", "the cn id which cn lock stays on"),
		table.StringColumn("txn_id", "the txn id which txn holds the lock"),
		table.StringColumn("create_ts", "the timestamp of the creation of the txn"),
		table.StringColumn("snapshot_ts", "the snapshot timestamp"),
		table.StringColumn("prepared_ts", "the prepared timestamp"),
		table.StringColumn("commit_ts", "the commit timestamp"),
		table.StringColumn("txn_mode", "pessimistic or optimistic"),
		table.StringColumn("isolation", "the txn isolation"),
		table.StringColumn("user_txn", "the user txn or not"),
		table.StringColumn("txn_status", "the txn status(active, committed, aborting, aborted). (distributed txn, prepared, committing"),
		table.StringColumn("table_id", "the table id"),
		table.StringColumn("lock_key", "point or range"),
		table.StringColumn("lock_content", "the content the clock is on"),
		table.StringColumn("lock_mode", "shared or exclusive"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `mo_catalog`.`mo_transactions` AS SELECT * FROM mo_transactions() AS mo_transactions_tmp;",
	//actually drop view here
	CreateTableSql: "drop view if exists `mo_catalog`.`mo_transactions`;",
}

var MoCacheView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_CATALOG,
	Table:    "mo_cache",
	Columns: []table.Column{
		table.StringColumn("node_type", "the type of the node. cn,tn"),
		table.StringColumn("node_id", "the id of node"),
		table.StringColumn("type", "the type of fileservice cache. memory, disk_cache"),
		table.StringColumn("used", "used bytes of the cache"),
		table.StringColumn("free", "free bytes of the cache"),
		table.StringColumn("hit_ratio", "the hit ratio of the cache"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `mo_catalog`.`mo_cache` AS SELECT * FROM mo_cache() AS mo_cache_tmp;",
	//actually drop view here
	CreateTableSql: "drop view if exists `mo_catalog`.`mo_cache`;",
}

var ReferentialConstraintsView = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "referential_constraints",
	CreateViewSql: "CREATE VIEW information_schema.REFERENTIAL_CONSTRAINTS " +
		"AS " +
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
		"JOIN mo_catalog.mo_indexes idx ON (fk.refer_column_name = idx.column_name)",
}

var transactionMetricView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_SYSTEM_METRICS,
	Table:    "sql_statement_duration_total",
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `system_metrics`.`sql_statement_duration_total` as " +
		"SELECT `collecttime`, `value`, `node`, `role`, `account`, `type` " +
		"from `system_metrics`.`metric` " +
		"where `metric_name` = 'sql_statement_duration_total'",
}

var registeredViews = []*table.Table{processlistView, MoLocksView, MoVariablesView, MoTransactionsView, MoCacheView}
var needUpgradeNewView = []*table.Table{transactionMetricView, PARTITIONSView, STATISTICSView, MoSessionsView, SqlStatementHotspotView, MoLocksView, MoConfigurationsView, MoVariablesView, MoTransactionsView, MoCacheView, ReferentialConstraintsView}

var InformationSchemaSCHEMATA = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "SCHEMATA",
	CreateViewSql: "CREATE VIEW information_schema.SCHEMATA AS SELECT " +
		"dat_catalog_name AS CATALOG_NAME," +
		"datname AS SCHEMA_NAME," +
		"'utf8mb4' AS DEFAULT_CHARACTER_SET_NAME," +
		"'utf8mb4_0900_ai_ci' AS DEFAULT_COLLATION_NAME," +
		"if(true, NULL, '') AS SQL_PATH," +
		"cast('NO' as varchar(3)) AS DEFAULT_ENCRYPTION " +
		"FROM mo_catalog.mo_database where account_id = current_account_id() or (account_id = 0 and datname in ('mo_catalog'))",
}

var InformationSchemaCOLUMNS = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "COLUMNS",
	CreateViewSql: fmt.Sprintf("CREATE VIEW information_schema.COLUMNS AS select "+
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
}

var InformationSchemaPARTITIONS = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "PARTITIONS",
	CreateViewSql: fmt.Sprintf("CREATE VIEW information_schema.PARTITIONS AS " +
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
		"WHERE `tbl`.`partitioned` = 1;"),
}

var InformationSchemaTABLES = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "TABLES",
	CreateViewSql: fmt.Sprintf("CREATE VIEW IF NOT EXISTS information_schema.TABLES AS "+
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
}

var needUpgradeExistingView = []*table.Table{
	InformationSchemaSCHEMATA,
	InformationSchemaCOLUMNS,
	InformationSchemaPARTITIONS,
	InformationSchemaTABLES,
}
