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

package v1_3_0

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_mo_pitr,
	upg_mo_subs,
	upgradeShardingMetadata,
	upgradeSharding,
	upg_drop_idx_task_status,
	upg_drop_idx_task_runner,
	upg_drop_idx_task_executor,
	upg_drop_idx_task_epoch,
	upg_drop_task_metadata_id,
	upg_mo_cdc_task,
	upg_mo_cdc_watermark,
	upg_mo_data_key,
	upg_rename_system_stmt_info_120,
	upg_create_system_stmt_info_130,
	upg_rename_system_metrics_metric_120,
	upg_create_system_metrics_metric_130,
	upg_system_metrics_sql_stmt_cu_comment,
}

var needUpgradePubSub = false

var upg_mo_pitr = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_PITR,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoPitrDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_PITR)
		if err != nil {
			return false, err
		}

		if isExist {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_subs = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_SUBS,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoSubsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_SUBS)
		if err != nil {
			return false, err
		}

		if isExist {
			return true, nil
		}
		needUpgradePubSub = true
		return false, nil
	},
}

// ------------------------------------------------------------------------------------------------------------
var upg_drop_idx_task_status = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.DROP_INDEX,
	UpgSql:    fmt.Sprintf(`ALTER TABLE %s.%s DROP INDEX idx_task_status`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, err := versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_status")
		return !exists, err
	},
}

var upg_drop_idx_task_runner = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.DROP_INDEX,
	UpgSql:    fmt.Sprintf(`ALTER TABLE %s.%s DROP INDEX idx_task_runner`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, err := versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_runner")
		return !exists, err
	},
}

var upg_drop_idx_task_executor = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.DROP_INDEX,
	UpgSql:    fmt.Sprintf(`ALTER TABLE %s.%s DROP INDEX idx_task_executor`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, err := versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_executor")
		return !exists, err
	},
}

var upg_drop_idx_task_epoch = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.DROP_INDEX,
	UpgSql:    fmt.Sprintf(`ALTER TABLE %s.%s DROP INDEX idx_task_epoch`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, err := versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_epoch")
		return !exists, err
	},
}

var upg_drop_task_metadata_id = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.DROP_INDEX,
	UpgSql:    fmt.Sprintf(`ALTER TABLE %s.%s DROP INDEX task_metadata_id`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, err := versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "task_metadata_id")
		return !exists, err
	},
}

var upgradeShardingMetadata = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MOShardsMetadata,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    shardservice.MetadataTableSQL,
	CheckFunc: func(
		txn executor.TxnExecutor,
		accountId uint32,
	) (bool, error) {
		return versions.CheckTableDefinition(
			txn,
			accountId,
			catalog.MO_CATALOG,
			catalog.MOShardsMetadata,
		)
	},
}

var upgradeSharding = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MOShards,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    shardservice.ShardsTableSQL,
	CheckFunc: func(
		txn executor.TxnExecutor,
		accountId uint32,
	) (bool, error) {
		return versions.CheckTableDefinition(
			txn,
			accountId,
			catalog.MO_CATALOG,
			catalog.MOShards,
		)
	},
}

var upg_mo_cdc_task = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_CDC_TASK,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoCdcTaskDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_CDC_TASK)
	},
}

var upg_mo_cdc_watermark = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_CDC_WATERMARK,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoCdcWatermarkDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_CDC_WATERMARK)
	},
}

var upg_mo_data_key = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_DATA_KEY,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoDataKeyDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_DATA_KEY)
	},
}

const MO_STATEMENT_120 = "statement_info_10200"
const RenameStmtInfo_120 = "alter table `system`.`statement_info` rename `system`.`statement_info_10200`;"

// upg_rename_system_stmt_info_120 do rename table
var upg_rename_system_stmt_info_120 = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM,
	TableName: catalog.MO_STATEMENT,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    RenameStmtInfo_120,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM, MO_STATEMENT_120)
	},
}

const CreateStmtInfo_130 = "CREATE TABLE `system`.`statement_info` (\n  `statement_id` varchar(36) NOT NULL COMMENT 'statement uniq id',\n  `transaction_id` varchar(36) NOT NULL COMMENT 'txn uniq id',\n  `session_id` varchar(36) NOT NULL COMMENT 'session uniq id',\n  `account` varchar(1024) NOT NULL COMMENT 'account name',\n  `user` varchar(1024) NOT NULL COMMENT 'user name',\n  `host` varchar(1024) NOT NULL COMMENT 'user client ip',\n  `database` varchar(1024) NOT NULL COMMENT 'what database current session stay in.',\n  `statement` text NOT NULL COMMENT 'sql statement',\n  `statement_tag` text NOT NULL COMMENT 'note tag in statement(Reserved)',\n  `statement_fingerprint` text NOT NULL COMMENT 'note tag in statement(Reserved)',\n  `node_uuid` varchar(36) NOT NULL COMMENT 'node uuid, which node gen this data.',\n  `node_type` varchar(1024) NOT NULL COMMENT 'node type in MO, val in [DN, CN, LOG]',\n  `request_at` datetime(6) NOT NULL COMMENT 'request accept datetime',\n  `response_at` datetime(6) NOT NULL COMMENT 'response send datetime',\n  `duration` bigint unsigned DEFAULT '0' COMMENT 'exec time, unit: ns',\n  `status` varchar(1024) NOT NULL COMMENT 'sql statement running status, enum: Running, Success, Failed',\n  `err_code` varchar(1024) DEFAULT '0' COMMENT 'error code info',\n  `error` text NOT NULL COMMENT 'error message',\n  `exec_plan` text DEFAULT '{}' COMMENT 'statement execution plan',\n  `rows_read` bigint DEFAULT '0' COMMENT 'rows read total',\n  `bytes_scan` bigint DEFAULT '0' COMMENT 'bytes scan total',\n  `stats` text DEFAULT '[]' COMMENT 'global stats info in exec_plan',\n  `statement_type` varchar(1024) NOT NULL COMMENT 'statement type, val in [Insert, Delete, Update, Drop Table, Drop User, ...]',\n  `query_type` varchar(1024) NOT NULL COMMENT 'query type, val in [DQL, DDL, DML, DCL, TCL]',\n  `role_id` bigint DEFAULT '0' COMMENT 'role id',\n  `sql_source_type` text NOT NULL COMMENT 'sql statement source type',\n  `aggr_count` bigint DEFAULT '0' COMMENT 'the number of statements aggregated',\n  `result_count` bigint DEFAULT '0' COMMENT 'the number of rows of sql execution results'\n) COMMENT='record each statement and stats info[mo_no_del_hint]' CLUSTER BY (`account`, `request_at`)"

// upg_create_system_stmt_info_130 do create new table
var upg_create_system_stmt_info_130 = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM,
	TableName: catalog.MO_STATEMENT,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    CreateStmtInfo_130,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM, catalog.MO_STATEMENT)
	},
}

const MO_Metric_120 = "metric_10200"
const RenameSystemMetrics_Metric_120 = "alter table `system_metrics`.`metric` rename `system_metrics`.`metric_10200`;"

// upg_rename_system_stmt_info_120 do rename table
var upg_rename_system_metrics_metric_120 = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: catalog.MO_METRIC,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    RenameSystemMetrics_Metric_120,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM_METRICS, MO_Metric_120)
	},
}

const CreateSystemMetricsMetric_130 = "CREATE TABLE `system_metrics`.`metric` (\n  `metric_name` varchar(1024) DEFAULT 'sys' COMMENT 'metric name, like: sql_statement_total, server_connections, process_cpu_percent, sys_memory_used, ...',\n  `collecttime` datetime(6) NOT NULL COMMENT 'metric data collect time',\n  `value` double DEFAULT '0.0' COMMENT 'metric value',\n  `node` varchar(1024) DEFAULT 'monolithic' COMMENT 'mo node uuid',\n  `role` varchar(1024) DEFAULT 'monolithic' COMMENT 'mo node role, like: CN, DN, LOG',\n  `account` varchar(1024) DEFAULT 'sys' COMMENT 'account name',\n  `type` varchar(1024) NOT NULL COMMENT 'sql type, like: insert, select, ...'\n) COMMENT='metric data[mo_no_del_hint]' CLUSTER BY (`account`, `metric_name`, `collecttime`)"

// upg_create_system_stmt_info_130 do create new table
var upg_create_system_metrics_metric_130 = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM,
	TableName: catalog.MO_STATEMENT,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    CreateSystemMetricsMetric_130,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM_METRICS, catalog.MO_METRIC)
	},
}

const systemMetricSqlStmtCuComment121 = `sql_statement_cu metric data[mo_no_del_hint]`

func getDDLAlterComment(db, tbl, comment string) string {
	return fmt.Sprintf("ALTER TABLE %s.%s COMMENT %q", db, tbl, comment)
}

var upg_system_metrics_sql_stmt_cu_comment = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: catalog.MO_SQL_STMT_CU,
	UpgType:   versions.MODIFY_TABLE_COMMENT,
	UpgSql:    getDDLAlterComment(catalog.MO_SYSTEM_METRICS, catalog.MO_SQL_STMT_CU, systemMetricSqlStmtCuComment121),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, comment, err := versions.CheckTableComment(txn, accountId, catalog.MO_SYSTEM_METRICS, catalog.MO_SQL_STMT_CU)
		if err != nil {
			return false, err
		}
		if exists && comment == systemMetricSqlStmtCuComment121 {
			return true, nil
		}
		return false, nil
	},
}
