// Copyright 2025 Matrix Origin
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

package v4_0_0

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/predefine"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_mo_iscp_log_new,
	upg_mo_iscp_task,
	upg_mo_index_update_new,
	upg_create_mo_branch_metadata,
	upg_rename_system_stmt_info_4000,
	upg_create_system_stmt_info_4000,
	upg_rename_system_metrics_metric_4000,
	upg_create_system_metrics_metric_4000,
	upg_create_mo_feature_limit,
	upg_alter_mo_pitr,
	upg_create_mo_feature_registry,
	upg_init_mo_feature_registry,
}

var upg_mo_iscp_log_new = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_ISCP_LOG,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoISCPLogDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_ISCP_LOG)
	},
}

var upg_mo_iscp_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysDaemonTask,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    predefine.GenInitISCPTaskSQL(),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		ok, err := versions.CheckTableDataExist(txn, accountId, predefine.GenISCPTaskCheckSQL())
		return ok, err
	},
}

var upg_mo_index_update_new = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_INDEX_UPDATE,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoIndexUpdateDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_INDEX_UPDATE)
	},
}

var upg_create_mo_branch_metadata = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_BRANCH_METADATA,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogBranchMetadataDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA)
		return exist, err
	},
}

var upg_create_mo_feature_limit = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_FEATURE_LIMIT,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogFeatureLimitDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT)
		return exist, err
	},
}

var upg_create_mo_feature_registry = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_FEATURE_REGISTRY,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogFeatureRegistryDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY)
		return exist, err
	},
}

var upg_init_mo_feature_registry = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_FEATURE_REGISTRY,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogFeatureRegistryInitData,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDataExist(txn, accountId, fmt.Sprintf(
			"select * from %s.%s where feature_code in ('SNAPSHOT', 'BRANCH')",
			catalog.MO_CATALOG,
			catalog.MO_FEATURE_REGISTRY,
		))
		return exist, err
	},
}

var upg_alter_mo_pitr = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_PITR,
	UpgType:   versions.ADD_COLUMN,
	UpgSql: fmt.Sprintf(
		"alter table %s.%s add column %s varchar(32) not null default 'user'",
		catalog.MO_CATALOG, catalog.MO_PITR, kind,
	),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		info, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_PITR, "kind")
		if err != nil {
			return false, err
		}

		return info.IsExits, nil
	},
}

const MO_STATEMENT_4000 = "statement_info_4000"
const RenameStmtInfo_4000 = "ALTER TABLE `system`.`statement_info` RENAME `system`.`statement_info_4000`;"

const CreateStmtInfo_4000 = "CREATE TABLE `system`.`statement_info` (\n  `statement_id` varchar(36) NOT NULL COMMENT 'statement uniq id',\n  `transaction_id` varchar(36) NOT NULL COMMENT 'txn uniq id',\n  `session_id` varchar(36) NOT NULL COMMENT 'session uniq id',\n  `account` varchar(1024) NOT NULL COMMENT 'account name',\n  `account_id` int unsigned DEFAULT '0' COMMENT 'account id',\n  `user` varchar(1024) NOT NULL COMMENT 'user name',\n  `host` varchar(1024) NOT NULL COMMENT 'user client ip',\n  `database` varchar(1024) NOT NULL COMMENT 'what database current session stay in.',\n  `statement` text NOT NULL COMMENT 'sql statement',\n  `statement_tag` text NOT NULL COMMENT 'note tag in statement(Reserved)',\n  `statement_fingerprint` text NOT NULL COMMENT 'note tag in statement(Reserved)',\n  `node_uuid` varchar(36) NOT NULL COMMENT 'node uuid, which node gen this data.',\n  `node_type` varchar(1024) NOT NULL COMMENT 'node type in MO, val in [DN, CN, LOG]',\n  `request_at` datetime(6) NOT NULL COMMENT 'request accept datetime',\n  `response_at` datetime(6) NOT NULL COMMENT 'response send datetime',\n  `duration` bigint unsigned DEFAULT '0' COMMENT 'exec time, unit: ns',\n  `status` varchar(1024) NOT NULL COMMENT 'sql statement running status, enum: Running, Success, Failed',\n  `err_code` varchar(1024) DEFAULT '0' COMMENT 'error code info',\n  `error` text NOT NULL COMMENT 'error message',\n  `exec_plan` text DEFAULT '{}' COMMENT 'statement execution plan',\n  `rows_read` bigint DEFAULT '0' COMMENT 'rows read total',\n  `bytes_scan` bigint DEFAULT '0' COMMENT 'bytes scan total',\n  `stats` text DEFAULT '[]' COMMENT 'global stats info in exec_plan',\n  `statement_type` varchar(1024) NOT NULL COMMENT 'statement type, val in [Insert, Delete, Update, Drop Table, Drop User, ...]',\n  `query_type` varchar(1024) NOT NULL COMMENT 'query type, val in [DQL, DDL, DML, DCL, TCL]',\n  `role_id` bigint DEFAULT '0' COMMENT 'role id',\n  `sql_source_type` text NOT NULL COMMENT 'sql statement source type',\n  `aggr_count` bigint DEFAULT '0' COMMENT 'the number of statements aggregated',\n  `result_count` bigint DEFAULT '0' COMMENT 'the number of rows of sql execution results',\n  `connection_id` bigint DEFAULT '0' COMMENT 'connection id',\n  `cu` double DEFAULT '0.0' COMMENT 'cu cost'\n) COMMENT='record each statement and stats info[mo_no_del_hint]' CLUSTER BY (`request_at`, `account_id`)"

// upg_rename_system_stmt_info_4000 do rename table
var upg_rename_system_stmt_info_4000 = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM,
	TableName: catalog.MO_STATEMENT,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    RenameStmtInfo_4000,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM, MO_STATEMENT_4000)
	},
}

// upg_create_system_stmt_info_4000 do create new table
var upg_create_system_stmt_info_4000 = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM,
	TableName: catalog.MO_STATEMENT,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    CreateStmtInfo_4000,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM, catalog.MO_STATEMENT)
	},
}

const MO_METRIC_4000 = "metric_4000"
const RenameMetric_4000 = "ALTER TABLE `system_metrics`.`metric` RENAME `system_metrics`.`metric_4000`;"

const CreateMetric_4000 = "CREATE TABLE `system_metrics`.`metric` (\n  `metric_name` varchar(1024) DEFAULT 'sys' COMMENT 'metric name, like: sql_statement_total, server_connections, process_cpu_percent, sys_memory_used, ...',\n  `collecttime` datetime(6) NOT NULL COMMENT 'metric data collect time',\n  `value` double DEFAULT '0.0' COMMENT 'metric value',\n  `node` varchar(1024) DEFAULT 'monolithic' COMMENT 'mo node uuid',\n  `role` varchar(1024) DEFAULT 'monolithic' COMMENT 'mo node role, like: CN, DN, LOG',\n  `account` varchar(1024) DEFAULT 'sys' COMMENT 'account name',\n  `account_id` int unsigned DEFAULT '0' COMMENT 'account id',\n  `type` varchar(1024) NOT NULL COMMENT 'sql type, like: insert, select, ...'\n) COMMENT='metric data' CLUSTER BY (`collecttime`, `account_id`, `metric_name`)"

// upg_rename_system_metrics_metric_4000 do rename table
var upg_rename_system_metrics_metric_4000 = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: catalog.MO_METRIC,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    RenameMetric_4000,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM_METRICS, MO_METRIC_4000)
	},
}

// upg_create_system_metrics_metric_4000 do create new table
var upg_create_system_metrics_metric_4000 = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: catalog.MO_METRIC,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    CreateMetric_4000,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM_METRICS, catalog.MO_METRIC)
	},
}
