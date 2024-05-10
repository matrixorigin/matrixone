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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_sys_modify_async_task,
	upg_create_index1_async_task,
	upg_create_index2_async_task,
	upg_create_index3_async_task,
	upg_create_index4_async_task,
	upg_create_index1_daemon_task,
	upg_create_index2_daemon_task,
	upg_mo_role_privs,
	upg_mo_debug_eventTxnTable,
	upg_mo_debug_eventDataTable,
	upg_mo_debug_traceTableFilterTable,
	upg_mo_debug_traceTxnFilterTable,
	upg_mo_debug_traceStatementFilterTable,
	upg_mo_debug_eventErrorTable,
	upg_mo_debug_traceStatementTable,
	upg_mo_debug_eventTxnActionTable,
	upg_mo_debug_featuresTables,
	upg_mo_account,
}

var upg_sys_modify_async_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.MODIFY_COLUMN,
	UpgSql:    "alter table `mo_task`.`sys_async_task` modify task_id bigint auto_increment",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "task_id")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			if strings.EqualFold(colInfo.ColType, versions.T_int64) {
				return true, nil
			}
		}
		return false, nil
	},
}

// ------------------------------------------------------------------------------------------------------------
var upg_create_index1_async_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.ADD_INDEX,
	UpgSql:    fmt.Sprintf(`create index idx_task_status on %s.%s(task_status)`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_status")
	},
}

var upg_create_index2_async_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.ADD_INDEX,
	UpgSql:    fmt.Sprintf(`create index idx_task_runner on %s.%s(task_runner)`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_runner")
	},
}

var upg_create_index3_async_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.ADD_INDEX,
	UpgSql:    fmt.Sprintf(`create index idx_task_executor on %s.%s(task_metadata_executor)`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_executor")
	},
}

var upg_create_index4_async_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.ADD_INDEX,
	UpgSql:    fmt.Sprintf(`create index idx_task_epoch on %s.%s(task_epoch)`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_epoch")
	},
}

var upg_create_index1_daemon_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysDaemonTask,
	UpgType:   versions.ADD_INDEX,
	UpgSql:    fmt.Sprintf(`create index idx_account_id on %s.%s(account_id)`, catalog.MOTaskDB, catalog.MOSysDaemonTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysDaemonTask, "idx_account_id")
	},
}

var upg_create_index2_daemon_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysDaemonTask,
	UpgType:   versions.ADD_INDEX,
	UpgSql:    fmt.Sprintf(`create index idx_last_heartbeat on %s.%s(last_heartbeat)`, catalog.MOTaskDB, catalog.MOSysDaemonTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysDaemonTask, "idx_last_heartbeat")
	},
}

// ------------------------------------------------------------------------------------------------------------
var upg_mo_role_privs = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_role_privs",
	UpgType:   versions.MODIFY_METADATA,
	UpgSql:    frontend.GenSQLForInsertUpgradeAccountPrivilege(),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		sql := frontend.GenSQLForCheckUpgradeAccountPrivilegeExist()
		return versions.CheckTableDataExist(txn, accountId, sql)
	},
}

var upg_mo_debug_eventTxnTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.EventTxnTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    trace.EventTxnTableSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.EventTxnTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_eventDataTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.EventDataTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    trace.EventDataTableSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.EventDataTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_traceTableFilterTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.TraceTableFilterTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    trace.TraceTableFilterTableSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.TraceTableFilterTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_traceTxnFilterTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.TraceTxnFilterTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    trace.TraceTxnFilterTableSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.TraceTxnFilterTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_traceStatementFilterTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.TraceStatementFilterTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    trace.TraceStatementFilterTableSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.TraceStatementFilterTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_eventErrorTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.EventErrorTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    trace.EventErrorTableSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.EventErrorTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_traceStatementTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.TraceStatementTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    trace.TraceStatementTableSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.TraceStatementTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_eventTxnActionTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.EventTxnActionTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    trace.EventTxnActionTableSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.EventTxnActionTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_featuresTables = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.FeaturesTables,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    trace.FeaturesTablesSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.FeaturesTables)
	},
	PostSql: fmt.Sprintf(`insert into %s.%s (name, state) values 
			('%s', '%s'), ('%s', '%s'), ('%s', '%s'), ('%s', '%s'), ('%s', '%s')`,
		trace.DebugDB, trace.FeaturesTables,
		trace.FeatureTraceTxn, trace.StateDisable,
		trace.FeatureTraceTxnAction, trace.StateDisable,
		trace.FeatureTraceData, trace.StateDisable,
		trace.FeatureTraceStatement, trace.StateDisable,
		trace.FeatureTraceTxnWorkspace, trace.StateDisable),
}

var upg_mo_account = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MOAccountTable,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_account add column admin_name varchar(300) after account_name",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MOAccountTable, "admin_name")
		if err != nil {
			return false, err
		}
		return colInfo.IsExits, nil
	},
	PostSql: "update mo_account set admin_name = mo_admin_name(account_id)",
}
