// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// retiredKafkaSinkTaskCode is the wire value formerly assigned to
// TaskCode_ConnectorKafkaSink. The protobuf value is reserved, so persisted
// daemon tasks can still be identified without restoring the removed feature.
const retiredKafkaSinkTaskCode = 4

var clusterUpgEntries = []versions.UpgradeEntry{
	retireKafkaSinkDaemonTasks,
	createMoViewDependencies,
	createMoViewRefresh,
	addMoViewDependenciesTargetNameIndex,
	addMoViewRefreshTargetNameIndex,
	seedViewMetadataRevalidation,
	addSQLTaskAccountIndex,
	addSQLTaskRunAccountIndex,
	addAsyncTaskParentIndex,
	cleanupLegacyOrphanSQLTaskChildren,
}

var addSQLTaskAccountIndex = newTaskMetadataIndex(
	catalog.MOSQLTask, "idx_account_id", "account_id")

var addSQLTaskRunAccountIndex = newTaskMetadataIndex(
	catalog.MOSQLTaskRun, "idx_account_id", "account_id")

var addAsyncTaskParentIndex = newTaskMetadataIndex(
	catalog.MOSysAsyncTask, "idx_task_parent_id", "task_parent_id")

const legacyOrphanSQLTaskChildPredicate = "task_parent_id like 'sql-task:%' and task_parent_id not in (" +
	"select concat('sql-task:', task_id) from mo_task.sql_task " +
	"union select concat('sql-task:', task_id) from mo_task.sql_task_run)"

var cleanupLegacyOrphanSQLTaskChildren = versions.UpgradeEntry{
	Schema: catalog.MOTaskDB, TableName: catalog.MOSysAsyncTask, UpgType: versions.MODIFY_METADATA,
	UpgSql: fmt.Sprintf("delete from %s.%s where %s", catalog.MOTaskDB, catalog.MOSysAsyncTask,
		legacyOrphanSQLTaskChildPredicate),
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		exists, err := versions.CheckTableDataExist(txn, accountID, fmt.Sprintf(
			"select 1 from %s.%s where %s limit 1", catalog.MOTaskDB,
			catalog.MOSysAsyncTask, legacyOrphanSQLTaskChildPredicate))
		if err != nil || exists {
			return false, err
		}
		if err := versions.CheckCommonProtocolVersion(txn, defines.MORPCVersion42); err != nil {
			return false, err
		}
		return true, nil
	},
	RequiredProtocolVersion: defines.MORPCVersion42,
}

func newTaskMetadataIndex(tableName, indexName, columnName string) versions.UpgradeEntry {
	return versions.UpgradeEntry{Schema: catalog.MOTaskDB, TableName: tableName, UpgType: versions.ADD_INDEX,
		UpgSql: fmt.Sprintf("create index %s on %s.%s(%s)", indexName, catalog.MOTaskDB, tableName, columnName),
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckIndexDefinition(txn, accountID, catalog.MOTaskDB, tableName, indexName)
		}}
}

var createMoViewDependencies = newViewMetadataCatalogTable(
	catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesDDL)

var createMoViewRefresh = newViewMetadataCatalogTable(
	catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshDDL)

var addMoViewDependenciesTargetNameIndex = newViewMetadataTargetNameIndex(
	catalog.MO_VIEW_DEPENDENCIES, "idx_view_dependency_target_name")

var addMoViewRefreshTargetNameIndex = newViewMetadataTargetNameIndex(
	catalog.MO_VIEW_REFRESH, "idx_view_refresh_target_name")

func newViewMetadataTargetNameIndex(tableName, indexName string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: tableName,
		UpgType:   versions.MODIFY_METADATA,
		UpgSql: fmt.Sprintf("alter table %s.%s add index %s("+
			"account_id,target_database_name(256),target_relation_name(256))",
			catalog.MO_CATALOG, tableName, indexName),
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckIndexDefinition(
				txn, accountID, catalog.MO_CATALOG, tableName, indexName)
		},
	}
}

var seedViewMetadataRevalidation = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_view_metadata_revalidation",
	UpgType:   versions.MODIFY_METADATA,
	UpgSql: fmt.Sprintf(
		"replace into %s.%s (%s) select a.account_id,0,0,0,'%s','%s',0,0,0,0,0,"+
			"'','','','','%s','',0,null,0,1 from %s.%s a",
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesColumns,
		catalog.LegacyViewScanCursorDatabase, catalog.LegacyViewScanCursorRelation,
		catalog.ViewRefreshStatusRevalidateScan,
		catalog.MO_CATALOG, catalog.MOAccountTable),
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		return versions.CheckTableDataExist(txn, accountID, fmt.Sprintf(
			"select 1 from %s.%s where account_id=0 and target_relation_id=0 "+
				"and dependency_ordinal=0 and source_relation_kind='%s' limit 1",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
			catalog.ViewRefreshStatusRevalidateScan))
	},
}

func newViewMetadataCatalogTable(name, ddl string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: name,
		UpgType:   versions.CREATE_NEW_TABLE,
		UpgSql:    ddl,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountID, catalog.MO_CATALOG, name)
		},
	}
}

var retireKafkaSinkDaemonTasks = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysDaemonTask,
	UpgType:   versions.MODIFY_METADATA,
	UpgSql: fmt.Sprintf(
		"update %s.%s set task_status = %d, update_at = current_timestamp() where %s",
		catalog.MOTaskDB,
		catalog.MOSysDaemonTask,
		task.TaskStatus_CancelRequested,
		activeKafkaSinkTaskFilter(),
	),
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		exists, err := versions.CheckTableDataExist(
			txn,
			accountID,
			fmt.Sprintf(
				"select 1 from %s.%s where %s limit 1",
				catalog.MOTaskDB,
				catalog.MOSysDaemonTask,
				activeKafkaSinkTaskFilter(),
			),
		)
		return !exists, err
	},
}

// activeKafkaSinkTaskFilter deliberately enumerates known non-terminal states.
// Unknown future states and historical terminal rows must not be rewritten by
// a compatibility migration.
func activeKafkaSinkTaskFilter() string {
	return fmt.Sprintf(
		"task_metadata_executor = %d and task_status in (%d, %d, %d, %d, %d, %d)",
		retiredKafkaSinkTaskCode,
		task.TaskStatus_Created,
		task.TaskStatus_Running,
		task.TaskStatus_Paused,
		task.TaskStatus_ResumeRequested,
		task.TaskStatus_PauseRequested,
		task.TaskStatus_RestartRequested,
	)
}
