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
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/predefine"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// retiredKafkaSinkTaskCode is the wire value formerly assigned to
// TaskCode_ConnectorKafkaSink. The protobuf value is reserved, so persisted
// daemon tasks can still be identified without restoring the removed feature.
const retiredKafkaSinkTaskCode = 4

var clusterUpgEntries = append(
	[]versions.UpgradeEntry{
		retireKafkaSinkDaemonTasks,
		createMoViewDependencies,
		createMoViewRefresh,
	},
	makeLifecycleClusterUpgradeEntries()...,
)

var createMoViewDependencies = newViewMetadataCatalogTable(
	catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesDDL)

var createMoViewRefresh = newViewMetadataCatalogTable(
	catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshDDL)

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

func makeLifecycleClusterUpgradeEntries() []versions.UpgradeEntry {
	entries := make([]versions.UpgradeEntry, 0, len(catalog.LifecycleClusterTableDefinitions)+2)
	for _, table := range catalog.LifecycleClusterTableDefinitions {
		entries = append(entries, versions.UpgradeEntry{
			Schema:    table.Schema,
			TableName: table.Name,
			UpgType:   versions.CREATE_NEW_TABLE,
			UpgSql:    table.DDL,
			CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
				return versions.CheckTableDefinition(txn, accountID, table.Schema, table.Name)
			},
		})
	}
	entries = append(entries, versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: catalog.MO_FEATURE_REGISTRY,
		UpgType:   versions.MODIFY_METADATA,
		UpgSql:    frontend.MoCatalogLifecycleFeatureRegistryInitData,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDataExist(
				txn,
				accountID,
				"select feature_code from mo_catalog.mo_feature_registry where feature_code = 'LIFECYCLE'",
			)
		},
	})
	cronSQL, err := predefine.GenInitCronTaskSQL(int32(task.TaskCode_LifecycleCoordinator))
	if err != nil {
		panic(fmt.Sprintf("build Lifecycle coordinator upgrade SQL: %v", err))
	}
	entries = append(entries, versions.UpgradeEntry{
		Schema:    catalog.MOTaskDB,
		TableName: "sys_cron_task",
		UpgType:   versions.MODIFY_METADATA,
		UpgSql: cronSQL +
			" on duplicate key update task_metadata_id=task_metadata_id",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDataExist(
				txn,
				accountID,
				fmt.Sprintf(
					`select task_metadata_id from %s.sys_cron_task where task_metadata_id='tae_object_lifecycle' and task_metadata_executor=%d`,
					catalog.MOTaskDB,
					task.TaskCode_LifecycleCoordinator,
				),
			)
		},
	})
	return entries
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
