// Copyright 2026 Matrix Origin
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

package v4_0_5

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/predefine"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = makeLifecycleClusterUpgradeEntries()

func makeLifecycleClusterUpgradeEntries() []versions.UpgradeEntry {
	entries := make([]versions.UpgradeEntry, 0, len(catalog.LifecycleClusterTableDefinitions)+1)
	for _, def := range catalog.LifecycleClusterTableDefinitions {
		table := def
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
	cronSQL, err := predefine.GenInitCronTaskSQL(
		int32(task.TaskCode_LifecycleCoordinator),
	)
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
