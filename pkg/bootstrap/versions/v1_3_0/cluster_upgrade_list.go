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
		if exists || err != nil {
			return false, err
		}
		return true, nil
	},
}

var upg_drop_idx_task_runner = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.DROP_INDEX,
	UpgSql:    fmt.Sprintf(`ALTER TABLE %s.%s DROP INDEX idx_task_runner`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, err := versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_runner")
		if exists || err != nil {
			return false, err
		}
		return true, nil
	},
}

var upg_drop_idx_task_executor = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.DROP_INDEX,
	UpgSql:    fmt.Sprintf(`ALTER TABLE %s.%s DROP INDEX idx_task_executor`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, err := versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_executor")
		if exists || err != nil {
			return false, err
		}
		return true, nil
	},
}

var upg_drop_idx_task_epoch = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.DROP_INDEX,
	UpgSql:    fmt.Sprintf(`ALTER TABLE %s.%s DROP INDEX idx_task_epoch`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, err := versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "idx_task_epoch")
		if exists || err != nil {
			return false, err
		}
		return true, nil
	},
}

var upg_drop_task_metadata_id = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.DROP_INDEX,
	UpgSql:    fmt.Sprintf(`ALTER TABLE %s.%s DROP INDEX task_metadata_id`, catalog.MOTaskDB, catalog.MOSysAsyncTask),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, err := versions.CheckIndexDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "task_metadata_id")
		if exists || err != nil {
			return false, err
		}
		return true, nil
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
