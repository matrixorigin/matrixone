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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

var (
	Handler = &versionHandle{
		metadata: versions.Version{
			Version:           "1.2.0",
			MinUpgradeVersion: "1.1.0",
			UpgradeCluster:    versions.Yes,
			UpgradeTenant:     versions.Yes,
			VersionOffset:     uint32(len(clusterUpgEntries) + len(tenantUpgEntries)),
		},
	}
)

type versionHandle struct {
	metadata versions.Version
}

func (v *versionHandle) Metadata() versions.Version {
	return v.metadata
}

func (v *versionHandle) Prepare(
	ctx context.Context,
	txn executor.TxnExecutor,
	final bool) error {
	/*
		txn.Use(catalog.MO_CATALOG)
		created, err := versions.IsFrameworkTablesCreated(txn)
		if err != nil {
			return err
		}

		// get all tenantIDs in mo system, including system tenants
		tenants, err := versions.FetchAllTenants(txn)
		if err != nil {
			getLogger().Error("fetch all account ids error", zap.Error(err))
			return err
		}

		// create new upgrade framework tables for the first time,
		// which means using v1.2.0 for the first time
		if !created {
			//-------------------------------1. all tenants prepare process-------------------------------------
			for _, tenantID := range tenants {
				// NOTE: The `alter table` statements used for upgrading system table rely on `mo_foreign_keys`,
				// so preprocessing is performed first
				for _, upgEntry := range TenantUpgPrepareEntres {
					err = upgEntry.Upgrade(txn, uint32(tenantID))
					if err != nil {
						getLogger().Error("account prepare upgrade entry execute error", zap.Error(err), zap.Int32("AccountId", tenantID), zap.String("upgrade entry", upgEntry.String()))
						return err
					}
				}
			}

			// Many cn maybe create framework tables parallel, only one can create success.
			// Just return error, and upgrade framework will retry.
			err = v.createFrameworkTables(txn, final)
			if err != nil {
				getLogger().Error("create upgrade FrameworkTables error", zap.Error(err))
				return err
			}
			getLogger().Info("create upgrade FrameworkTables success")
		}

		//-------------------------------2. cluster metadata upgrade----------------------------------------
		// First version as a genesis version, always need to be PREPARE.
		// Because the first version need to init upgrade framework tables.
		for _, upgEntry := range clusterUpgEntries {
			err = upgEntry.Upgrade(txn, catalog.System_Account)
			if err != nil {
				getLogger().Error("cluster upgrade entry execute error", zap.Error(err), zap.String("upgrade entry", upgEntry.String()))
				return err
			}
		}
		getLogger().Info("cluster upgrade success", zap.String("version", "1.2.0"))

		// ------------------------------3. tenant metadata upgrade----------------------------------------
		for _, tenantID := range tenants {
			for _, upgEntry := range tenantUpgEntries {
				err = upgEntry.Upgrade(txn, uint32(tenantID))
				if err != nil {
					getLogger().Error("account upgrade entry execute error", zap.Error(err), zap.Int32("AccountId", tenantID), zap.String("upgrade entry", upgEntry.String()))
					return err
				}
			}
		}
		getLogger().Info("upgrade all account success", zap.String("version", "1.2.0"))
	*/
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(
	ctx context.Context,
	tenantID int32,
	txn executor.TxnExecutor) error {

	for _, upgEntry := range tenantUpgEntries {
		err := upgEntry.Upgrade(txn, uint32(tenantID))
		if err != nil {
			getLogger().Error("account upgrade entry execute error", zap.Error(err), zap.Int32("AccountId", tenantID), zap.String("upgrade entry", upgEntry.String()))
			return err
		}
	}

	return nil
}

func (v *versionHandle) HandleClusterUpgrade(
	ctx context.Context,
	txn executor.TxnExecutor) error {
	//if err := handleCreateIndexesForTaskTables(ctx, txn); err != nil {
	//	return err
	//}
	//if err := v.handleCreateTxnTrace(txn); err != nil {
	//	return err
	//}
	txn.Use(catalog.MO_CATALOG)
	for _, upgEntry := range clusterUpgEntries {
		err := upgEntry.Upgrade(txn, catalog.System_Account)
		if err != nil {
			getLogger().Error("cluster upgrade entry execute error", zap.Error(err), zap.String("upgrade entry", upgEntry.String()))
			return err
		}
	}
	return nil
}

func (v *versionHandle) createFrameworkTables(
	txn executor.TxnExecutor,
	final bool) error {
	values := versions.FrameworkInitSQLs
	if final {
		values = append(values, v.metadata.GetInsertSQL(versions.StateReady))
	}

	for _, sql := range values {
		r, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		r.Close()
	}
	return nil
}

func handleCreateIndexesForTaskTables(ctx context.Context,
	txn executor.TxnExecutor) error {
	result, err := txn.Exec(`show indexes in mo_task.sys_async_task;`, executor.StatementOption{})
	if err != nil {
		return err
	}
	defer result.Close()
	hasIndex := false
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		hasIndex = true
		return false
	})
	if hasIndex {
		return nil
	}

	indexSqls := []string{
		fmt.Sprintf(`create index idx_task_status on %s.sys_async_task(task_status)`,
			catalog.MOTaskDB),
		fmt.Sprintf(`create index idx_task_runner on %s.sys_async_task(task_runner)`,
			catalog.MOTaskDB),
		fmt.Sprintf(`create index idx_task_executor on %s.sys_async_task(task_metadata_executor)`,
			catalog.MOTaskDB),
		fmt.Sprintf(`create index idx_task_epoch on %s.sys_async_task(task_epoch)`,
			catalog.MOTaskDB),
		fmt.Sprintf(`create index idx_account_id on %s.sys_daemon_task(account_id)`,
			catalog.MOTaskDB),
		fmt.Sprintf(`create index idx_last_heartbeat on %s.sys_daemon_task(last_heartbeat)`,
			catalog.MOTaskDB),
	}
	for _, sql := range indexSqls {
		r, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		r.Close()
	}
	return nil
}

func (v *versionHandle) handleCreateTxnTrace(txn executor.TxnExecutor) error {
	txn.Use(catalog.MO_CATALOG)
	res, err := txn.Exec("show databases", executor.StatementOption{})
	if err != nil {
		return err
	}
	completed := false
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			if cols[0].GetStringAt(i) == trace.DebugDB {
				completed = true
			}
		}
		return true
	})
	res.Close()

	if completed {
		return nil
	}

	for _, sql := range trace.InitSQLs {
		res, err = txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		res.Close()
	}
	return nil
}
