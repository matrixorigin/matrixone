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
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var (
	Handler = &versionHandle{
		metadata: versions.Version{
			Version:           "1.2.0",
			MinUpgradeVersion: "1.1.0",
			UpgradeCluster:    versions.Yes,
			UpgradeTenant:     versions.No,
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
	txn.Use(catalog.MO_CATALOG)
	created, err := versions.IsFrameworkTablesCreated(txn)
	if err != nil {
		return err
	}

	// create new upgrade framework tables for the first time,
	// which means using v1.2.0 for the first time
	if !created {
		// Many cn maybe create framework tables parallel, only one can create success.
		// Just return error, and upgrade framework will retry.
		err = v.createFrameworkTables(txn, final)
		if err != nil {
			return err
		}

		// First version as a genesis version, always need to be PREPARE.
		// Because the first version need to init upgrade framework tables.
		for _, upgEntry := range clusterUpgEntries {
			err = upgEntry.Upgrade(txn, catalog.System_Account)
			if err != nil {
				return err
			}
		}
	}

	// get all tenantIDs in mo system, including system tenants
	//tenants, err := versions.FetchAllTenants(txn)
	//if err != nil {
	//	return err
	//}
	//for _, tenantID := range tenants {
	//	//ctx = defines.AttachAccountId(ctx, uint32(tenantID))
	//	for _, upgEntry := range tenantUpgEntries {
	//		err = upgEntry.Upgrade(txn, uint32(tenantID))
	//		if err != nil {
	//			return err
	//		}
	//	}
	//}
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(
	ctx context.Context,
	tenantID int32,
	txn executor.TxnExecutor) error {

	//ctx = defines.AttachAccountId(ctx, uint32(tenantID))

	return nil
}

func (v *versionHandle) HandleClusterUpgrade(
	ctx context.Context,
	txn executor.TxnExecutor) error {
	if err := v.handleCreateTxnTrace(txn); err != nil {
		return err
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
