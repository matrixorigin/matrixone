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
		for _, upgEntry := range upgradeEntries {
			err = upgEntry.Upgrade(txn)
			if err != nil {
				return err
			}
		}
	}
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
		fmt.Printf("-------------createFrameworkTables: sql: %s\n", sql)
		r, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		r.Close()
	}
	return nil
}
