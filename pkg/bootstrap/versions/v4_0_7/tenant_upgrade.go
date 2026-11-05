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

package v4_0_7

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// TenantUpgradeRunner owns the transaction. false means another worker already
// completed this account. A successful return makes the next scan cursor usable.
type TenantUpgradeRunner func(operation string, limit uint64, adjustable bool, work func(executor.TxnExecutor) error) (bool, error)

// UpgradeTenant runs the same bounded sequence for background, manual and login
// upgrades. Supplied transactions retain atomicity; service-owned transactions
// commit each operation. No durable progress exists beyond the account version.
func (v *versionHandle) UpgradeTenant(ctx context.Context, accountID int32, objectLimit uint64, run TenantUpgradeRunner) (bool, error) {
	for index := 0; index < v.TenantUpgradeEntryCount(); index++ {
		var cursor TenantPageCursor
		for {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			next := cursor
			var more bool
			active, err := run(fmt.Sprintf("entry %d", index), objectLimit, true, func(tx executor.TxnExecutor) error {
				var err error
				more, err = v.UpgradeTenantEntryPage(ctx, accountID, index, tx, objectLimit, &next)
				return err
			})
			if err != nil || !active {
				return false, err
			}
			cursor = next
			if !more {
				break
			}
		}
	}
	var after CatalogTableKey
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var tables []CatalogTableKey
		active, err := run("enumerating foreign-key tables", versions.CatalogUpgradePageBytes, false, func(tx executor.TxnExecutor) error {
			var err error
			tables, err = v.ListForeignKeyTables(tx, accountID, after, versions.CatalogUpgradePageBytes)
			return err
		})
		if err != nil || !active {
			return false, err
		}
		for _, table := range tables {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			active, err = run("foreign-key table "+table.Database+"."+table.Table, objectLimit, true, func(tx executor.TxnExecutor) error {
				return v.UpgradeForeignKeyTable(ctx, accountID, tx, table, objectLimit)
			})
			if err != nil || !active {
				return false, err
			}
			after = table
		}
		if len(tables) < CatalogDataPageSize {
			break
		}
	}
	var afterIndex IndexMetadataKey
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var tables []IndexMetadataKey
		active, err := run("enumerating index metadata tables", versions.CatalogUpgradePageBytes, false, func(tx executor.TxnExecutor) error {
			var err error
			tables, err = v.ListIndexMetadataTables(tx, uint32(accountID), afterIndex, versions.CatalogUpgradePageBytes)
			return err
		})
		if err != nil || !active {
			return false, err
		}
		for _, table := range tables {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			active, err = run("index metadata table "+table[0]+"."+table[1], objectLimit, true, func(tx executor.TxnExecutor) error {
				return v.UpgradeIndexMetadataTable(ctx, accountID, limitedCatalogTxn{TxnExecutor: tx, limit: objectLimit}, table)
			})
			if err != nil || !active {
				return false, err
			}
			afterIndex = table
		}
		if len(tables) < CatalogDataPageSize {
			break
		}
	}
	return true, nil
}
