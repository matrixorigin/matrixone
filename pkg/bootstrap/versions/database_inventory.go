// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package versions

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	DatabaseInventoryPageSize = 128
	// Two pending-catalog migration effects: create the table and fill it.
	DatabaseInventoryUpgradeStages = 2
)

type inventoryUpgradeCommitObserver func(context.Context, int32, string)

var inventoryUpgradeCommitted atomic.Pointer[inventoryUpgradeCommitObserver]

// SetInventoryUpgradeCommittedHookForTest observes confirmed owner commits, not
// statement completion. Exclusive fixtures must restore it after their worker joins.
func SetInventoryUpgradeCommittedHookForTest(hook func(context.Context, int32, string)) func() {
	observer := inventoryUpgradeCommitObserver(hook)
	previous := inventoryUpgradeCommitted.Swap(&observer)
	return func() { inventoryUpgradeCommitted.Store(previous) }
}

func observeInventoryUpgradeCommit(ctx context.Context, accountID int32, operation string) {
	if observer := inventoryUpgradeCommitted.Load(); observer != nil && *observer != nil {
		(*observer)(ctx, accountID, operation)
	}
}

// UpgradeDatabaseInventory uses the enclosing tenant upgrade's transaction
// owner, before any ordinary user-table DDL can reach inventory write guards.
// No cursor or readiness derived from unconfirmed work escapes this function.
func UpgradeDatabaseInventory(ctx context.Context, accountID int32, objectLimit uint64,
	run func(string, uint64, bool, func(executor.TxnExecutor) error) (bool, error)) (bool, error) {
	if accountID < 0 || objectLimit == 0 || run == nil {
		return false, moerr.NewInvalidInput(ctx, "invalid database inventory upgrade owner")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	account := uint32(accountID)
	ddl := strings.Replace(catalog.MoDatabaseInventoryDDL, "create table ", "create table if not exists ", 1)
	active, err := run("create database inventory", objectLimit, true, func(tx executor.TxnExecutor) error {
		result, err := tx.Exec(ddl, UpgradeStatementOption(account))
		if err != nil {
			return err
		}
		result.Close()
		return nil
	})
	if err != nil || !active {
		return false, err
	}
	observeInventoryUpgradeCommit(ctx, accountID, "create database inventory")
	var after uint64
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		next := after
		var more bool
		active, err := run("backfill database inventory", CatalogUpgradePageBytes, false, func(tx executor.TxnExecutor) error {
			var err error
			next, more, err = BackfillDatabaseInventoryPage(ctx, tx, account, after)
			return err
		})
		if err != nil || !active {
			return false, err
		}
		observeInventoryUpgradeCommit(ctx, accountID, "backfill database inventory")
		after = next
		if !more {
			return true, nil
		}
	}
}

// BackfillDatabaseInventoryPage borrows a shutdown-upgrade transaction after
// catalog creation. The upgrade owner excludes external DDL and publishes the
// returned cursor only after commit. Restarting at zero is idempotent; this is
// not a runtime fallback for missing inventory rows or a tenant-ready marker.
func BackfillDatabaseInventoryPage(ctx context.Context, tx executor.TxnExecutor, account uint32, after uint64) (next uint64, more bool, err error) {
	if err := ctx.Err(); err != nil {
		return after, false, err
	}
	excluded := make([]string, len(catalog.SystemDatabases))
	for i, name := range catalog.SystemDatabases {
		excluded[i] = sqlquote.String(strings.ToLower(name))
	}
	query := fmt.Sprintf("select cast(dat_id as bigint unsigned) from mo_catalog.mo_database where account_id=%d and dat_id>%d and lower(datname) not in (%s) order by dat_id limit %d",
		account, after, strings.Join(excluded, ","), DatabaseInventoryPageSize)
	result, err := tx.Exec(query, UpgradeStatementOption(account))
	if err != nil {
		return after, false, err
	}
	ids := make([]string, 0, DatabaseInventoryPageSize)
	last := after
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) != 1 || cols[0] == nil || cols[0].GetType().Oid != types.T_uint64 || rows > DatabaseInventoryPageSize-len(ids) {
			err = moerr.NewInternalError(ctx, "invalid database inventory upgrade page")
			return false
		}
		for i := 0; i < rows; i++ {
			if cols[0].IsNull(uint64(i)) {
				err = moerr.NewInternalError(ctx, "null database inventory upgrade identity")
				return false
			}
			id := vector.GetFixedAtWithTypeCheck[uint64](cols[0], i)
			if id <= last {
				err = moerr.NewInternalError(ctx, "unordered database inventory upgrade identity")
				return false
			}
			ids = append(ids, strconv.FormatUint(id, 10))
			last = id
		}
		return true
	})
	result.Close()
	if err != nil {
		return after, false, err
	}
	if len(ids) == 0 {
		return after, false, nil
	}
	if err := ctx.Err(); err != nil {
		return after, false, err
	}
	// Never REPLACE or reset an existing counter, including on page replay.
	query = fmt.Sprintf("insert into mo_catalog.mo_database_inventory(account_id,database_id,epoch) select d.account_id,d.dat_id,1 from mo_catalog.mo_database d left join mo_catalog.mo_database_inventory i on i.account_id=d.account_id and i.database_id=d.dat_id where d.account_id=%d and d.dat_id in (%s) and i.database_id is null",
		account, strings.Join(ids, ","))
	written, err := tx.Exec(query, UpgradeStatementOption(account))
	if err != nil {
		return after, false, err
	}
	affected := written.AffectedRows
	written.Close()
	if affected > uint64(len(ids)) {
		return after, false, moerr.NewInternalError(ctx, "database inventory backfill exceeded its page")
	}
	return last, len(ids) == DatabaseInventoryPageSize, nil
}
