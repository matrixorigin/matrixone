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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestPrivateAlterCopyRejectsWrongOwner(t *testing.T) {
	_, db, txn, ctx := newResultOwnershipFixture(t, &inventoryOwnerExecutor{}, false)
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		database string
		table    string
		ownerNil bool
	}{
		{name: "wrong database", ctx: ctx, database: "other", table: "copy"},
		{name: "wrong tenant", ctx: defines.AttachAccountId(ctx, 2), database: "db", table: "copy"},
		{name: "empty table", ctx: ctx, database: "db"},
		{name: "missing owner", ctx: ctx, database: "db", table: "copy", ownerNil: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := txn.op
			if tc.ownerNil {
				owner = nil
			}
			handled, err := TryCreatePrivateAlterCopy(tc.ctx, db, owner, tc.database, tc.table, nil)
			require.True(t, handled)
			require.Error(t, err)
			require.Empty(t, txn.tableOps.privateInventories)
		})
	}
	handled, err := TryCreatePrivateAlterCopy(ctx, nil, txn.op, "db", "copy", nil)
	require.False(t, handled, "other engines retain their ordinary Create path")
	require.NoError(t, err)
	var nilDB *txnDatabase
	handled, err = TryCreatePrivateAlterCopy(ctx, nilDB, txn.op, "db", "copy", nil)
	require.True(t, handled)
	require.Error(t, err)
}

func TestPrivateInventoryFenceCommitAndRollback(t *testing.T) {
	txn := &Transaction{tableOps: newTableOps()}
	txn.readOnly.Store(true)
	txn.tableOps.addPrivateInventory(7, 41, 1)
	txn.tableOps.addPrivateInventory(7, 41, 1)
	_, err := txn.Commit(context.Background())
	require.Error(t, err, "unfenced private relations cannot commit")

	txn.tableOps.fencePrivateInventory(8, 41, 2)
	txn.tableOps.fencePrivateInventory(7, 42, 2)
	_, err = txn.Commit(context.Background())
	require.Error(t, err, "another account or database cannot discharge the obligation")

	txn.tableOps.fencePrivateInventory(7, 41, 2)
	_, err = txn.Commit(context.Background())
	require.NoError(t, err)

	txn.tableOps.rollbackLastStatement(2)
	_, err = txn.Commit(context.Background())
	require.Error(t, err, "rolling back the fencing statement must restore the obligation")
	txn.tableOps.rollbackLastStatement(1)
	_, err = txn.Commit(context.Background())
	require.NoError(t, err, "rolling back the private create removes its obligation")
}

func TestPrivateInventoryKeepsEarlierFenceAcrossLaterRollback(t *testing.T) {
	txn := &Transaction{tableOps: newTableOps()}
	txn.tableOps.addPrivateInventory(7, 41, 1)
	txn.tableOps.fencePrivateInventory(7, 41, 2)
	txn.tableOps.fencePrivateInventory(7, 41, 3)
	txn.tableOps.rollbackLastStatement(3)
	require.NoError(t, txn.checkPrivateInventoryFences(context.Background()))
	txn.tableOps.rollbackLastStatement(2)
	require.Error(t, txn.checkPrivateInventoryFences(context.Background()))
}
