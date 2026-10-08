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

package disttae

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTombstoneTransferCandidates(t *testing.T) {
	bat := newWorkspaceDeleteBatch(t, []types.Rowid{{}})
	bat.SetRowCount(1)
	empty := batch.NewWithSize(0)
	base := Entry{typ: DELETE, bat: bat, tableId: catalog.MO_COLUMNS_ID,
		databaseId: catalog.MO_CATALOG_ID, databaseName: catalog.MO_CATALOG,
		tableName: catalog.MO_COLUMNS_UPDATE}
	txn := &Transaction{workspace: newTxnWorkspace()}
	tables, err := txn.collectTombstoneTransferTablesLocked()
	require.NoError(t, err)
	require.Nil(t, tables)
	for _, modify := range []func(*Entry){
		func(e *Entry) { e.typ = INSERT },
		func(e *Entry) { e.bat = nil },
		func(e *Entry) { e.bat = empty },
		func(e *Entry) { e.skipTransfer = true },
	} {
		e := base
		modify(&e)
		txn.appendWorkspaceEntryLocked(e)
	}
	tables, err = txn.collectTombstoneTransferTablesLocked()
	require.NoError(t, err)
	require.Nil(t, tables)
	txn.appendWorkspaceEntryLocked(base)
	txn.appendWorkspaceEntryLocked(base)
	persisted := base
	persisted.fileName = "tombstone"
	persisted.tableName = catalog.MO_COLUMNS
	txn.appendWorkspaceEntryLocked(persisted)
	for _, modify := range []func(*Entry){
		func(e *Entry) { e.accountId++ },
		func(e *Entry) { e.databaseId++ },
		func(e *Entry) { e.tableId++ },
	} {
		e := base
		modify(&e)
		txn.appendWorkspaceEntryLocked(e)
	}
	tables, err = txn.collectTombstoneTransferTablesLocked()
	require.NoError(t, err)
	require.Len(t, tables, 4)
	key := tombstoneTransferKey{base.accountId, base.databaseId, base.tableId}
	mixed := tables[key]
	require.Equal(t, [2]bool{true, true}, mixed.hasDeletes)
	require.Equal(t, [2]string{catalog.MO_COLUMNS_UPDATE, catalog.MO_COLUMNS}, mixed.writeNames)
	require.Equal(t, catalog.MO_COLUMNS, physicalCatalogTableName(key.databaseId, key.tableId, mixed.writeNames[0]))
	// Appending/replacing workspace entries cannot invalidate collected metadata.
	txn.workspace = newTxnWorkspace()
	require.Equal(t, catalog.MO_CATALOG, mixed.databaseName)
}

func TestTombstoneTransferCallbacks(t *testing.T) {
	txn := &Transaction{}
	tables := map[tombstoneTransferKey]tombstoneTransferTable{}
	for id := uint64(1); id <= 3; id++ {
		tables[tombstoneTransferKey{tableId: id}] = tombstoneTransferTable{
			table: &txnTable{tableId: id}, hasDeletes: [2]bool{true, true},
			writeNames: [2]string{"memory", "persisted"},
		}
	}
	txn.Lock()
	defer txn.Unlock()
	seen := make(map[uint64]*txnTable)
	require.NoError(t, txn.forEachTableHasDeletesLocked(tables, 0, func(tbl *txnTable, name string) error {
		require.Equal(t, "memory", name)
		seen[tbl.tableId] = tbl
		return nil
	}))
	require.NoError(t, txn.forEachTableHasDeletesLocked(tables, 1, func(tbl *txnTable, name string) error {
		require.Len(t, seen, 3)
		require.Same(t, seen[tbl.tableId], tbl)
		require.Equal(t, "persisted", name)
		return nil
	}))
	failure := errors.New("transfer failed")
	calls := 0
	require.ErrorIs(t, txn.forEachTableHasDeletesLocked(tables, 0, func(*txnTable, string) error {
		calls++
		return failure
	}), failure)
	require.Equal(t, 1, calls)
}

func TestTombstoneTransferLookupReentry(t *testing.T) {
	enableReenterWorkspaceReadViewFault(t)
	txn := &Transaction{engine: &Engine{}, workspace: newTxnWorkspace(), op: transferBenchmarkOperator{},
		proc: &process.Process{Ctx: context.Background()}}
	tables := map[tombstoneTransferKey]tombstoneTransferTable{
		{tableId: 42}: {databaseName: "db", hasDeletes: [2]bool{true}, writeNames: [2]string{"tbl"}},
	}
	txn.Lock()
	err := txn.forEachTableHasDeletesLocked(tables, 0, func(*txnTable, string) error {
		t.Fatal("callback must not run after lookup failure")
		return nil
	})
	require.ErrorContains(t, err, "reenter workspace read view")
	require.False(t, txn.TryLock(), "lookup failure must return with workspace locked")
	txn.Unlock()
	require.True(t, txn.TryLock(), "no lock may remain after caller unlocks")
	txn.Unlock()
}

// Empty/insert-only workspaces must not require a process or SHARED FS.
func TestTombstoneTransferWithoutDeletes(t *testing.T) {
	txn := &Transaction{op: transferBenchmarkOperator{}, workspace: newTxnWorkspace()}
	txn.appendWorkspaceEntryLocked(Entry{typ: INSERT})
	txn.Lock()
	defer txn.Unlock()
	require.NoError(t, txn.transferTombstones(context.Background()))
}
