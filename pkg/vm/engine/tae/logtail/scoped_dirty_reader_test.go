// Copyright 2021 Matrix Origin
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

package logtail

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func scopedDirtyTxn(ts int64, state txnif.TxnState, tableID uint64) *txnbase.Txn {
	txn := &txnbase.Txn{TxnCtx: txnbase.NewEmptyTxnCtx()}
	txn.PrepareTS = types.BuildTS(ts, 0)
	txn.State = state
	if tableID != 0 {
		txn.Memo.AddTable(1, tableID)
	}
	return txn
}

func TestScopedDirtyReaderChargesInactiveRecords(t *testing.T) {
	table := NewTxnTable(3, func() types.TS { return types.TS{} })
	require.NoError(t, table.AddTxn(scopedDirtyTxn(10, txnif.TxnStateCommitted, 1)))
	require.NoError(t, table.AddTxn(scopedDirtyTxn(20, txnif.TxnStateRollbacked, 2)))
	require.NoError(t, table.AddTxn(scopedDirtyTxn(30, txnif.TxnStateCommitted, 0)))
	reader := Reader{table: table, from: types.BuildTS(20, 0), to: types.BuildTS(30, 0)}

	count := 0
	exhausted := errors.New("exhausted")
	err := reader.ForEachDirtyTable(func() error {
		count++
		// Snapshot, pivot, block, predecessor, rolled-back row. The
		// next empty committed row must check the budget too.
		if count > 5 {
			return exhausted
		}
		return nil
	}, func(model.TableRecord) error {
		t.Fatal("reader yielded an out-of-window, rolled-back, or missing table")
		return nil
	})
	require.ErrorIs(t, err, exhausted)
	require.Equal(t, 6, count)
}

func TestScopedDirtyReaderCompactionDoesNotLoseCandidates(t *testing.T) {
	table := NewTxnTable(2, func() types.TS { return types.TS{} })
	first := scopedDirtyTxn(10, txnif.TxnStateCommitted, 1)
	require.NoError(t, table.AddTxn(first))
	require.NoError(t, table.AddTxn(scopedDirtyTxn(20, txnif.TxnStateCommitted, 2)))
	var block *txnBlock
	table.Scan(func(b *txnBlock) bool {
		block = b
		return false
	})
	require.NotNil(t, block)
	reader := Reader{table: table, from: types.BuildTS(20, 0), to: types.BuildTS(20, 0)}
	charge := func() error { return nil }

	all, changed := block.TryCompact()
	require.True(t, all && changed)
	published := block.summary.Swap(nil)
	var ids []uint64
	visit := func(record model.TableRecord) error {
		ids = append(ids, record.ID)
		return nil
	}
	require.ErrorContains(t, reader.ForEachDirtyTable(charge, visit), "during compaction")
	require.Empty(t, ids)
	block.summary.Store(published)
	require.NoError(t, reader.ForEachDirtyTable(charge, visit))
	require.Contains(t, ids, uint64(2), "compacted history must retain the inclusive upper endpoint")
}

func TestScopedDirtyReaderSkipsDisjointCompactedBlocks(t *testing.T) {
	table := NewTxnTable(2, func() types.TS { return types.BuildTS(5, 0) })
	first := scopedDirtyTxn(10, txnif.TxnStateCommitted, 1)
	// Insertion can precede prepare timestamp assignment. The resulting
	// block birth time is then earlier than the actual first row.
	first.PrepareTS = txnif.UncommitTS
	require.NoError(t, table.AddTxn(first))
	first.PrepareTS = types.BuildTS(10, 0)
	require.NoError(t, table.AddTxn(scopedDirtyTxn(20, txnif.TxnStateCommitted, 2)))
	require.NoError(t, table.AddTxn(scopedDirtyTxn(30, txnif.TxnStateCommitted, 3)))
	require.NoError(t, table.AddTxn(scopedDirtyTxn(40, txnif.TxnStateCommitted, 4)))
	blocks := 0
	table.Scan(func(block *txnBlock) bool {
		all, changed := block.TryCompact()
		require.True(t, all && changed)
		blocks++
		return true
	})
	require.Equal(t, 2, blocks)

	read := func(from, to int64) []uint64 {
		t.Helper()
		reader := Reader{table: table, from: types.BuildTS(from, 0), to: types.BuildTS(to, 0)}
		var ids []uint64
		require.NoError(t, reader.ForEachDirtyTable(func() error { return nil }, func(record model.TableRecord) error {
			ids = append(ids, record.ID)
			return nil
		}))
		return ids
	}
	require.Empty(t, read(8, 9), "the birth timestamp is not the first actual prepare timestamp")
	require.Empty(t, read(21, 29), "the preceding block is selected by the pivot but ends before the window")
	require.Contains(t, read(8, 10), uint64(1), "the upper endpoint is inclusive")
	require.Contains(t, read(20, 29), uint64(2), "the lower endpoint is inclusive")
	require.ElementsMatch(t, []uint64{3, 4}, read(21, 40), "skipping the predecessor still visits the next block")
}
