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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// Capture through the production workspace owner, then consume through the
// actual datasource. Only two rows are needed to distinguish prior deletes,
// later deletes, cross-member leakage, and a borrowed mutable batch.
func TestM0StatementViewOwnsVisiblePrefix(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	op, closeOp := client.NewTestTxnOperator(t.Context())
	t.Cleanup(closeOp)
	txn := &Transaction{proc: proc, op: op, engine: &Engine{}, batchSelectList: make(map[*batch.Batch][]int64), deletedBlocks: &deletedBlocks{byTable: make(map[uint64]map[types.Blockid][]int64)}, cn_flushed_s3_tombstone_object_stats_list: new(sync.Map)}
	op.AddWorkspace(txn)
	table := &txnTable{tableId: 42, db: &txnDatabase{databaseId: 7, op: op}}
	table.proc.Store(proc)
	row := types.RandomRowid()
	block := *row.BorrowBlockID()
	rows := batch.New([]string{catalog.Row_ID, "a"})
	rows.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	rows.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { rows.Clean(proc.Mp()) })
	for i := 0; i < 2; i++ {
		require.NoError(t, vector.AppendFixed(rows.Vecs[0], types.NewRowid(&block, uint32(i)), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(rows.Vecs[1], int32(i+10), false, proc.Mp()))
	}
	rows.SetRowCount(2)
	txn.writes = []Entry{{typ: INSERT, databaseId: 7, tableId: 42, bat: rows}}
	txn.deletedBlocks.byTable[42] = map[types.Blockid][]int64{block: {1}}
	txn.deletedBlocks.byTable[43] = map[types.Blockid][]int64{block: {0}}
	baseline := proc.Mp().CurrNB()
	captured, err := txn.CaptureM0StatementReadView(t.Context(), 1)
	require.NoError(t, err)
	defer func() {
		if captured != nil {
			captured.Release()
		}
	}()
	v := captured.(*m0StatementReadView)
	source, err := NewLocalDataSource(m0read.WithStatementReadView(t.Context(), captured), table, 1, nil, nil, nil, false, engine.Policy_CheckAll, engine.GeneralLocalDataSource)
	require.NoError(t, err)
	defer source.Close()
	source.memPKFilter = &readutil.MemPKFilter{}
	// Change live payload and delete state after capture. Neither can affect the
	// view; the other relation's shared-block delete must also remain isolated.
	txn.beginCatalogMutation()
	txn.Lock()
	vector.MustFixedColNoTypeCheck[int32](rows.Vecs[1])[0] = 99
	txn.deletedBlocks.Lock()
	txn.deletedBlocks.byTable[42][block] = []int64{0, 1}
	txn.deletedBlocks.Unlock()
	txn.writes = append(txn.writes, txn.writes[0])
	txn.Unlock()
	txn.endCatalogMutation()
	out := batch.New([]string{"a"})
	out.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	defer out.Clean(proc.Mp())
	require.NoError(t, source.filterInMemUnCommittedInserts(t.Context(), []uint16{0}, -1, proc.Mp(), out))
	require.Equal(t, []int32{10}, vector.MustFixedColNoTypeCheck[int32](out.Vecs[0]))
	source.Close()
	out.Clean(proc.Mp())
	// Owner release must wait for the last reader, and then refund the metadata
	// reservation and cloned vectors exactly once.
	require.Greater(t, proc.Mp().CurrNB(), baseline)
	require.NoError(t, v.acquire(txn, 1))
	captured.Release()
	captured = nil
	require.Greater(t, proc.Mp().CurrNB(), baseline)
	v.Release()
	require.Equal(t, baseline, proc.Mp().CurrNB())
	require.ErrorContains(t, v.acquire(txn, 1), "closed")
}

func TestM0StatementViewFailureAndBinding(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	op, closeOp := client.NewTestTxnOperator(t.Context())
	defer closeOp()
	txn := &Transaction{proc: proc, op: op}
	op.AddWorkspace(txn)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := txn.CaptureM0StatementReadView(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
	_, err = txn.CaptureM0StatementReadView(t.Context(), 1)
	require.Error(t, err)
	txn.beginCatalogMutation()
	_, err = txn.CaptureM0StatementReadView(t.Context(), 0)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
	txn.endCatalogMutation()
	view, err := txn.CaptureM0StatementReadView(t.Context(), 0)
	require.NoError(t, err)
	v := view.(*m0StatementReadView)
	require.ErrorContains(t, v.acquire(&Transaction{op: op}, 0), "binding changed")
	require.ErrorContains(t, v.acquire(txn, 1), "binding changed")
	view.Release()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestM0StatementViewBudgetFailure(t *testing.T) {
	mp, err := mpool.NewMPool("m0-statement-budget", 1024*1024, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)
	proc := testutil.NewProcess(t, testutil.WithMPool(mp))
	defer proc.Free()
	op, closeOp := client.NewTestTxnOperator(t.Context())
	defer closeOp()
	txn := &Transaction{proc: proc, op: op}
	// Occupy all but 128 bytes; even the empty-view metadata reservation must
	// fail before publishing any state.
	held, err := mp.Alloc(1024*1024-128, true)
	require.NoError(t, err)
	view, err := txn.CaptureM0StatementReadView(t.Context(), 0)
	require.Error(t, err)
	mp.Free(held)
	require.Nil(t, view)
	require.Zero(t, mp.CurrNB())
}

func TestM0StatementViewPartialFailureCleanup(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	op, closeOp := client.NewTestTxnOperator(t.Context())
	defer closeOp()
	txn := &Transaction{proc: proc, op: op}
	first := batch.New([]string{"a"})
	first.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(first.Vecs[0], int32(1), false, proc.Mp()))
	first.SetRowCount(1)
	defer first.Clean(proc.Mp())
	bad := batch.New([]string{catalog.ObjectMeta_ObjectStats})
	bad.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bad.Vecs[0], []byte("truncated"), false, proc.Mp()))
	bad.SetRowCount(1)
	defer bad.Clean(proc.Mp())
	txn.writes = []Entry{{typ: INSERT, bat: first}, {typ: DELETE, fileName: "bad", bat: bad}}
	before := proc.Mp().CurrNB()
	view, err := txn.CaptureM0StatementReadView(t.Context(), 2)
	require.ErrorContains(t, err, "invalid flushed tombstone stats")
	require.Nil(t, view)
	require.Equal(t, before, proc.Mp().CurrNB(), "both already-cloned batches and the metadata reservation must be refunded")
}

func TestM0StatementViewCollectorsUseCapturedMember(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	op, closeOp := client.NewTestTxnOperator(t.Context())
	defer closeOp()
	txn := &Transaction{proc: proc, op: op, deletedBlocks: &deletedBlocks{byTable: make(map[uint64]map[types.Blockid][]int64)}}
	op.AddWorkspace(txn)
	row := types.RandomRowid()
	block := *row.BorrowBlockID()
	txn.deletedBlocks.byTable[42] = map[types.Blockid][]int64{block: {1}}
	txn.deletedBlocks.byTable[43] = map[types.Blockid][]int64{block: {0}}
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsLocation(stats, objectio.NewRandomLocation(1, 1)))
	meta := batch.New([]string{catalog.ObjectMeta_ObjectStats})
	meta.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(meta.Vecs[0], stats.Marshal(), false, proc.Mp()))
	meta.SetRowCount(1)
	defer meta.Clean(proc.Mp())
	txn.writes = []Entry{{typ: INSERT, databaseId: 7, tableId: 42, fileName: "data", bat: meta}}
	view, err := txn.CaptureM0StatementReadView(t.Context(), 1)
	require.NoError(t, err)
	defer view.Release()
	table := &txnTable{tableId: 42, db: &txnDatabase{databaseId: 7, op: op}}
	ctx := m0read.WithStatementReadView(t.Context(), view)
	// Subsequent removal/compaction of live entries cannot make ranges lose the
	// captured persistent data or adopt another relation's raw deletes.
	txn.writes = nil
	txn.deletedBlocks.byTable[42][block] = []int64{0, 1}
	objects, _, err := table.collectUnCommittedDataObjs(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []objectio.ObjectStats{*stats}, objects)
	deletes, err := table.CollectTombstones(ctx, 1, engine.Policy_CollectUncommittedTombstones)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, deletes.ApplyInMemTombstones(&block, []int64{0, 1}, nil))
}

type statementSnapshotOp struct {
	client.TxnOperator
	snapshot timestamp.Timestamp
}

func (op *statementSnapshotOp) SnapshotTS() timestamp.Timestamp { return op.snapshot }

func TestM0StatementViewSnapshotRefreshRequiresRecapture(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	op, closeOp := client.NewTestTxnOperator(t.Context())
	defer closeOp()
	bound := &statementSnapshotOp{TxnOperator: op, snapshot: timestamp.Timestamp{PhysicalTime: 1}}
	txn := &Transaction{proc: proc, op: bound}
	old, err := txn.CaptureM0StatementReadView(t.Context(), 0)
	require.NoError(t, err)
	defer old.Release()
	bound.snapshot = timestamp.Timestamp{PhysicalTime: 2}
	require.True(t, moerr.IsMoErrCode(old.(*m0StatementReadView).acquire(txn, 0), moerr.ErrTxnNeedRetry))
	fresh, err := txn.CaptureM0StatementReadView(t.Context(), 0)
	require.NoError(t, err)
	defer fresh.Release()
	require.NoError(t, fresh.(*m0StatementReadView).acquire(txn, 0))
	fresh.Release()
}
