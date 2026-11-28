// Copyright 2022 Matrix Origin
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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceRewriteTombstoneOffsets(t *testing.T) {
	ctx := t.Context()
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)
	var tombstones workspaceFlushedTombstones
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	blockA := types.NewBlockidWithObjectID(name.ObjectId(), 0)
	blockB := types.NewBlockidWithObjectID(name.ObjectId(), 1)

	writeTombstones := func(tableID uint64, block types.Blockid, offset uint32) objectio.ObjectStats {
		stats := writeRewriteTombstoneForTest(t, mp, fs, block, offset)
		vec := vector.NewVec(types.T_blob.ToType())
		defer vec.Free(mp)
		require.NoError(t, vector.AppendBytes(vec, stats[:], false, mp))
		tombstones.adjust(tableID, vec, 1)
		return stats
	}

	inherited := writeTombstones(1, blockA, 2)
	writeTombstones(2, blockA, 7)
	unrelated := types.RandomRowid()
	unrelatedStats := writeTombstones(1, *unrelated.BorrowBlockID(), 0)
	require.NoError(t, fs.Delete(ctx, unrelatedStats.ObjectName().String()))
	first := tombstones.objectTombstones(1, name.ObjectId())
	second := tombstones.objectTombstones(2, name.ObjectId())
	require.Equal(t, []objectio.ObjectStats{inherited}, first)
	require.Len(t, second, 1)
	require.Empty(t, tombstones.objectTombstones(3, name.ObjectId()))

	var snapshot types.TS
	pending := []int64{2, 1, 1}
	got, err := mergeTombstoneOffsets(ctx, fs, &snapshot, &blockA, first, pending, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, got)
	require.Equal(t, []int64{2, 1, 1}, pending)
	got, err = mergeTombstoneOffsets(ctx, fs, &snapshot, &blockA, second, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{7}, got)
	got, err = mergeTombstoneOffsets(ctx, fs, &snapshot, &blockB, first, []int64{4}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{4}, got)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = mergeTombstoneOffsets(canceled, fs, &snapshot, &blockA, first, pending, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, fs.Delete(ctx, inherited.ObjectName().String()))
	_, err = mergeTombstoneOffsets(ctx, fs, &snapshot, &blockA, first, pending, nil)
	require.Error(t, err)
	require.Equal(t, []int64{2, 1, 1}, pending)
}

// writeRewriteTombstoneForTest writes the same file format as a CN spill.
func writeRewriteTombstoneForTest(t *testing.T, mp *mpool.MPool, fs fileservice.FileService, block types.Blockid, offset uint32) objectio.ObjectStats {
	t.Helper()
	pkType := types.T_int32.ToType()
	writer := colexec.NewCNS3TombstoneWriter(mp, fs, pkType, -1)
	defer writer.Close()
	bat := readutil.NewCNTombstoneBatch(&pkType, objectio.HiddenColumnSelection_None)
	defer bat.Clean(mp)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.NewRowid(&block, offset), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(offset), false, mp))
	bat.SetRowCount(1)
	require.NoError(t, writer.Write(t.Context(), bat))
	stats, err := writer.Sync(t.Context())
	require.NoError(t, err)
	require.Len(t, stats, 1)
	return stats[0]
}

type rewriteGCWitness struct {
	fileservice.FileService
	deleted chan []string
	// When set, persist the second replacement and then return this error.
	writeErr      error
	writes        atomic.Int32
	ambiguousName atomic.Value
	deleteRetried atomic.Bool
}

func (f *rewriteGCWitness) Write(ctx context.Context, io fileservice.IOVector) error {
	if err := f.FileService.Write(ctx, io); err != nil {
		return err
	}
	if f.writeErr != nil && f.writes.Add(1) == 2 {
		f.ambiguousName.Store(io.FilePath)
		return f.writeErr
	}
	return nil
}

func (f *rewriteGCWitness) Delete(ctx context.Context, names ...string) error {
	if len(names) == 1 && names[0] == f.ambiguousName.Load() && f.deleteRetried.CompareAndSwap(false, true) {
		return errors.New("temporary delete failure")
	}
	err := f.FileService.Delete(ctx, names...)
	if err == nil {
		f.deleted <- names
	}
	return err
}

func TestCloneRewriteFailureRequiresFullRollback(t *testing.T) {
	t.Run("missing inherited tombstone", func(t *testing.T) { testCloneRewriteFailure(t, false) })
	t.Run("ambiguous upload with cleanup retry", func(t *testing.T) { testCloneRewriteFailure(t, true) })
}

func testCloneRewriteFailure(t *testing.T, ambiguousUpload bool) {
	t.Helper()
	ctx := t.Context()
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	op, closeOp := client.NewTestTxnOperator(ctx)
	t.Cleanup(closeOp)
	op.AddWorkspace(txn)
	txn.op = op
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	tbl.db.op = op
	mp := txn.proc.Mp()
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	gc := &rewriteGCWitness{FileService: fs, deleted: make(chan []string, 4)}
	txn.engine.fs = gc
	txn.engine.localInsertObjects = colexec.NewLocalInsertObjectRegistry(ctx, gc, 4)
	t.Cleanup(func() { require.NoError(t, txn.engine.CloseLocalInsertObjects()) })
	txn.cnObjsSummary = make(map[uint64]map[types.Objectid]Summary)
	txn.engine.cloneTxnCache = newCloneTxnCache()
	txn.engine.gcPool, err = ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(txn.engine.gcPool.Release)
	colexec.NewServer(txn.engine.service)
	txn.SetCloneTxn(1)
	txn.tnStores = []DNStore{catalogCreateTNStoreForTest()}
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[uint64]map[types.Blockid][]int64)}
	txn.compactWorker, err = ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(txn.compactWorker.Release)
	t.Cleanup(func() {
		if !txn.removed {
			cleanTxnWritesForTest(txn)
		}
	})

	tbl.tableDef.Cols = []*plan.ColDef{
		{Name: "pk", Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}, Seqnum: 1},
	}
	source := make([]string, 0, 2)
	var lastBlock types.Blockid
	for i := range 2 {
		writer := colexec.NewCNS3DataWriter(mp, fs, tbl.tableDef, -1, false)
		t.Cleanup(func() { _ = writer.Close() })
		rows := batch.New([]string{"pk"})
		rows.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		for id := range 3 {
			require.NoError(t, vector.AppendFixed(rows.Vecs[0], int32(10*i+id), false, mp))
		}
		rows.SetRowCount(3)
		err := writer.Write(ctx, rows)
		rows.Clean(mp)
		require.NoError(t, err)
		stats, err := writer.Sync(ctx)
		require.NoError(t, err)
		require.Len(t, stats, 1)
		metadata, err := writer.FillBlockInfoBat()
		require.NoError(t, err)
		name := stats[0].ObjectName()
		source = append(source, name.String())
		txn.engine.cloneTxnCache.AddSharedFile(txn.op.Txn().ID, name.String())
		require.NoError(t, txn.writeFileLockedWithAutoIncrEpochKnown(
			INSERT, 1, 7, 42, "db", "tbl", name.String(), metadata, txn.tnStores[0], 0, false,
		))
		lastBlock = types.NewBlockidWithObjectID(name.ObjectId(), 0)
		txn.deletedBlocks.addDeletedBlocks(42, &lastBlock, []int64{1})
	}

	if ambiguousUpload {
		gc.writeErr = errors.New("ambiguous rewrite upload")
		txn.proc.SetFileService(gc)
	} else {
		missing := writeRewriteTombstoneForTest(t, mp, fs, lastBlock, 0)
		vec := vector.NewVec(types.T_blob.ToType())
		require.NoError(t, vector.AppendBytes(vec, missing[:], false, mp))
		txn.flushedTombstones.adjust(42, vec, 1)
		vec.Free(mp)
		require.NoError(t, fs.Delete(ctx, missing.ObjectName().String()))
	}

	originalA, originalB := txn.writes[0].bat, txn.writes[1].bat
	txn.offsets = []int{0, 2}
	txn.statementID = 2
	txn.StartStatement()
	admissionErr := txn.IncrStatementID(ctx, false)
	require.Error(t, admissionErr)
	if ambiguousUpload {
		require.ErrorContains(t, admissionErr, gc.writeErr.Error())
	}
	require.ErrorIs(t, txn.RollbackLastStatement(ctx), admissionErr)
	txn.EndStatement()
	require.Len(t, txn.writes, 3, "first replacement must remain owned by the transaction")
	require.Same(t, originalA, txn.writes[0].bat)
	require.Same(t, originalB, txn.writes[1].bat)
	require.Equal(t, 2, txn.statementID)
	require.Equal(t, []int{0, 2}, txn.offsets)
	require.Equal(t, 2, txn.deletedBlocks.size())
	replacementStats := objectio.ObjectStats(txn.writes[2].bat.Vecs[1].GetBytesAt(0))
	replacement := replacementStats.ObjectName().String()

	reqs, err := txn.Commit(ctx)
	require.ErrorIs(t, err, admissionErr)
	require.Empty(t, reqs)
	require.NoError(t, txn.Rollback(ctx))
	expectedGC := []string{replacement}
	if ambiguousUpload {
		name, _ := gc.ambiguousName.Load().(string)
		require.NotEmpty(t, name)
		expectedGC = append(expectedGC, name)
	}
	var deleted []string
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for len(deleted) < len(expectedGC) {
		select {
		case names := <-gc.deleted:
			deleted = append(deleted, names...)
		case <-deadline.C:
			t.Fatalf("only reclaimed %v, expected %v", deleted, expectedGC)
		}
	}
	require.ElementsMatch(t, expectedGC, deleted)
	if ambiguousUpload {
		require.True(t, gc.deleteRetried.Load(), "the registry must retry the transient delete failure")
	}

	for _, name := range source {
		_, err := fs.StatFile(ctx, name)
		require.NoError(t, err, "rollback must retain the committed source object")
	}
}
