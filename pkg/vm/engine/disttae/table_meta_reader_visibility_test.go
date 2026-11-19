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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

type cancelMaterializationWriteFS struct {
	fileservice.FileService
	cancel context.CancelFunc
}

func (fs cancelMaterializationWriteFS) Write(ctx context.Context, v fileservice.IOVector) error {
	if err := fs.FileService.Write(ctx, v); err != nil {
		return err
	}
	fs.cancel()
	return ctx.Err()
}

// Materialization assigns new rowids, so transaction deletes must be applied
// while the source rowids still identify the rows.
func TestTableMetaReaderMaterializesVisibleRows(t *testing.T) {
	txn, _, fs, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()
	disarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	defer disarm()
	ids := []types.Rowid{buildTestMaterializedSnapshotRowID(t, 0), buildTestMaterializedSnapshotRowID(t, 1)}
	state := newTestMaterializedSnapshotPartitionState(t, mp, []testMaterializedSnapshotRow{
		{rowID: ids[0], ts: types.BuildTS(1, 0), id: 1, payload: 10},
		{rowID: ids[1], ts: types.BuildTS(1, 0), id: 2, payload: 20},
		{rowID: buildTestMaterializedSnapshotRowID(t, 2), ts: types.BuildTS(3, 0), id: 3, payload: 30},
	})
	appendDelete := func(tableID uint64, id types.Rowid, key int64) {
		b := batch.NewWithSize(2)
		t.Cleanup(func() { b.Clean(mp) })
		b.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
		b.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		b.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(b.Vecs[0], id, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], key, false, mp))
		b.SetRowCount(1)
		txn.appendWorkspaceEntryLocked(Entry{typ: DELETE, accountId: 1, databaseId: tbl.db.databaseId, tableId: tableID, databaseName: "db", tableName: "r32tbl", bat: b})
	}
	materialize := func(offset int) []int64 {
		r := &TableMetaReader{table: tbl, fs: fs, snapshot: types.BuildTS(2, 0), txnOffset: offset, pState: state, sinkerOption: txn.SinkerCapacityOption()}
		out := colexec.AllocCNS3ResultBat(false)
		defer out.Clean(mp)
		seqs, typs, attrs, _, _ := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(tbl.tableDef)
		_, err := r.collectDataOfAObjsAndInMem(ctx, mp, out, seqs, attrs, typs, readutil.ObjListRelData{})
		require.NoError(t, err)
		if out.RowCount() == 0 {
			return nil
		}
		stats := r32FirstOutBatchStats(t, out)
		reader, err := ioutil.NewObjectReader(fs, stats.ObjectLocation())
		require.NoError(t, err)
		var keys []int64
		for i := uint16(0); i < uint16(stats.BlkCnt()); i++ {
			loaded, release, err := reader.LoadColumns(ctx, []uint16{0}, nil, i, mp)
			require.NoError(t, err)
			keys = append(keys, vector.MustFixedColWithTypeCheck[int64](loaded.Vecs[0])...)
			release()
		}
		return keys
	}
	appendDelete(r32TableID+1, ids[1], 2)
	initial := len(txn.writes)
	appendDelete(r32TableID, ids[0], 1)
	boundary := len(txn.writes)
	appendDelete(r32TableID, ids[1], 2)
	require.ElementsMatch(t, []int64{1, 2}, materialize(initial), "deletes after boundary are invisible")
	require.ElementsMatch(t, []int64{2}, materialize(boundary), "only the live source row is rewritten")
	// Fail after the filtered nonempty batch reaches durable storage. The
	// transaction must retain cleanup ownership even though no metadata was
	// returned to the destination relation.
	failFS := fs
	failFS.armPostPersistFailure(func() bool { return true })
	r := &TableMetaReader{table: tbl, fs: fs, snapshot: types.BuildTS(2, 0), txnOffset: boundary, pState: state, sinkerOption: txn.SinkerCapacityOption()}
	before := mp.CurrNB()
	out := colexec.AllocCNS3ResultBat(false)
	seqs, typs, attrs, _, _ := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(tbl.tableDef)
	_, err := r.collectDataOfAObjsAndInMem(ctx, mp, out, seqs, attrs, typs, readutil.ObjListRelData{})
	out.Clean(mp)
	require.ErrorContains(t, err, "injected post-persist fs write failure")
	require.Equal(t, before, mp.CurrNB(), "failed reader and writer release their batches")
	failedName := failFS.failedAtName()
	require.NotEmpty(t, failedName)
	failFS.stopPostPersistFailure()
	txn.Lock()
	txn.drainUnpublishedDumpReservationsLocked(drainUnpublishedDumpReservationsTerminalReason)
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.NoError(t, txn.engine.DrainUnpublishedPendingForTest(ctx))
	issue25590RequireGone(t, failFS, failedName, "failed output is reclaimed")
	_, reserved := txn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, reserved)
	require.ElementsMatch(t, []int64{2}, materialize(boundary), "a new reader succeeds after failure")
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cancelFS := cancelMaterializationWriteFS{FileService: fs, cancel: cancel}
	cancelReader := &TableMetaReader{table: tbl, fs: cancelFS, snapshot: types.BuildTS(2, 0), txnOffset: boundary, pState: state, sinkerOption: txn.SinkerCapacityOption()}
	beforeCancelWrites := len(failFS.persistedSnapshot())
	beforeCancelMemory := mp.CurrNB()
	cancelOut := colexec.AllocCNS3ResultBat(false)
	_, err = cancelReader.collectDataOfAObjsAndInMem(cancelCtx, mp, cancelOut, seqs, attrs, typs, readutil.ObjListRelData{})
	cancelOut.Clean(mp)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, beforeCancelMemory, mp.CurrNB(), "canceled reader and writer release their batches")
	persisted := failFS.persistedSnapshot()
	require.Len(t, persisted, beforeCancelWrites+1)
	canceledName := persisted[len(persisted)-1]
	txn.Lock()
	txn.drainUnpublishedDumpReservationsLocked(drainUnpublishedDumpReservationsTerminalReason)
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.NoError(t, txn.engine.DrainUnpublishedPendingForTest(ctx))
	issue25590RequireGone(t, failFS, canceledName, "canceled output is reclaimed")
	pending, reserved := txn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, pending)
	require.Empty(t, reserved)
	require.ElementsMatch(t, []int64{2}, materialize(boundary), "a new reader succeeds after cancellation")
	require.Empty(t, materialize(len(txn.writes)), "all deleted rows produce no data")
}

// Appendable files carry per-row commit timestamps and are rewritten rather
// than shared. Exercise the actual object format and source rowids.
func TestTableMetaReaderMaterializesAppendableVisibility(t *testing.T) {
	txn, _, fs, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()
	disarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	defer disarm()
	src := batch.NewWithSize(4)
	defer src.Clean(mp)
	src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	src.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	src.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	src.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	for i, ts := range []int64{1, 1, 3} {
		require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(i+1), false, mp))
		require.NoError(t, vector.AppendFixed(src.Vecs[1], int64(i+10), false, mp))
		require.NoError(t, vector.AppendFixed(src.Vecs[2], types.BuildTS(ts, 0), false, mp))
		require.NoError(t, vector.AppendFixed(src.Vecs[3], false, false, mp))
	}
	src.SetRowCount(3)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT}, 0, true, false, fs)
	writer.SetAppendable()
	_, err := writer.WriteBatch(src)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats(objectio.WithAppendable())
	require.True(t, stats.GetAppendable())
	sharedRows := batch.NewWithSize(2)
	sharedRows.Vecs[0], sharedRows.Vecs[1] = src.Vecs[0], src.Vecs[1]
	sharedRows.SetRowCount(3)
	sharedWriter := ioutil.ConstructWriter(0, []uint16{0, 1}, 0, true, false, fs)
	_, err = sharedWriter.WriteBatch(sharedRows)
	require.NoError(t, err)
	_, _, err = sharedWriter.Sync(ctx)
	require.NoError(t, err)
	shared := sharedWriter.GetObjectStats()
	require.False(t, shared.GetAppendable())
	id := types.NewRowIDWithObjectIDBlkNumAndRowID(*stats.ObjectName().ObjectId(), 0, 0)
	sharedID := types.NewRowIDWithObjectIDBlkNumAndRowID(*shared.ObjectName().ObjectId(), 0, 0)
	del := batch.NewWithSize(2)
	defer del.Clean(mp)
	del.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
	del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	del.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(del.Vecs[0], id, false, mp))
	require.NoError(t, vector.AppendFixed(del.Vecs[1], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(del.Vecs[0], sharedID, false, mp))
	require.NoError(t, vector.AppendFixed(del.Vecs[1], int64(1), false, mp))
	del.SetRowCount(2)
	txn.appendWorkspaceEntryLocked(Entry{typ: DELETE, accountId: 1, databaseId: tbl.db.databaseId, tableId: r32TableID, databaseName: "db", tableName: "r32tbl", bat: del})
	state := newTestMaterializedSnapshotPartitionState(t, mp, nil)
	require.NoError(t, state.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}, false))
	require.NoError(t, state.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{ObjectStats: shared, CreateTime: types.BuildTS(1, 0)}, false))
	r := &TableMetaReader{table: tbl, fs: fs, snapshot: types.BuildTS(2, 0), txnOffset: len(txn.writes), pState: state, sinkerOption: txn.SinkerCapacityOption()}
	out := colexec.AllocCNS3ResultBat(false)
	defer out.Clean(mp)
	seqs, typs, attrs, _, _ := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(tbl.tableDef)
	_, err = r.collect(ctx, mp, out, false, seqs, attrs, typs)
	require.NoError(t, err)
	require.Equal(t, 2, out.RowCount())
	col, area := vector.MustVarlenaRawData(out.Vecs[1])
	first := objectio.ObjectStats(col[0].GetByteSlice(area))
	second := objectio.ObjectStats(col[1].GetByteSlice(area))
	require.Equal(t, shared.ObjectName(), first.ObjectName(), "immutable object remains shared")
	written := second
	require.NotEqual(t, stats.ObjectName(), written.ObjectName())
	reader, err := ioutil.NewObjectReader(fs, written.ObjectLocation())
	require.NoError(t, err)
	loaded, release, err := reader.LoadColumns(ctx, []uint16{0}, nil, 0, mp)
	require.NoError(t, err)
	defer release()
	require.Equal(t, []int64{2}, vector.MustFixedColWithTypeCheck[int64](loaded.Vecs[0]))
	tombs := colexec.AllocCNS3ResultBat(true)
	defer tombs.Clean(mp)
	_, err = r.collect(ctx, mp, tombs, true, []uint16{0, 1}, objectio.TombstoneAttrs_CN_Created, []types.Type{types.T_Rowid.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	tombStats := r32FirstOutBatchStats(t, tombs)
	tombReader, err := ioutil.NewObjectReader(fs, tombStats.ObjectLocation())
	require.NoError(t, err)
	tombRows, releaseTombs, err := tombReader.LoadColumns(ctx, []uint16{0}, nil, 0, mp)
	require.NoError(t, err)
	defer releaseTombs()
	tombIDs := vector.MustFixedColWithTypeCheck[types.Rowid](tombRows.Vecs[0])
	require.ElementsMatch(t, []types.Rowid{id, sharedID}, tombIDs)
	sharedReader, err := ioutil.NewObjectReader(fs, shared.ObjectLocation())
	require.NoError(t, err)
	sharedData, releaseShared, err := sharedReader.LoadColumns(ctx, []uint16{0}, nil, 0, mp)
	require.NoError(t, err)
	defer releaseShared()
	sharedKeys := vector.MustFixedColWithTypeCheck[int64](sharedData.Vecs[0])
	require.Equal(t, []int64{1, 2, 3}, sharedKeys)
	deleted := make(map[types.Rowid]bool, len(tombIDs))
	for _, rowID := range tombIDs {
		deleted[rowID] = true
	}
	var live []int64
	for offset, key := range sharedKeys {
		rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(*shared.ObjectName().ObjectId(), 0, uint32(offset))
		if !deleted[rowID] {
			live = append(live, key)
		}
	}
	require.Equal(t, []int64{2, 3}, live)

	// Fail the actual mixed-object collection after the replacement object is
	// persisted. The already committed shared source must survive cleanup.
	fs.armPostPersistFailure(func() bool { return true })
	failedOut := colexec.AllocCNS3ResultBat(false)
	_, err = r.collect(ctx, mp, failedOut, false, seqs, attrs, typs)
	failedOut.Clean(mp)
	require.ErrorContains(t, err, "injected post-persist fs write failure")
	failedName := fs.failedAtName()
	require.NotEmpty(t, failedName)
	fs.stopPostPersistFailure()
	txn.Lock()
	txn.drainUnpublishedDumpReservationsLocked(drainUnpublishedDumpReservationsTerminalReason)
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.NoError(t, txn.engine.DrainUnpublishedPendingForTest(ctx))
	issue25590RequireGone(t, fs, failedName, "failed replacement is reclaimed")
	issue25590RequireKept(t, fs, shared.ObjectName().String(), "committed shared source survives mixed collection failure")
}
