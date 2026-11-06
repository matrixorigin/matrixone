// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestRangesOnePartSlowUncommittedOwnership(t *testing.T) {
	ctx := context.Background()
	makeObject := func(id byte) objectio.ObjectStats {
		objID := types.Objectid{id}
		obj := objectio.NewObjectStatsWithObjectID(&objID, false, true, true)
		require.NoError(t, objectio.SetObjectStatsBlkCnt(obj, 1))
		require.NoError(t, objectio.SetObjectStatsRowCnt(obj, 1))
		require.NoError(t, objectio.SetObjectStatsSize(obj, 1))
		return *obj
	}
	uncommitted, committed := makeObject(11), makeObject(22)
	state := logtailreplay.NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: committed, CreateTime: types.BuildTS(1, 0),
	}, false))
	txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	snapshot := timestamp.Timestamp{PhysicalTime: 2}
	txnOp.EXPECT().SnapshotTS().Return(snapshot).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Transaction{engine: &Engine{}}).AnyTimes()
	tbl := &txnTable{db: &txnDatabase{op: txnOp}, tableDef: &plan.TableDef{Name: "test"}}
	proc := testutil.NewProcess(t)
	node := &plan.Node{Stats: plan2.DefaultStats()}
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
	owner := int32(plan2.SimpleCharHashToRange(uncommitted.ObjectName().ObjectId()[:], 2))
	localCN := 1 - owner
	// An unevaluated fold must retain candidate blocks for residual evaluation,
	// and deliberately uses rangesOnePart's slow path without loading metadata.
	filters := []*plan.Expr{{Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{Id: 0}}}}
	seen := make(map[types.Blockid]int)
	for cn := int32(0); cn < 2; cn++ {
		rsp := &engine.RangesShuffleParam{Node: node, CNCNT: 2, CNIDX: cn, IsLocalCN: cn == localCN}
		param := engine.RangesParam{Rsp: rsp, BlockFilters: filters}
		var workspace []objectio.ObjectStats
		if cn == localCN {
			workspace = []objectio.ObjectStats{uncommitted}
			require.True(t, plan2.ShouldSkipObjByShuffle(rsp, &uncommitted))
		}
		var blocks objectio.BlockInfoSlice
		fast, err := readutil.TryFastFilterBlocks(ctx, snapshot, tbl.tableDef, param, state, nil, workspace, &blocks, nil, nil)
		require.NoError(t, err)
		require.False(t, fast, "regression must exercise the slow path")
		require.Zero(t, blocks.Len())
		require.NoError(t, tbl.rangesOnePart(ctx, state, snapshot, tbl.tableDef, param, &blocks, proc, workspace))
		var want []types.Blockid
		if cn == localCN {
			want = append(want, uncommitted.ConstructBlockInfo(0).BlockID)
		}
		if !plan2.ShouldSkipObjByShuffle(rsp, &committed) {
			want = append(want, committed.ConstructBlockInfo(0).BlockID)
		}
		var got []types.Blockid
		for i := 0; i < blocks.Len(); i++ {
			id := blocks.Get(i).BlockID
			got = append(got, id)
			seen[id]++
		}
		require.Equal(t, want, got)
	}
	require.Len(t, seen, 2)
	for _, count := range seen {
		require.Equal(t, 1, count)
	}
}

func TestRangesOnePartUsesCapturedSnapshot(t *testing.T) {
	ctx := context.Background()
	objID := types.Objectid{42}
	obj := objectio.NewObjectStatsWithObjectID(&objID, false, true, true)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(obj, 1))
	require.NoError(t, objectio.SetObjectStatsRowCnt(obj, 1))
	require.NoError(t, objectio.SetObjectStatsSize(obj, 1))
	state := logtailreplay.NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *obj, CreateTime: types.BuildTS(3, 0),
	}, false))
	oldSnapshot := timestamp.Timestamp{PhysicalTime: 2}
	newSnapshot := timestamp.Timestamp{PhysicalTime: 4}
	txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOp.EXPECT().SnapshotTS().Return(newSnapshot).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Transaction{engine: &Engine{}}).AnyTimes()
	tbl := &txnTable{db: &txnDatabase{op: txnOp}, tableDef: &plan.TableDef{Name: "test"}}
	proc := testutil.NewProcess(t)
	defer proc.Free()

	for _, tc := range []struct {
		name    string
		filters []*plan.Expr
		fast    bool
	}{
		{"fast", nil, true},
		{"slow", []*plan.Expr{{Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{Id: 0}}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			param := engine.RangesParam{BlockFilters: tc.filters}
			var direct objectio.BlockInfoSlice
			fast, err := readutil.TryFastFilterBlocks(ctx, oldSnapshot, tbl.tableDef, param, state, nil, nil, &direct, nil, nil)
			require.NoError(t, err)
			require.Equal(t, tc.fast, fast)
			for _, snapshot := range []struct {
				ts   timestamp.Timestamp
				want int
			}{{oldSnapshot, 0}, {newSnapshot, 1}} {
				var blocks objectio.BlockInfoSlice
				require.NoError(t, tbl.rangesOnePart(ctx, state, snapshot.ts, tbl.tableDef, param, &blocks, proc, nil))
				require.Equal(t, snapshot.want, blocks.Len())
			}
		})
	}
}

func TestTombstoneReadersUseCapturedSnapshot(t *testing.T) {
	ctx := context.Background()
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	tbl.fake = false
	tbl.remoteWorkspace = true
	tbl.createdInTxn = true
	txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 4}).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Transaction{engine: eng}).AnyTimes()
	tbl.db.op = txnOp
	part := eng.GetOrCreateLatestPart(ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	state, commit := part.MutateState()
	stats := testTombstoneStats(10, 20)
	require.NoError(t, objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectName(objectio.NewSegmentid(), 0)))
	require.NoError(t, objectio.SetObjectStatsSize(&stats, 1))
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: stats, CreateTime: types.BuildTS(3, 0),
	}, true))
	commit()
	state = part.Snapshot()

	for _, tc := range []struct {
		name     string
		snapshot timestamp.Timestamp
		want     bool
	}{{"before tombstone", timestamp.Timestamp{PhysicalTime: 2}, false}, {"after tombstone", timestamp.Timestamp{PhysicalTime: 4}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			source, err := NewLocalDataSource(ctx, tbl, types.TimestampToTS(tc.snapshot), 0, state, nil, nil, false, engine.Policy_CheckAll, engine.GeneralLocalDataSource)
			require.NoError(t, err)
			defer source.Close()
			require.NoError(t, source.initPStateTombstoneObjectIndex())
			require.Equal(t, tc.want, len(source.pStateTombstoneObjects.index.objects) != 0)
			tombstones, err := tbl.collectTombstonesAt(ctx, 0, engine.Policy_CollectCommittedTombstones, tc.snapshot)
			require.NoError(t, err)
			require.Equal(t, tc.want, tombstones.HasAnyTombstoneFile())
		})
	}
	current, err := tbl.CollectTombstones(ctx, 0, engine.Policy_CollectCommittedTombstones)
	require.NoError(t, err)
	require.True(t, current.HasAnyTombstoneFile())
}

func TestCollectTombstonesWaitsForCapturedSnapshot(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 150}).AnyTimes()
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 150}}).AnyTimes()
	tbl.db.op = txnOp
	eng.pClient.subscribed.m[tbl.tableId] = &subEntry{dbID: tbl.db.databaseId, state: Subscribed}
	part := eng.GetOrCreateLatestPart(context.Background(), uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	state, commit := part.MutateState()
	state.UpdateAppliedTo(types.BuildTS(75, 0))
	commit()
	eng.pClient.subscribed.setTablePendingUpdate(tbl.db.databaseId, tbl.tableId, timestamp.Timestamp{PhysicalTime: 100})

	// The partition is ready for 50 but has a pending update before the live
	// transaction snapshot 150. A reader fixed at 50 must not wait for 150.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, err := tbl.collectTombstonesAt(ctx, 0, engine.Policy_CollectCommittedTombstones, timestamp.Timestamp{PhysicalTime: 50})
	require.NoError(t, err)
	_, err = extractPStateFromRelData(ctx, tbl, readutil.NewBlockListRelationData(0), timestamp.Timestamp{PhysicalTime: 50})
	require.NoError(t, err)

	cancelled, stop := context.WithCancel(context.Background())
	stop()
	_, err = tbl.collectTombstonesAt(cancelled, 0, engine.Policy_CollectCommittedTombstones, timestamp.Timestamp{PhysicalTime: 150})
	require.ErrorIs(t, err, context.Canceled)
	readers, err := tbl.BuildReaders(cancelled, tbl.proc.Load(), nil, nil, 1, 0, false, engine.Policy_CheckAll, engine.FilterHint{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, readers)
}

func TestBuildReadersUsesOneSnapshotWithoutRanges(t *testing.T) {
	ctx := context.Background()
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	proc := tbl.proc.Load()
	mp := proc.Mp()
	eng.fs = proc.GetFileService()
	tbl.tableDef = &plan.TableDef{
		Name:          "snapshot_reader",
		Cols:          []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0}},
		Name2ColIndex: map[string]int32{"id": 0},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
	txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	calls := 0
	txnOp.EXPECT().SnapshotTS().DoAndReturn(func() timestamp.Timestamp {
		calls++
		if calls == 1 {
			return timestamp.Timestamp{PhysicalTime: 100}
		}
		return timestamp.Timestamp{PhysicalTime: 200}
	}).AnyTimes()
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 200}}).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Transaction{engine: eng}).AnyTimes()
	txnOp.EXPECT().IsSnapOp().Return(false).AnyTimes()
	tbl.db.op = txnOp
	eng.pClient.subscribed.m[tbl.tableId] = &subEntry{dbID: tbl.db.databaseId, state: Subscribed}
	part := eng.GetOrCreateLatestPart(ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	rowIDs := vector.NewVec(types.T_Rowid.ToType())
	timestamps := vector.NewVec(types.T_TS.ToType())
	ids := vector.NewVec(types.T_int64.ToType())
	defer rowIDs.Free(mp)
	defer timestamps.Free(mp)
	defer ids.Free(mp)
	objectID := types.Uuid{42}
	blockID := objectio.NewBlockid(&objectID, 0, 1)
	for i, ts := range []int64{50, 150} {
		require.NoError(t, vector.AppendFixed(rowIDs, types.NewRowid(blockID, uint32(i)), false, mp))
		require.NoError(t, vector.AppendFixed(timestamps, types.BuildTS(ts, 0), false, mp))
		require.NoError(t, vector.AppendFixed(ids, int64(i+1), false, mp))
	}
	packer := types.NewPacker()
	defer packer.Close()
	state, commit := part.MutateState()
	state.HandleRowsInsert(ctx, &api.Batch{
		Attrs: []string{"rowid", "time", "id"},
		Vecs: []api.Vector{
			mustVectorToProtoForMaterializedSnapshotTest(t, rowIDs),
			mustVectorToProtoForMaterializedSnapshotTest(t, timestamps),
			mustVectorToProtoForMaterializedSnapshotTest(t, ids),
		},
	}, 0, packer, mp)
	commit()
	readers, err := tbl.BuildReaders(ctx, proc, nil, nil, 1, 0, false, engine.Policy_CheckCommittedOnly, engine.FilterHint{})
	require.NoError(t, err)
	require.Len(t, readers, 1)
	defer readers[0].Close()
	out := batch.NewWithSchema(false, []string{"id"}, []types.Type{types.T_int64.ToType()})
	defer out.Clean(mp)
	var got []int64
	for {
		out.CleanOnlyData()
		end, e := readers[0].Read(ctx, []string{"id"}, nil, mp, out)
		require.NoError(t, e)
		if end {
			break
		}
		got = append(got, vector.MustFixedColNoTypeCheck[int64](out.Vecs[0])...)
	}
	require.Equal(t, []int64{1}, got)
	require.Equal(t, 1, calls)
}

func TestTableMetaReaderUsesOneSnapshot(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	calls := 0
	txnOp.EXPECT().SnapshotTS().DoAndReturn(func() timestamp.Timestamp {
		calls++
		if calls == 1 {
			return timestamp.Timestamp{PhysicalTime: 50}
		}
		return timestamp.Timestamp{PhysicalTime: 150}
	}).AnyTimes()
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 150}}).AnyTimes()
	txnOp.EXPECT().IsSnapOp().Return(false).AnyTimes()
	workspace := &Transaction{engine: eng, proc: tbl.proc.Load(), op: txnOp}
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	tbl.db.op = txnOp
	eng.pClient.subscribed.m[tbl.tableId] = &subEntry{dbID: tbl.db.databaseId, state: Subscribed}
	part := eng.GetOrCreateLatestPart(context.Background(), uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	state, commit := part.MutateState()
	state.UpdateAppliedTo(types.BuildTS(75, 0))
	commit()
	eng.pClient.subscribed.setTablePendingUpdate(tbl.db.databaseId, tbl.tableId, timestamp.Timestamp{PhysicalTime: 100})

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	reader, err := NewTableMetaReader(ctx, tbl)
	require.NoError(t, err)
	defer reader.Close()
	require.Equal(t, types.BuildTS(50, 0), reader.(*TableMetaReader).snapshot)
	require.Equal(t, 1, calls)

	cancelled, stop := context.WithCancel(context.Background())
	stop()
	reader, err = NewTableMetaReader(cancelled, tbl)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, reader)
}

func TestStatementRangesChargeSelectedBlocksAndMatchFullReader(t *testing.T) {
	tbl, eng, fs, _ := newPersistedPKCheckTable(t)
	proc, mp := tbl.proc.Load(), tbl.proc.Load().Mp()
	ctx := proc.Ctx
	part := eng.GetOrCreateLatestPart(ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	data := batch.NewWithSize(2)
	data.Vecs[0], data.Vecs[1] = vector.NewVec(types.T_int64.ToType()), vector.NewVec(types.T_TS.ToType())
	defer data.Clean(mp)
	writer := ioutil.ConstructWriter(0, []uint16{0, objectio.SEQNUM_COMMITTS}, 0, true, false, fs)
	// Three full persisted blocks suffice to exceed the old 1 KiB metadata
	// budget. Full non-final blocks preserve the storage writer's row layout.
	values := make([]int64, objectio.BlockMaxRows)
	for block := range 3 {
		for row := range values {
			values[row] = int64(1000 + block*len(values) + row)
		}
		require.NoError(t, vector.AppendFixedList(data.Vecs[0], values, nil, mp))
		require.NoError(t, vector.AppendMultiFixed(data.Vecs[1], types.BuildTS(20, 0), false, len(values), mp))
		data.SetRowCount(len(values))
		_, err := writer.WriteBatch(data)
		require.NoError(t, err)
		data.CleanOnlyData()
	}
	_, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	state, publish := part.MutateState()
	require.NoError(t, state.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{ObjectStats: writer.GetObjectStats(), CreateTime: types.BuildTS(40, 0)}, false))
	publish()
	snapshot := timestamp.Timestamp{PhysicalTime: 60}
	read := func(blocks objectio.BlockInfoSlice, expr *plan.Expr) map[int64]types.Rowid {
		t.Helper()
		ds, err := NewLocalDataSource(ctx, tbl, types.TimestampToTS(snapshot), 0, state, blocks, nil, false, engine.Policy_CheckCommittedOnly, engine.GeneralLocalDataSource)
		require.NoError(t, err)
		ds.statementView = true
		ds.rc.prefetchDisabled = true // Synchronous IO keeps this fixture's ownership local.
		reader, err := readutil.NewReader(ctx, mp, eng.packerPool, fs, tbl.tableDef, snapshot, expr, ds, readutil.GetThresholdForReader(1), engine.FilterHint{})
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()
		result := batch.NewWithSize(2)
		result.Vecs[0], result.Vecs[1] = vector.NewVec(types.T_int64.ToType()), vector.NewVec(types.T_Rowid.ToType())
		defer result.Clean(mp)
		out := make(map[int64]types.Rowid)
		for {
			result.CleanOnlyData()
			end, err := reader.Read(ctx, []string{"pk", catalog.Row_ID}, expr, mp, result)
			require.NoError(t, err)
			for row := range result.RowCount() {
				out[vector.GetFixedAtNoTypeCheck[int64](result.Vecs[0], row)] = vector.GetFixedAtNoTypeCheck[types.Rowid](result.Vecs[1], row)
			}
			if end {
				break
			}
		}
		return out
	}
	var all objectio.BlockInfoSlice
	all.AppendBlockInfo(&objectio.EmptyBlockInfo)
	require.NoError(t, tbl.rangesOnePart(ctx, state, snapshot, tbl.tableDef, engine.RangesParam{}, &all, proc, nil))
	require.Equal(t, 5, all.Len())
	reference := read(all, nil)
	require.Len(t, reference, 1+3*objectio.BlockMaxRows)
	generation, err := proc.GetHashBuildBudget()
	require.NoError(t, err)
	reserve := func(n uint64) (engine.StatementViewReservation, error) {
		if generation.Snapshot().Used+n > 1<<10 {
			return nil, mpool.ErrAllocationAccountCapacity
		}
		return generation.ReserveMemory(ctx, n)
	}
	require.Greater(t, uint64(4*all.Len()*objectio.BlockInfoSize), uint64(1<<10), "old whole-table admission cannot fit")
	for _, key := range []int64{7, 8} {
		v := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(v, key, false, mp))
		encoded, err := v.MarshalBinary()
		v.Free(mp)
		require.NoError(t, err)
		expr := plan2.MakeInExpr(ctx, readutil.NewColumnExpr(0, tbl.tableDef.Cols[0].Typ, "pk"), 1, encoded, false)
		selected := statementKeyRanges{}
		appendBlock := func(out *objectio.BlockInfoSlice, b *objectio.BlockInfo) error {
			return selected.append(ctx, reserve, out, b)
		}
		require.NoError(t, appendBlock(&selected.blocks, &objectio.EmptyBlockInfo))
		require.NoError(t, tbl.rangesOnePart(ctx, state, snapshot, tbl.tableDef, engine.RangesParam{BlockFilters: []*plan.Expr{expr}, BlockAppender: appendBlock}, &selected.blocks, proc, nil))
		want := make(map[int64]types.Rowid)
		if rowid, ok := reference[key]; ok {
			want[key] = rowid
		}
		require.Equal(t, want, read(selected.blocks, expr))
		require.Equal(t, 1+len(want), selected.blocks.Len(), "only the hit block and memory sentinel are retained")
		require.Less(t, generation.Snapshot().Used, uint64(1<<10))
		selected.release()
		require.Zero(t, generation.Snapshot().Used)
	}
	// Rejected growth leaves the old collector and its charge intact.
	selected := statementKeyRanges{}
	for range 2 {
		require.NoError(t, selected.append(ctx, reserve, &selected.blocks, &objectio.EmptyBlockInfo))
	}
	before := append([]byte(nil), selected.blocks...)
	used := generation.Snapshot().Used
	require.ErrorIs(t, selected.append(ctx, func(uint64) (engine.StatementViewReservation, error) { return nil, mpool.ErrAllocationAccountCapacity }, &selected.blocks, &objectio.EmptyBlockInfo), mpool.ErrAllocationAccountCapacity)
	require.Equal(t, before, []byte(selected.blocks))
	require.Equal(t, used, generation.Snapshot().Used)
	cancelled, cancel := context.WithCancelCause(ctx)
	cause := moerr.NewTxnNeedRetryNoCtx()
	cancel(cause)
	admitted := false
	require.Same(t, cause, selected.append(cancelled, func(uint64) (engine.StatementViewReservation, error) { admitted = true; return nil, nil }, &selected.blocks, &objectio.EmptyBlockInfo))
	require.False(t, admitted)
	selected.release()
	require.Zero(t, generation.Snapshot().Used)

	// Both range engines must propagate rejected growth, without converting
	// admission failure to an unsupported-filter fallback or a missing row.
	for _, filters := range [][]*plan.Expr{nil, {{Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{Id: 0}}}}} {
		failure := mpool.ErrAllocationAccountCapacity
		var selected objectio.BlockInfoSlice
		param := engine.RangesParam{BlockFilters: filters, BlockAppender: func(*objectio.BlockInfoSlice, *objectio.BlockInfo) error { return failure }}
		require.ErrorIs(t, tbl.rangesOnePart(ctx, state, snapshot, tbl.tableDef, param, &selected, proc, nil), failure)
		require.Empty(t, selected)
	}
}
