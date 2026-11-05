//go:build mo_m0

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
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// This is an information-boundary counterexample, not a claim that a
// production partitioned table can contain the same block in two partitions.
// If it can, a flattened combined tombstone stream alone cannot distinguish
// which relation owns a delete of that shared row.
func TestM0CombinedTombstonesLoseSharedRowOwnership(t *testing.T) {
	row := types.RandomRowid()
	makeWire := func(deletedPartition int) []byte {
		relations := make([]engine.Relation, 0, 2)
		for i := 0; i < 2; i++ {
			tombstones := readutil.NewEmptyTombstoneData()
			if i == deletedPartition {
				require.NoError(t, tombstones.AppendInMemory(row))
				tombstones.SortInMemory()
			}
			got := tombstones.ApplyInMemTombstones(row.BorrowBlockID(), []int64{int64(row.GetRowOffset())}, nil)
			if i == deletedPartition {
				require.Empty(t, got)
			} else {
				require.Len(t, got, 1)
			}
			relations = append(relations, &mockRelation{
				collectTombstonesFunc: func(context.Context, int, engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
					return tombstones, nil
				},
			})
		}
		combined := &combinedTxnTable{tablesFunc: func(context.Context) ([]engine.Relation, error) { return relations, nil }}
		flattened, err := combined.CollectTombstones(t.Context(), 0, engine.Policy_CollectAllTombstones)
		require.NoError(t, err)
		var encoded bytes.Buffer
		require.NoError(t, flattened.MarshalBinaryWithBuffer(&encoded))
		return encoded.Bytes()
	}
	leftDeleted := makeWire(0)
	rightDeleted := makeWire(1)
	require.Equal(t, leftDeleted, rightDeleted, "flattened payload cannot identify which partition owned the delete")
}

// The states and shared row below are synthetic. They show why removing the
// combined-range guard and applying the flattened sender data to every local
// reader would be unsafe if distinct members can share a physical row.
func TestM0FlattenedTombstoneCrossesSharedRelationBoundary(t *testing.T) {
	ctx := t.Context()
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	input := batch.NewWithSize(4)
	input.Attrs = []string{"rowid", "time", "pk", "tbrowid"}
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	input.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	input.Vecs[3] = vector.NewVec(types.T_Rowid.ToType())
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	row := types.RandomRowid()
	require.NoError(t, vector.AppendFixed(input.Vecs[0], row, false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], types.BuildTS(2, 0), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[2], int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[3], types.RandomRowid(), false, proc.Mp()))
	input.SetRowCount(1)
	pb, err := batch.BatchToProtoBatch(input)
	require.NoError(t, err)
	packer := types.NewPacker()
	defer packer.Close()
	deletedState := logtailreplay.NewPartitionState("", true, 41, false)
	deletedState.HandleRowsDelete(ctx, pb, packer, proc.Mp())
	visibleState := logtailreplay.NewPartitionState("", true, 42, false)
	policy := engine.TombstoneApplyPolicy(engine.Policy_SkipUncommitedInMemory | engine.Policy_SkipUncommitedS3 | engine.Policy_SkipCommittedS3)
	read := func(state *logtailreplay.PartitionState, sent engine.Tombstoner) []int64 {
		t.Helper()
		readCtx := ctx
		if sent != nil {
			readCtx = m0read.WithSingleRemote(ctx)
		}
		source := &LocalDisttaeDataSource{ctx: readCtx, category: engine.GeneralLocalDataSource,
			pState: state, snapshotTS: types.BuildTS(3, 0), tombstonePolicy: policy, extraTombstones: sent}
		left, err := source.ApplyTombstones(readCtx, row.BorrowBlockID(), []int64{int64(row.GetRowOffset())}, engine.Policy_CheckAll)
		require.NoError(t, err)
		return left
	}
	require.Empty(t, read(deletedState, nil))
	require.Len(t, read(visibleState, nil), 1)
	flattened := readutil.NewEmptyTombstoneData()
	require.NoError(t, flattened.AppendInMemory(row))
	flattened.SortInMemory()
	require.Empty(t, read(visibleState, flattened), "unscoped sender data masks another relation's row")
}

// The table-local no-pending fast path assumes a separate global logtail
// watermark. Without that assumption, a state can be admitted although it
// lacks a delete visible at the requested snapshot. This test does not model
// a normal CN with its configured timestamp waiter or a live remote run.
func TestM0ReceiverLocalStateNeedsGlobalWatermark(t *testing.T) {
	row := types.RandomRowid()
	snapshot := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	state := logtailreplay.NewPartitionState("", true, 42, false)
	state.UpdateDuration(types.TS{}, types.MaxTs())
	state.UpdateAppliedTo(types.BuildTS(10, 0))
	canServe, needWait := canServeTableSnapshotWithPending(state, snapshot, false)
	require.True(t, canServe)
	require.False(t, needWait)
	actual := state.GetAppliedTo()
	needed := types.TimestampToTS(snapshot.Prev())
	require.False(t, actual.GE(&needed), "fixture must lack a table-local freshness proof")
	policy := engine.TombstoneApplyPolicy(engine.Policy_SkipUncommitedInMemory | engine.Policy_SkipUncommitedS3 | engine.Policy_SkipCommittedS3)
	source := &LocalDisttaeDataSource{ctx: t.Context(), pState: state,
		snapshotTS: types.TimestampToTS(snapshot), tombstonePolicy: policy}
	left, err := source.ApplyTombstones(t.Context(), row.BorrowBlockID(), []int64{int64(row.GetRowOffset())}, engine.Policy_CheckAll)
	require.NoError(t, err)
	require.Len(t, left, 1, "stale local state alone cannot reconstruct the missing delete")
	sent := readutil.NewEmptyTombstoneData()
	require.NoError(t, sent.AppendInMemory(row))
	sent.SortInMemory()
	require.Empty(t, sent.ApplyInMemTombstones(row.BorrowBlockID(), []int64{int64(row.GetRowOffset())}, nil))
}

// A clone may share an immutable block with its parent. A tombstone written
// for the child must not be shipped to a scan of the parent in that txn.
func TestM0SenderTombstonesRespectSharedBlockRelation(t *testing.T) {
	ctx := t.Context()
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	op, closeOp := client.NewTestTxnOperator(context.Background())
	t.Cleanup(closeOp)
	workspace := &Transaction{proc: proc, op: op, deletedBlocks: &deletedBlocks{offsets: make(map[types.Blockid][]int64)}, cn_flushed_s3_tombstone_object_stats_list: new(sync.Map)}
	op.AddWorkspace(workspace)
	t.Cleanup(func() {
		for _, entry := range workspace.writes {
			if entry.bat != nil {
				entry.bat.Clean(proc.Mp())
			}
		}
	})
	pkType := types.T_int32.ToType()
	writer := colexec.NewCNS3TombstoneWriter(proc.Mp(), fs, pkType, -1)
	t.Cleanup(func() { _ = writer.Close() })
	rows := readutil.NewCNTombstoneBatch(&pkType, objectio.HiddenColumnSelection_None)
	t.Cleanup(func() { rows.Clean(proc.Mp()) })
	row := types.RandomRowid()
	require.NoError(t, vector.AppendFixed(rows.Vecs[0], row, false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(rows.Vecs[1], int32(1), false, proc.Mp()))
	rows.SetRowCount(1)
	require.NoError(t, writer.Write(ctx, rows))
	stats, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	result := colexec.AllocCNS3ResultBat(true)
	t.Cleanup(func() { result.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendBytes(result.Vecs[0], stats[0].Marshal(), false, proc.Mp()))
	result.SetRowCount(1)
	const databaseID, parentID, childID = 1, 101, 102
	require.NoError(t, workspace.WriteFile(DELETE, 0, databaseID, childID, "db", "child", "child-tombstone", result, DNStore{}))
	check := func(t *testing.T, tableID uint64, offset int) bool {
		t.Helper()
		table := &txnTable{tableId: tableID, db: &txnDatabase{op: op, databaseId: databaseID}}
		tombstones, err := table.CollectTombstones(ctx, offset, engine.Policy_CollectUncommittedTombstones)
		require.NoError(t, err)
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		snapshot := types.MaxTs()
		_, err = tombstones.ApplyPersistedTombstones(ctx, fs, &snapshot, row.BorrowBlockID(), nil, &mask)
		require.NoError(t, err)
		return mask.Contains(uint64(row.GetRowOffset()))
	}
	t.Run("own relation", func(t *testing.T) {
		require.True(t, check(t, childID, len(workspace.writes)), "child must see its own uncommitted delete")
	})
	t.Run("shared parent", func(t *testing.T) {
		require.False(t, check(t, parentID, len(workspace.writes)), "parent must not inherit the child's uncommitted delete")
	})
	t.Run("earlier write offset", func(t *testing.T) {
		require.False(t, check(t, childID, 0), "earlier offsets must not include later tombstones")
	})
	t.Run("different account", func(t *testing.T) {
		table := &txnTable{accountId: 1, tableId: childID, db: &txnDatabase{op: op, databaseId: databaseID}}
		stats, err := m0ScopedFlushedTombstones(table, len(workspace.writes), false)
		require.NoError(t, err)
		require.Empty(t, stats)
	})
	checkLocal := func(t *testing.T, tableID uint64, offset int, locked bool) bool {
		t.Helper()
		table := &txnTable{tableId: tableID, db: &txnDatabase{op: op, databaseId: databaseID}}
		source := &LocalDisttaeDataSource{ctx: ctx, fs: fs, table: table, txnOffset: offset, snapshotTS: types.MaxTs()}
		if locked {
			workspace.Lock()
			defer workspace.Unlock()
			source.rc.WorkspaceLocked = true
		}
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		_, err := source.applyWorkspaceFlushedS3Deletes(row.BorrowBlockID(), nil, &mask)
		require.NoError(t, err)
		return mask.Contains(uint64(row.GetRowOffset()))
	}
	t.Run("local own relation", func(t *testing.T) {
		require.True(t, checkLocal(t, childID, len(workspace.writes), false))
	})
	t.Run("local shared parent", func(t *testing.T) {
		require.False(t, checkLocal(t, parentID, len(workspace.writes), false))
	})
	t.Run("local earlier write offset", func(t *testing.T) {
		require.False(t, checkLocal(t, childID, 0, false))
	})
	t.Run("local locked own relation", func(t *testing.T) {
		require.True(t, checkLocal(t, childID, len(workspace.writes), true))
	})
	t.Run("local locked shared parent", func(t *testing.T) {
		require.False(t, checkLocal(t, parentID, len(workspace.writes), true))
	})
	t.Run("local locked earlier write offset", func(t *testing.T) {
		require.False(t, checkLocal(t, childID, 0, true))
	})
	t.Run("single remote reader applies shipped file", func(t *testing.T) {
		shipped := readutil.NewEmptyTombstoneData()
		require.NoError(t, shipped.AppendFiles(stats[0]))
		remoteCtx := m0read.WithSingleRemote(ctx)
		source := &LocalDisttaeDataSource{ctx: remoteCtx, fs: fs, category: engine.GeneralLocalDataSource,
			pState: logtailreplay.NewPartitionState("", true, childID, false), snapshotTS: types.MaxTs(),
			extraTombstones: shipped, tombstonePolicy: engine.Policy_SkipAll}
		left, err := source.ApplyTombstones(remoteCtx, row.BorrowBlockID(), []int64{int64(row.GetRowOffset())}, engine.Policy_CheckAll)
		require.NoError(t, err)
		require.Empty(t, left)
		mask, err := source.GetTombstones(remoteCtx, row.BorrowBlockID())
		require.NoError(t, err)
		defer mask.Release()
		require.True(t, mask.Contains(uint64(row.GetRowOffset())))
	})
	t.Run("ownerless tombstone is rejected", func(t *testing.T) {
		var orphan objectio.ObjectStats
		objectio.SetObjectStatsLocation(&orphan, objectio.NewRandomLocation(1, 1111))
		workspace.StashFlushedTombstones(orphan)
		t.Cleanup(func() { workspace.cn_flushed_s3_tombstone_object_stats_list.Delete(orphan) })
		table := &txnTable{tableId: childID, db: &txnDatabase{op: op, databaseId: databaseID}}
		_, err := table.CollectTombstones(ctx, len(workspace.writes), engine.Policy_CollectUncommittedTombstones)
		require.Error(t, err)
		source := &LocalDisttaeDataSource{ctx: ctx, fs: fs, table: table, txnOffset: len(workspace.writes), snapshotTS: types.MaxTs()}
		_, err = source.applyWorkspaceFlushedS3Deletes(row.BorrowBlockID(), nil, nil)
		require.Error(t, err)
	})
}

func TestM0SingleRemoteLocalReaderUsesShippedTombstones(t *testing.T) {
	row := types.RandomRowid()
	tombstones := readutil.NewEmptyTombstoneData()
	require.NoError(t, tombstones.AppendInMemory(row))
	tombstones.SortInMemory()
	for _, tc := range []struct {
		name    string
		marked  bool
		visible bool
	}{{"ordinary general reader", false, true}, {"single remote reader", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			if tc.marked {
				ctx = m0read.WithSingleRemote(ctx)
			}
			source := &LocalDisttaeDataSource{ctx: ctx, category: engine.GeneralLocalDataSource,
				pState:          logtailreplay.NewPartitionState("", true, 42, false),
				extraTombstones: tombstones, tombstonePolicy: engine.Policy_SkipAll}
			left, err := source.ApplyTombstones(ctx, row.BorrowBlockID(), []int64{int64(row.GetRowOffset())}, engine.Policy_CheckAll)
			require.NoError(t, err)
			require.Equal(t, tc.visible, len(left) == 1)
			mask, err := source.GetTombstones(ctx, row.BorrowBlockID())
			require.NoError(t, err)
			defer mask.Release()
			require.Equal(t, !tc.visible, mask.Contains(uint64(row.GetRowOffset())))
		})
	}
}

// A fresh reader must apply committed deletes from its own snapshot state,
// including when no sender tombstone object is attached to the relation data.
func TestM0LocalReaderUsesSnapshotCommittedDelete(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	input := batch.NewWithSize(4)
	input.Attrs = []string{"rowid", "time", "pk", "tbrowid"}
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	input.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	input.Vecs[3] = vector.NewVec(types.T_Rowid.ToType())
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	row := types.RandomRowid()
	require.NoError(t, vector.AppendFixed(input.Vecs[0], row, false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], types.BuildTS(2, 0), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[2], int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[3], types.RandomRowid(), false, proc.Mp()))
	input.SetRowCount(1)
	pb, err := batch.BatchToProtoBatch(input)
	require.NoError(t, err)
	packer := types.NewPacker()
	defer packer.Close()
	state := logtailreplay.NewPartitionState("", true, 42, false)
	state.HandleRowsDelete(t.Context(), pb, packer, proc.Mp())
	sibling := logtailreplay.NewPartitionState("", true, 43, false)
	for _, tc := range []struct {
		name    string
		ts      types.TS
		state   *logtailreplay.PartitionState
		visible bool
	}{
		{"before commit", types.BuildTS(1, 0), state, true},
		{"after commit", types.BuildTS(3, 0), state, false},
		{"sibling with same row after commit", types.BuildTS(3, 0), sibling, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &LocalDisttaeDataSource{ctx: t.Context(), pState: tc.state, snapshotTS: tc.ts,
				tombstonePolicy: engine.Policy_SkipUncommitedInMemory | engine.Policy_SkipUncommitedS3 | engine.Policy_SkipCommittedS3}
			left, err := source.ApplyTombstones(t.Context(), row.BorrowBlockID(), []int64{int64(row.GetRowOffset())}, engine.Policy_CheckAll)
			require.NoError(t, err)
			require.Equal(t, tc.visible, len(left) == 1)
			mask, err := source.GetTombstones(t.Context(), row.BorrowBlockID())
			require.NoError(t, err)
			defer mask.Release()
			require.Equal(t, !tc.visible, mask.Contains(uint64(row.GetRowOffset())))
		})
	}
}

func TestM0LocalReaderUsesSnapshotPersistedTombstone(t *testing.T) {
	ctx := t.Context()
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	pkType := types.T_int32.ToType()
	rows := readutil.NewCNTombstoneBatch(&pkType, objectio.HiddenColumnSelection_None)
	t.Cleanup(func() { rows.Clean(proc.Mp()) })
	writer := colexec.NewCNS3TombstoneWriter(proc.Mp(), fs, pkType, -1)
	t.Cleanup(func() { _ = writer.Close() })
	row := types.RandomRowid()
	require.NoError(t, vector.AppendFixed(rows.Vecs[0], row, false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(rows.Vecs[1], int32(1), false, proc.Mp()))
	rows.SetRowCount(1)
	require.NoError(t, writer.Write(ctx, rows))
	stats, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.True(t, stats[0].GetCNCreated(), "fixture must exercise CN-created tombstone decoding")
	state := logtailreplay.NewPartitionState("", true, 42, false)
	state.UpdateDuration(types.BuildTS(0, 0), types.MaxTs())
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: stats[0], CreateTime: types.BuildTS(2, 0),
	}, true))
	for _, tc := range []struct {
		name    string
		ts      types.TS
		visible bool
	}{{"before commit", types.BuildTS(1, 0), true}, {"after commit", types.BuildTS(3, 0), false}} {
		t.Run(tc.name, func(t *testing.T) {
			source := &LocalDisttaeDataSource{ctx: ctx, fs: fs, pState: state, snapshotTS: tc.ts,
				tombstonePolicy: engine.Policy_SkipUncommitedInMemory | engine.Policy_SkipUncommitedS3 | engine.Policy_SkipCommittedInMemory}
			left, err := source.ApplyTombstones(ctx, row.BorrowBlockID(), []int64{int64(row.GetRowOffset())}, engine.Policy_CheckAll)
			require.NoError(t, err)
			require.Equal(t, tc.visible, len(left) == 1)
			otherOffset := int64(row.GetRowOffset() ^ 1)
			other, err := source.ApplyTombstones(ctx, row.BorrowBlockID(), []int64{otherOffset}, engine.Policy_CheckAll)
			require.NoError(t, err)
			require.Equal(t, []int64{otherOffset}, other)
			mask, err := source.GetTombstones(ctx, row.BorrowBlockID())
			require.NoError(t, err)
			defer mask.Release()
			require.Equal(t, !tc.visible, mask.Contains(uint64(row.GetRowOffset())))
		})
	}
}

func TestM0LocalReaderUsesTNCreatedTombstoneCommitTS(t *testing.T) {
	ctx := t.Context()
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	rows := batch.NewWithSize(3)
	rows.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	rows.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	rows.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	t.Cleanup(func() { rows.Clean(proc.Mp()) })
	obj := objectio.NewObjectid()
	for i, commit := range []int64{2, 4} {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(obj, 0, uint32(7+i))
		require.NoError(t, vector.AppendFixed(rows.Vecs[0], row, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(rows.Vecs[1], int32(i), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(rows.Vecs[2], types.BuildTS(commit, 0), false, proc.Mp()))
	}
	rows.SetRowCount(2)
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_CommitTS, fs)
	writer.SetAppendable()
	_, err = writer.WriteBatch(rows)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats(objectio.WithAppendable())
	require.False(t, stats.GetCNCreated())
	require.True(t, stats.GetAppendable())
	state := logtailreplay.NewPartitionState("", true, 42, false)
	state.UpdateDuration(types.BuildTS(0, 0), types.MaxTs())
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: stats, CreateTime: types.BuildTS(1, 0),
	}, true))
	firstRow := types.NewRowIDWithObjectIDBlkNumAndRowID(obj, 0, 7)
	bid := firstRow.CloneBlockID()
	for _, tc := range []struct {
		name string
		ts   types.TS
		want []int64
	}{{"before first delete", types.BuildTS(1, 0), []int64{7, 8}},
		{"between deletes", types.BuildTS(3, 0), []int64{8}},
		{"after both deletes", types.BuildTS(5, 0), nil}} {
		t.Run(tc.name, func(t *testing.T) {
			source := &LocalDisttaeDataSource{ctx: ctx, fs: fs, pState: state, snapshotTS: tc.ts,
				tombstonePolicy: engine.Policy_SkipUncommitedInMemory | engine.Policy_SkipUncommitedS3 | engine.Policy_SkipCommittedInMemory}
			left, err := source.ApplyTombstones(ctx, &bid, []int64{7, 8}, engine.Policy_CheckAll)
			require.NoError(t, err)
			require.Len(t, left, len(tc.want))
			for i := range tc.want {
				require.Equal(t, tc.want[i], left[i])
			}
			mask, err := source.GetTombstones(ctx, &bid)
			require.NoError(t, err)
			defer mask.Release()
			require.Equal(t, len(tc.want), 2-mask.Count())
			visible := make(map[int64]bool, len(tc.want))
			for _, offset := range tc.want {
				visible[offset] = true
			}
			require.Equal(t, !visible[7], mask.Contains(7))
			require.Equal(t, !visible[8], mask.Contains(8))
		})
	}
}

func TestM0ScopedFlushedTombstonesAfterCCPRRollback(t *testing.T) {
	txn := newCatalogCreateTxnForTest(t)
	txn.isCCPRTxn = true
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.tableCache = new(sync.Map)
	txn.tableOps = newTableOps()
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	op := txn.op.(*mock_frontend.MockTxnOperator)
	op.EXPECT().EnterRollbackStmt()
	op.EXPECT().ExitRollbackStmt()
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	var stats objectio.ObjectStats
	objectio.SetObjectStatsLocation(&stats, objectio.NewRandomLocation(1, 1111))
	result := colexec.AllocCNS3ResultBat(true)
	defer result.Clean(txn.proc.Mp())
	require.NoError(t, vector.AppendBytes(result.Vecs[0], stats.Marshal(), false, txn.proc.Mp()))
	result.SetRowCount(1)
	txn.statementID = 1
	txn.offsets = []int{0}
	require.NoError(t, txn.WriteFile(DELETE, 0, 1, 102, "db", "child", "tombstone", result, DNStore{}))
	require.NoError(t, txn.RollbackLastStatement(t.Context()))
	table := &txnTable{tableId: 102, db: &txnDatabase{op: op, databaseId: 1}}
	visible, err := m0ScopedFlushedTombstones(table, 0, false)
	require.NoError(t, err)
	require.Empty(t, visible)
}

func TestM0ScopedFlushedTombstonesCCPRRollbackRetainsSharedReference(t *testing.T) {
	txn := newCatalogCreateTxnForTest(t)
	txn.isCCPRTxn = true
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.tableCache = new(sync.Map)
	txn.tableOps = newTableOps()
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	op := txn.op.(*mock_frontend.MockTxnOperator)
	op.EXPECT().EnterRollbackStmt()
	op.EXPECT().ExitRollbackStmt()
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	var stats objectio.ObjectStats
	objectio.SetObjectStatsLocation(&stats, objectio.NewRandomLocation(1, 1111))
	result := colexec.AllocCNS3ResultBat(true)
	defer result.Clean(txn.proc.Mp())
	require.NoError(t, vector.AppendBytes(result.Vecs[0], stats.Marshal(), false, txn.proc.Mp()))
	result.SetRowCount(1)
	require.NoError(t, txn.WriteFile(DELETE, 0, 1, 102, "db", "child", "tombstone", result, DNStore{}))
	t.Cleanup(func() { cleanTxnWritesForTest(txn) })
	txn.statementID = 1
	txn.offsets = []int{len(txn.writes)}
	require.NoError(t, txn.WriteFile(DELETE, 0, 1, 102, "db", "child", "tombstone", result, DNStore{}))
	require.NoError(t, txn.RollbackLastStatement(t.Context()))
	_, ok := txn.cn_flushed_s3_tombstone_object_stats_list.Load(stats)
	require.True(t, ok, "a live entry must retain its tombstone registration")
	table := &txnTable{tableId: 102, db: &txnDatabase{op: op, databaseId: 1}}
	visible, err := m0ScopedFlushedTombstones(table, len(txn.writes), false)
	require.NoError(t, err)
	require.Equal(t, []objectio.ObjectStats{stats}, visible)
}

func TestM0ScopedFlushedTombstonesCCPRDiscardedTable(t *testing.T) {
	for _, retain := range []bool{false, true} {
		t.Run(fmt.Sprintf("retain=%v", retain), func(t *testing.T) {
			txn := newCatalogCreateTxnForTest(t)
			txn.isCCPRTxn = true
			txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
			txn.tableOps = newTableOps()
			txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
			txn.tablesInVain[102] = 1
			op := txn.op.(*mock_frontend.MockTxnOperator)
			op.EXPECT().IsSnapOp().Return(false).AnyTimes()
			var stats objectio.ObjectStats
			objectio.SetObjectStatsLocation(&stats, objectio.NewRandomLocation(1, 1111))
			result := colexec.AllocCNS3ResultBat(true)
			defer result.Clean(txn.proc.Mp())
			require.NoError(t, vector.AppendBytes(result.Vecs[0], stats.Marshal(), false, txn.proc.Mp()))
			result.SetRowCount(1)
			require.NoError(t, txn.WriteFile(DELETE, 0, 1, 102, "db", "discard", "tombstone", result, DNStore{}))
			if retain {
				require.NoError(t, txn.WriteFile(DELETE, 0, 1, 103, "db", "retain", "tombstone", result, DNStore{}))
			}
			t.Cleanup(func() { cleanTxnWritesForTest(txn) })
			require.NoError(t, txn.mergeTxnWorkspaceLocked(t.Context()))
			_, ok := txn.cn_flushed_s3_tombstone_object_stats_list.Load(stats)
			require.Equal(t, retain, ok)
			table := &txnTable{tableId: 103, db: &txnDatabase{op: op, databaseId: 1}}
			visible, err := m0ScopedFlushedTombstones(table, len(txn.writes), false)
			require.NoError(t, err)
			require.Equal(t, retain, len(visible) == 1)
		})
	}
}
