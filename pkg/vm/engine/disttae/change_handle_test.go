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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/stretchr/testify/require"
)

func TestPartitionChangesHandleCloseWithTypedNil(t *testing.T) {
	var handle engine.ChangesHandle = (*PartitionChangesHandle)(nil)
	require.NoError(t, handle.Close())
}

func TestPartitionChangesHandleClose_CleansBufferedBatches(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(1), false, mp))
	data.SetRowCount(1)

	tombstone := batch.NewWithSize(1)
	tombstone.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(tombstone.Vecs[0], int64(2), false, mp))
	tombstone.SetRowCount(1)

	stub := &stubChangesHandle{}
	handle := &PartitionChangesHandle{
		mp:                  mp,
		currentChangeHandle: stub,
		bufferedBatches: []queuedChangeBatch{{
			data:      data,
			tombstone: tombstone,
		}},
	}

	require.NoError(t, handle.Close())
	require.True(t, stub.closed)
	require.Nil(t, handle.currentChangeHandle)
	require.Nil(t, handle.bufferedBatches)
}

func TestPartitionChangesHandleNextWithSnapshotRecovery_UsesBufferedBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(7), false, mp))
	data.SetRowCount(1)
	defer data.Clean(mp)

	handle := &PartitionChangesHandle{
		snapshotReadPolicy: engine.SnapshotReadPolicyVisibleState,
		bufferedBatches: []queuedChangeBatch{{
			data: data,
			hint: engine.ChangesHandle_Snapshot,
		}},
	}

	gotData, gotTombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Same(t, data, gotData)
	require.Nil(t, gotTombstone)
	require.Equal(t, engine.ChangesHandle_Snapshot, hint)
	require.Empty(t, handle.bufferedBatches)
}

func TestPartitionChangesHandleRejectsInvalidInputs(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	from, to := types.BuildTS(1, 0), types.BuildTS(2, 0)
	_, err := NewPartitionChangesHandle(
		context.Background(), nil, from, to, false,
		engine.SnapshotReadPolicyCheckpointReplay, mp,
	)
	require.Error(t, err)

	var nilHandle *PartitionChangesHandle
	_, _, _, err = nilHandle.Next(context.Background(), mp)
	require.Error(t, err)

	handle := &PartitionChangesHandle{}
	_, _, _, err = handle.Next(nil, mp)
	require.Error(t, err)
	_, _, _, err = handle.Next(context.Background(), nil)
	require.Error(t, err)
	_, _, _, err = handle.Next(context.Background(), mp)
	require.Error(t, err)
}

func TestPartitionChangesHandleBufferCurrentRangeCleansBatchReturnedWithError(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := batch.NewOffHeapWithSize(1)
	data.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(1), false, mp))
	data.SetRowCount(1)
	require.Positive(t, mp.CurrNB())

	wantErr := moerr.NewInternalErrorNoCtx("partial change batch")
	handle := &PartitionChangesHandle{
		currentChangeHandle: &stubChangesHandle{
			next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
				return data, nil, engine.ChangesHandle_Tail_wip, wantErr
			},
		},
	}

	require.ErrorIs(t, handle.bufferCurrentRange(context.Background(), mp), wantErr)
	require.Zero(t, mp.CurrNB())
}

func TestPartitionChangesHandleCloseUsesQueuedBatchAllocator(t *testing.T) {
	handleMP := mpool.MustNewZero()
	defer mpool.DeleteMPool(handleMP)
	batchMP := mpool.MustNewZero()
	defer mpool.DeleteMPool(batchMP)

	data := batch.NewOffHeapWithSize(1)
	data.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(1), false, batchMP))
	data.SetRowCount(1)
	require.Positive(t, batchMP.CurrNB())

	handle := &PartitionChangesHandle{
		mp: handleMP,
		bufferedBatches: []queuedChangeBatch{{
			data: data,
			mp:   batchMP,
		}},
	}
	require.NoError(t, handle.Close())
	require.Zero(t, batchMP.CurrNB())
	require.Zero(t, handleMP.CurrNB())
}

func TestPartitionChangesHandleLoadCheckpointEntriesRejectsMalformedResponse(t *testing.T) {
	oldSnapshotRead := RequestSnapshotRead
	t.Cleanup(func() { RequestSnapshotRead = oldSnapshotRead })

	start := types.BuildTS(2, 0).ToTimestamp()
	end := types.BuildTS(1, 0).ToTimestamp()
	tests := []struct {
		name  string
		entry *cmd_util.CheckpointEntryResp
	}{
		{name: "nil entry"},
		{name: "missing start", entry: &cmd_util.CheckpointEntryResp{End: &end}},
		{name: "missing end", entry: &cmd_util.CheckpointEntryResp{Start: &start}},
		{name: "reversed range", entry: &cmd_util.CheckpointEntryResp{Start: &start, End: &end}},
		{name: "invalid type", entry: &cmd_util.CheckpointEntryResp{Start: &end, End: &start, EntryType: 100}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			RequestSnapshotRead = func(context.Context, *txnTable, *types.TS) (any, error) {
				return &cmd_util.SnapshotReadResp{
					Succeed: true,
					Entries: []*cmd_util.CheckpointEntryResp{test.entry},
				}, nil
			}
			handle := &PartitionChangesHandle{tbl: &txnTable{}}
			_, _, _, err := handle.loadCheckpointEntries(context.Background(), types.BuildTS(1, 0))
			require.Error(t, err)
		})
	}
}

type stubChangesHandle struct {
	closed bool
	next   func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error)
}

func (s *stubChangesHandle) Next(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	if s.next != nil {
		return s.next(ctx, mp)
	}
	return nil, nil, engine.ChangesHandle_Tail_done, nil
}

func (s *stubChangesHandle) Close() error {
	s.closed = true
	return nil
}

func TestUseBoundedVisibleStateRangeForEmptyWatermark(t *testing.T) {
	base := context.Background()
	require.False(t, useBoundedVisibleStateRange(base))
	require.False(t, useBoundedVisibleStateRange(
		engine.WithSnapshotReadPolicy(base, engine.SnapshotReadPolicyVisibleState),
	))

	bounded := engine.WithChangeRangeLimit(base, engine.ChangeRangeLimit{
		MaxInMemoryBytes: 64 << 20,
	})
	require.False(t, useBoundedVisibleStateRange(bounded))
	require.True(t, useBoundedVisibleStateRange(
		engine.WithSnapshotReadPolicy(bounded, engine.SnapshotReadPolicyVisibleState),
	))
}

func TestCollectChangesRoutesBoundedEmptyWatermarkToVisibleStateRange(t *testing.T) {
	original := newPartitionChangesHandle
	t.Cleanup(func() { newPartitionChangesHandle = original })
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	_, err := original(
		context.Background(), &txnTable{}, types.BuildTS(1, 0), types.TS{}, false,
		engine.SnapshotReadPolicyVisibleState, mp,
	)
	require.Error(t, err)

	want := &stubChangesHandle{}
	called := false
	newPartitionChangesHandle = func(
		_ context.Context,
		_ *txnTable,
		from, to types.TS,
		skipDeletes bool,
		policy engine.SnapshotReadPolicy,
		_ *mpool.MPool,
	) (engine.ChangesHandle, error) {
		called = true
		require.True(t, from.IsEmpty())
		require.Equal(t, types.BuildTS(20, 0), to)
		require.False(t, skipDeletes)
		require.Equal(t, engine.SnapshotReadPolicyVisibleState, policy)
		return want, nil
	}
	ctx := engine.WithSnapshotReadPolicy(context.Background(), engine.SnapshotReadPolicyVisibleState)
	ctx = engine.WithChangeRangeLimit(ctx, engine.ChangeRangeLimit{MaxInMemoryBytes: 64 << 20})

	got, err := (&txnTable{}).CollectChanges(
		ctx, types.TS{}, types.BuildTS(20, 0), false, mp,
	)
	require.NoError(t, err)
	require.True(t, called)
	require.Same(t, want, got)
}

func TestCollectChangesInitializesLogicalSeqnumsBeforeBuildingRange(t *testing.T) {
	original := newPartitionChangesHandle
	t.Cleanup(func() { newPartitionChangesHandle = original })
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	var got []uint16
	newPartitionChangesHandle = func(
		_ context.Context, tbl *txnTable, _, _ types.TS, _ bool,
		_ engine.SnapshotReadPolicy, _ *mpool.MPool,
	) (engine.ChangesHandle, error) {
		got = append([]uint16(nil), tbl.seqnums...)
		return &stubChangesHandle{}, nil
	}
	tbl := &txnTable{tableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
		{Name: "added", Typ: plan2.Type{Id: int32(types.T_int32)}, Seqnum: 2},
		{Name: "id", Typ: plan2.Type{Id: int32(types.T_int32)}, Seqnum: 0},
		{Name: "payload", Typ: plan2.Type{Id: int32(types.T_varchar)}, Seqnum: 1},
		{Name: catalog.Row_ID, Typ: plan2.Type{Id: int32(types.T_Rowid)}, Seqnum: uint32(objectio.SEQNUM_ROWID)},
	}}}
	ctx := engine.WithSnapshotReadPolicy(context.Background(), engine.SnapshotReadPolicyVisibleState)
	ctx = engine.WithChangeRangeLimit(ctx, engine.ChangeRangeLimit{MaxInMemoryBytes: 64 << 20})
	_, err := tbl.CollectChanges(ctx, types.TS{}, types.BuildTS(20, 0), false, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{2, 0, 1}, got)
}

func TestDeferredChangesHandleBuildErrorDoesNotRetry(t *testing.T) {
	want := moerr.NewInternalErrorNoCtx("range construction failed")
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	builds := 0
	handle := &deferredChangesHandle{build: func(context.Context) (engine.ChangesHandle, error) {
		builds++
		return nil, want
	}}
	for range 2 {
		data, tombstone, hint, err := handle.Next(context.Background(), mp)
		require.Nil(t, data)
		require.Nil(t, tombstone)
		require.Equal(t, engine.ChangesHandle_Tail_done, hint)
		require.Equal(t, want, err)
	}
	require.Equal(t, 1, builds)
	require.NoError(t, handle.Close())
}
