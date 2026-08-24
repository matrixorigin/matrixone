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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
