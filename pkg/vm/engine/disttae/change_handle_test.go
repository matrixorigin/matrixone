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
	resources := newTestVisibleStateResources()
	resources.reserved = 123
	handle := &PartitionChangesHandle{
		mp:                  mp,
		visibleResources:    resources,
		currentChangeHandle: stub,
		bufferedBatches: []queuedChangeBatch{{
			data: data, tombstone: tombstone, reservedBytes: 123,
		}},
	}

	require.NoError(t, handle.Close())
	require.True(t, stub.closed)
	require.Nil(t, handle.currentChangeHandle)
	require.Nil(t, handle.bufferedBatches)
	require.Zero(t, resources.reserved)
}

func TestPartitionChangesHandleNextWithSnapshotRecovery_UsesBufferedBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(7), false, mp))
	data.SetRowCount(1)
	defer data.Clean(mp)

	resources := newTestVisibleStateResources()
	resources.reserved = 77
	handle := &PartitionChangesHandle{
		snapshotReadPolicy: engine.SnapshotReadPolicyVisibleState,
		visibleResources:   resources,
		bufferedBatches: []queuedChangeBatch{{
			data: data, hint: engine.ChangesHandle_Snapshot, reservedBytes: 77,
		}},
	}

	gotData, gotTombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Same(t, data, gotData)
	require.Nil(t, gotTombstone)
	require.Equal(t, engine.ChangesHandle_Snapshot, hint)
	require.Empty(t, handle.bufferedBatches)
	require.Zero(t, resources.reserved)
}

func TestPartitionChangesHandleBufferCurrentRangeCapacity(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	baseline := mp.CurrNB()

	first := makeBufferedTestBatch(t, mp, 7)
	second := makeBufferedTestBatch(t, mp, 8)
	resources := newTestVisibleStateResources()
	resources.reserveErr = moerr.NewMPoolCapacityNoCtxf("test visible-state buffer limit")
	resources.failAt = 2
	handle := &PartitionChangesHandle{
		mp: mp, visibleResources: resources,
		currentChangeHandle: &batchSequenceChangesHandle{data: []*batch.Batch{first, second}},
	}

	err := handle.bufferCurrentRange(context.Background(), mp)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity))
	require.Empty(t, handle.bufferedBatches)
	require.Zero(t, resources.reserved)
	require.Equal(t, 2, resources.reserveCnt)
	require.Equal(t, baseline, mp.CurrNB())
}

func TestPartitionChangesHandleCollectChangesContext(t *testing.T) {
	ctx := context.Background()
	require.False(t, engine.CollectChangesPreserveAllVersionsFromContext(
		(&PartitionChangesHandle{}).collectChangesContext(ctx),
	))
	require.True(t, engine.CollectChangesPreserveAllVersionsFromContext(
		(&PartitionChangesHandle{preserveAllVersions: true}).collectChangesContext(ctx),
	))
}

type stubChangesHandle struct {
	closed bool
}

type batchSequenceChangesHandle struct {
	data []*batch.Batch
	idx  int
}

func (s *batchSequenceChangesHandle) Next(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	if s.idx >= len(s.data) {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}
	data := s.data[s.idx]
	s.idx++
	return data, nil, engine.ChangesHandle_Tail_done, nil
}

func (s *batchSequenceChangesHandle) Close() error { return nil }

func makeBufferedTestBatch(t *testing.T, mp *mpool.MPool, value int64) *batch.Batch {
	t.Helper()
	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], value, false, mp))
	data.SetRowCount(1)
	return data
}

func (s *stubChangesHandle) Next(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	return nil, nil, engine.ChangesHandle_Tail_done, nil
}

func (s *stubChangesHandle) Close() error {
	s.closed = true
	return nil
}
