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

func TestGetTableCreationCommitTSFromCatalogCache_RejectsNonDisttaeEngine(t *testing.T) {
	_, err := GetTableCreationCommitTSFromCatalogCache(&engine.EntireEngine{}, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not *disttae.Engine")
}

type stubChangesHandle struct {
	closed bool
}

func (s *stubChangesHandle) Next(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	return nil, nil, engine.ChangesHandle_Tail_done, nil
}

func (s *stubChangesHandle) Close() error {
	s.closed = true
	return nil
}
