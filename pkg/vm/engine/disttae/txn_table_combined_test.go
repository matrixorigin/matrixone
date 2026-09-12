// Copyright 2021-2024 Matrix Origin
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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMockCombinedTxnTable() *combinedTxnTable {
	return &combinedTxnTable{
		primary:     nil, // Will be set in individual tests
		pruneFunc:   func(ctx context.Context, param engine.RangesParam) ([]engine.Relation, error) { return nil, nil },
		tablesFunc:  func() ([]engine.Relation, error) { return nil, nil },
		prunePKFunc: func(bat *batch.Batch, partitionIndex int32) ([]engine.Relation, error) { return nil, nil },
	}
}

func TestNewCombinedTxnTableFiltersNilRelations(t *testing.T) {
	valid := &mockRelation{}
	expected := []engine.Relation{valid}
	returned := []engine.Relation{valid, nil}
	table := newCombinedTxnTable(
		nil,
		func() ([]engine.Relation, error) {
			return returned, nil
		},
		func(context.Context, engine.RangesParam) ([]engine.Relation, error) {
			return returned, nil
		},
		func(*batch.Batch, int32) ([]engine.Relation, error) {
			return returned, nil
		},
	)

	relations, err := table.tablesFunc()
	assert.NoError(t, err)
	assert.Equal(t, expected, relations)

	relations, err = table.pruneFunc(context.Background(), engine.RangesParam{})
	assert.NoError(t, err)
	assert.Equal(t, expected, relations)

	relations, err = table.prunePKFunc(nil, 0)
	assert.NoError(t, err)
	assert.Equal(t, expected, relations)

	relations, err = table.tablesFunc()
	assert.NoError(t, err)
	assert.Equal(t, expected, relations)
	assert.Equal(t, []engine.Relation{valid, nil}, returned)

	assert.Empty(t, filterNilRelations([]engine.Relation{nil, nil}))
	withoutNil := []engine.Relation{valid}
	assert.Equal(t, withoutNil, filterNilRelations(withoutNil))
}

func TestNewCombinedTxnTablePreservesRelationErrors(t *testing.T) {
	table := newCombinedTxnTable(
		nil,
		func() ([]engine.Relation, error) {
			return []engine.Relation{nil}, assert.AnError
		},
		func(context.Context, engine.RangesParam) ([]engine.Relation, error) {
			return []engine.Relation{nil}, assert.AnError
		},
		func(*batch.Batch, int32) ([]engine.Relation, error) {
			return []engine.Relation{nil}, assert.AnError
		},
	)

	relations, err := table.tablesFunc()
	assert.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, relations)

	relations, err = table.pruneFunc(context.Background(), engine.RangesParam{})
	assert.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, relations)

	relations, err = table.prunePKFunc(nil, 0)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, relations)
}

func TestCombinedTxnTable_BuildShardingReaders(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "Not Support", func() {
		table.BuildShardingReaders(
			context.Background(),
			nil,
			nil,
			nil,
			0,
			0,
			false,
			engine.Policy_SkipCommittedS3,
		)
	})
}

func TestCombinedTxnTable_CollectChanges(t *testing.T) {
	table := newMockCombinedTxnTable()

	handle, err := table.CollectChanges(
		context.Background(),
		types.TS{},
		types.TS{},
		false,
		&mpool.MPool{},
	)
	assert.NoError(t, err)
	assert.NotNil(t, handle)

	data, tombstone, hint, err := handle.Next(context.Background(), &mpool.MPool{})
	assert.NoError(t, err)
	assert.Nil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Tail_done, hint)
	assert.NoError(t, handle.Close())
}

func TestCombinedTxnTable_CollectChangesOrdersPartitionsByCommitTS(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	later := newChangesTestBatch(t, mp, []int64{20}, []types.TS{types.BuildTS(20, 0)})
	earliest := newChangesTestBatch(t, mp, []int64{10}, []types.TS{types.BuildTS(10, 0)})
	table := newMockCombinedTxnTable()
	table.tablesFunc = func() ([]engine.Relation, error) {
		return []engine.Relation{
			&mockRelation{collectChangesFunc: func(context.Context, types.TS, types.TS, bool, *mpool.MPool) (engine.ChangesHandle, error) {
				return &mockChangesHandle{data: []*batch.Batch{later}}, nil
			}},
			&mockRelation{collectChangesFunc: func(context.Context, types.TS, types.TS, bool, *mpool.MPool) (engine.ChangesHandle, error) {
				return &mockChangesHandle{data: []*batch.Batch{earliest}}, nil
			}},
		}, nil
	}

	handle, err := table.CollectChanges(context.Background(), types.BuildTS(1, 0), types.TS{}, false, mp)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, handle.Close()) }()

	data, tombstone, hint, err := handle.Next(context.Background(), mp)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](data.Vecs[0], 0))
	assert.Equal(t, types.BuildTS(10, 0), vector.GetFixedAtNoTypeCheck[types.TS](data.Vecs[1], 0))
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Tail_wip, hint)
	data.Clean(mp)

	data, tombstone, hint, err = handle.Next(context.Background(), mp)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](data.Vecs[0], 0))
	assert.Equal(t, types.BuildTS(20, 0), vector.GetFixedAtNoTypeCheck[types.TS](data.Vecs[1], 0))
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Tail_done, hint)
	data.Clean(mp)

	data, tombstone, hint, err = handle.Next(context.Background(), mp)
	assert.NoError(t, err)
	assert.Nil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Tail_done, hint)
}

func TestCombinedTxnTable_CollectChangesKeepsPartitionMoveAtomic(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	insert := newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)})
	insertSameCommit := newChangesTestBatch(t, mp, []int64{2}, []types.TS{types.BuildTS(10, 0)})
	delete := newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)})
	later := newChangesTestBatch(t, mp, []int64{3}, []types.TS{types.BuildTS(20, 0)})
	table := newMockCombinedTxnTable()
	table.tablesFunc = func() ([]engine.Relation, error) {
		return []engine.Relation{
			&mockRelation{collectChangesFunc: func(context.Context, types.TS, types.TS, bool, *mpool.MPool) (engine.ChangesHandle, error) {
				return &mockChangesHandle{changes: []mockChange{
					{data: insert, hint: engine.ChangesHandle_Tail_done},
					{data: insertSameCommit, hint: engine.ChangesHandle_Tail_done},
					{data: later, hint: engine.ChangesHandle_Tail_done},
				}}, nil
			}},
			&mockRelation{collectChangesFunc: func(context.Context, types.TS, types.TS, bool, *mpool.MPool) (engine.ChangesHandle, error) {
				return &mockChangesHandle{changes: []mockChange{{tombstone: delete, hint: engine.ChangesHandle_Tail_done}}}, nil
			}},
		}, nil
	}

	handle, err := table.CollectChanges(context.Background(), types.BuildTS(1, 0), types.TS{}, false, mp)
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()

	data, tombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.NotNil(t, tombstone)
	assert.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](data.Vecs[0]))
	assert.Equal(t, types.BuildTS(10, 0), vector.GetFixedAtNoTypeCheck[types.TS](data.Vecs[1], 0))
	assert.Equal(t, types.BuildTS(10, 0), vector.GetFixedAtNoTypeCheck[types.TS](tombstone.Vecs[1], 0))
	assert.Equal(t, engine.ChangesHandle_Tail_done, hint)
	data.Clean(mp)
	tombstone.Clean(mp)

	data, tombstone, hint, err = handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, types.BuildTS(20, 0), vector.GetFixedAtNoTypeCheck[types.TS](data.Vecs[1], 0))
	assert.Equal(t, engine.ChangesHandle_Tail_done, hint)
	data.Clean(mp)
}

func TestCombinedTxnTable_CollectChangesKeepsSnapshotBatchesBounded(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	const snapshotTS = 100
	first := &mockChangesHandle{changes: []mockChange{
		{data: newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(snapshotTS, 0)}), hint: engine.ChangesHandle_Snapshot},
		{data: newChangesTestBatch(t, mp, []int64{3}, []types.TS{types.BuildTS(snapshotTS, 0)}), hint: engine.ChangesHandle_Snapshot},
	}}
	second := &mockChangesHandle{changes: []mockChange{
		{data: newChangesTestBatch(t, mp, []int64{2}, []types.TS{types.BuildTS(snapshotTS, 0)}), hint: engine.ChangesHandle_Snapshot},
		{data: newChangesTestBatch(t, mp, []int64{4}, []types.TS{types.BuildTS(snapshotTS, 0)}), hint: engine.ChangesHandle_Snapshot},
	}}
	table := newMockCombinedTxnTable()
	table.tablesFunc = func() ([]engine.Relation, error) {
		return []engine.Relation{
			&mockRelation{collectChangesFunc: func(context.Context, types.TS, types.TS, bool, *mpool.MPool) (engine.ChangesHandle, error) {
				return first, nil
			}},
			&mockRelation{collectChangesFunc: func(context.Context, types.TS, types.TS, bool, *mpool.MPool) (engine.ChangesHandle, error) {
				return second, nil
			}},
		}, nil
	}

	handle, err := table.CollectChanges(context.Background(), types.TS{}, types.TS{}, false, mp)
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()

	data, tombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Snapshot, hint)
	assert.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](data.Vecs[0]))
	assert.Equal(t, 1, first.idx)
	assert.Zero(t, second.idx)
	data.Clean(mp)

	data, tombstone, hint, err = handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Snapshot, hint)
	assert.Equal(t, []int64{3}, vector.MustFixedColWithTypeCheck[int64](data.Vecs[0]))
	assert.Equal(t, 2, first.idx)
	assert.Zero(t, second.idx)
	data.Clean(mp)

	data, tombstone, hint, err = handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Snapshot, hint)
	assert.Equal(t, []int64{2}, vector.MustFixedColWithTypeCheck[int64](data.Vecs[0]))
	assert.Equal(t, 2, first.idx)
	assert.Equal(t, 1, second.idx)
	data.Clean(mp)

	data, tombstone, hint, err = handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Snapshot, hint)
	assert.Equal(t, []int64{4}, vector.MustFixedColWithTypeCheck[int64](data.Vecs[0]))
	assert.Equal(t, 2, first.idx)
	assert.Equal(t, 2, second.idx)
	data.Clean(mp)
}

func TestCombinedChangesHandle_CloseClosesAllHandlesOnError(t *testing.T) {
	errFirst := errors.New("first close failed")
	errSecond := errors.New("second close failed")
	first := &mockChangesHandle{closeErr: errFirst}
	second := &mockChangesHandle{closeErr: errSecond}
	third := &mockChangesHandle{}
	handle := &combinedChangesHandle{handles: []engine.ChangesHandle{first, second, third}}

	err := handle.Close()
	assert.ErrorIs(t, err, errFirst)
	assert.Equal(t, 1, first.closeCount)
	assert.Equal(t, 1, second.closeCount)
	assert.Equal(t, 1, third.closeCount)
	assert.True(t, first.closed)
	assert.True(t, second.closed)
	assert.True(t, third.closed)
	assert.NoError(t, handle.Close())
	assert.NoError(t, handle.closeRemaining())
}

func TestCombinedChangesHandleNextAfterCloseReturnsDone(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	handle := &combinedChangesHandle{closed: true}

	data, tombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	assert.Nil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Tail_done, hint)
}

func TestCombinedChangesHandle_NextCloseErrorClosesRemainingHandles(t *testing.T) {
	errFirst := errors.New("first close failed")
	first := &mockChangesHandle{closeErr: errFirst}
	second := &mockChangesHandle{}
	handle := &combinedChangesHandle{handles: []engine.ChangesHandle{first, second}}

	data, tombstone, _, err := handle.Next(context.Background(), &mpool.MPool{})
	assert.ErrorIs(t, err, errFirst)
	assert.Nil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, 1, first.closeCount)
	assert.Equal(t, 1, second.closeCount)
	assert.True(t, first.closed)
	assert.True(t, second.closed)
}

func TestCombinedTxnTable_CollectChangesClosesAcquiredHandlesOnError(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	acquired := &mockChangesHandle{}
	expectedErr := errors.New("collect changes failed")
	table := newMockCombinedTxnTable()
	table.tablesFunc = func() ([]engine.Relation, error) {
		return []engine.Relation{
			&mockRelation{collectChangesFunc: func(context.Context, types.TS, types.TS, bool, *mpool.MPool) (engine.ChangesHandle, error) {
				return acquired, nil
			}},
			&mockRelation{collectChangesFunc: func(context.Context, types.TS, types.TS, bool, *mpool.MPool) (engine.ChangesHandle, error) {
				return nil, expectedErr
			}},
		}, nil
	}

	handle, err := table.CollectChanges(context.Background(), types.BuildTS(1, 0), types.TS{}, false, mp)
	require.ErrorIs(t, err, expectedErr)
	assert.Nil(t, handle)
	assert.True(t, acquired.closed)
	assert.Equal(t, 1, acquired.closeCount)
}

func TestCombinedTxnTable_CollectChangesReturnsTablesError(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	expectedErr := errors.New("list partitions failed")
	table := newMockCombinedTxnTable()
	table.tablesFunc = func() ([]engine.Relation, error) {
		return nil, expectedErr
	}

	handle, err := table.CollectChanges(context.Background(), types.BuildTS(1, 0), types.TS{}, false, mp)
	require.ErrorIs(t, err, expectedErr)
	assert.Nil(t, handle)
}

func TestCombinedChangesHandleRejectsInvalidStreams(t *testing.T) {
	newHandle := func(mp *mpool.MPool, changes []mockChange, snapshot bool) *combinedChangesHandle {
		return &combinedChangesHandle{
			handles:  []engine.ChangesHandle{&mockChangesHandle{changes: changes}},
			mp:       mp,
			snapshot: snapshot,
		}
	}

	t.Run("snapshot data must use snapshot hint", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		handle := newHandle(mp, []mockChange{{
			data: newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)}),
			hint: engine.ChangesHandle_Tail_done,
		}}, true)

		data, tombstone, _, err := handle.Next(context.Background(), mp)
		require.ErrorContains(t, err, "checkpoint changes handle returned tail data")
		assert.Nil(t, data)
		assert.Nil(t, tombstone)
	})

	t.Run("child next error closes every partition", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		expectedErr := errors.New("read checkpoint failed")
		child := &mockChangesHandle{nextErr: expectedErr}
		handle := &combinedChangesHandle{
			handles:  []engine.ChangesHandle{child},
			mp:       mp,
			snapshot: true,
		}

		data, tombstone, _, err := handle.Next(context.Background(), mp)
		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, data)
		assert.Nil(t, tombstone)
		assert.True(t, child.closed)
	})

	t.Run("child close error is returned", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		expectedErr := errors.New("close checkpoint failed")
		child := &mockChangesHandle{closeErr: expectedErr}
		handle := &combinedChangesHandle{
			handles:  []engine.ChangesHandle{child},
			mp:       mp,
			snapshot: true,
		}

		data, tombstone, _, err := handle.Next(context.Background(), mp)
		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, data)
		assert.Nil(t, tombstone)
		assert.True(t, child.closed)
	})

	t.Run("tail data must not use snapshot hint", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		handle := newHandle(mp, []mockChange{{
			data: newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)}),
			hint: engine.ChangesHandle_Snapshot,
		}}, false)

		data, tombstone, _, err := handle.Next(context.Background(), mp)
		require.ErrorContains(t, err, "tail changes handle returned snapshot data")
		assert.Nil(t, data)
		assert.Nil(t, tombstone)
	})

	t.Run("tail data must be ordered by commit timestamp", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		handle := newHandle(mp, []mockChange{
			{data: newChangesTestBatch(t, mp, []int64{2}, []types.TS{types.BuildTS(20, 0)}), hint: engine.ChangesHandle_Tail_done},
			{data: newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)}), hint: engine.ChangesHandle_Tail_done},
		}, false)

		data, tombstone, _, err := handle.Next(context.Background(), mp)
		require.ErrorContains(t, err, "partition change stream is not ordered by commit timestamp")
		assert.Nil(t, data)
		assert.Nil(t, tombstone)
	})
}

func TestCombinedChangesHandleSnapshotSkipsEmptyBatches(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	child := &mockChangesHandle{changes: []mockChange{
		{
			data:      newChangesTestBatch(t, mp, nil, nil),
			tombstone: newChangesTestBatch(t, mp, nil, nil),
			hint:      engine.ChangesHandle_Snapshot,
		},
		{data: newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)}), hint: engine.ChangesHandle_Snapshot},
	}}
	handle := &combinedChangesHandle{
		handles:  []engine.ChangesHandle{child},
		mp:       mp,
		snapshot: true,
	}

	data, tombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Snapshot, hint)
	assert.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](data.Vecs[0]))
	data.Clean(mp)

	data, tombstone, hint, err = handle.Next(context.Background(), mp)
	require.NoError(t, err)
	assert.Nil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Tail_done, hint)
	assert.True(t, child.closed)
}

func TestCombinedChangesHandleTailSkipsEmptyBatches(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	child := &mockChangesHandle{changes: []mockChange{{
		data:      newChangesTestBatch(t, mp, nil, nil),
		tombstone: newChangesTestBatch(t, mp, nil, nil),
		hint:      engine.ChangesHandle_Tail_done,
	}}}
	handle := &combinedChangesHandle{
		handles: []engine.ChangesHandle{child},
		mp:      mp,
	}

	data, tombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	assert.Nil(t, data)
	assert.Nil(t, tombstone)
	assert.Equal(t, engine.ChangesHandle_Tail_done, hint)
	assert.True(t, child.closed)
}

func TestCombinedChangesHandleTailNextErrorClosesEveryPartition(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	expectedErr := errors.New("read tail failed")
	child := &mockChangesHandle{nextErr: expectedErr}
	handle := &combinedChangesHandle{
		handles: []engine.ChangesHandle{child},
		mp:      mp,
	}

	data, tombstone, _, err := handle.Next(context.Background(), mp)
	require.ErrorIs(t, err, expectedErr)
	assert.Nil(t, data)
	assert.Nil(t, tombstone)
	assert.True(t, child.closed)
}

func TestCombinedChangesHelpers(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	t.Run("append one commit timestamp and retain the following rows", func(t *testing.T) {
		src := newChangesTestBatch(t, mp, []int64{1, 2}, []types.TS{types.BuildTS(10, 0), types.BuildTS(20, 0)})
		var dst *batch.Batch
		offset := 0

		appended, err := appendChangesAtCommitTS(&dst, &src, &offset, types.BuildTS(10, 0), mp)
		require.NoError(t, err)
		require.True(t, appended)
		require.NotNil(t, dst)
		assert.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](dst.Vecs[0]))
		assert.Equal(t, 1, offset)
		assert.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](src.Vecs[0]))

		dst.Clean(mp)
		src.Clean(mp)
	})

	t.Run("merge hints rejects mixed streams", func(t *testing.T) {
		hint, hasHint, err := mergeChangesHint(engine.ChangesHandle_Tail_done, false, engine.ChangesHandle_Snapshot)
		require.NoError(t, err)
		assert.True(t, hasHint)
		assert.Equal(t, engine.ChangesHandle_Snapshot, hint)

		_, hasHint, err = mergeChangesHint(engine.ChangesHandle_Snapshot, true, engine.ChangesHandle_Tail_done)
		require.ErrorContains(t, err, "mixed snapshot and tail data")
		assert.False(t, hasHint)

		hint, hasHint, err = mergeChangesHint(engine.ChangesHandle_Snapshot, true, engine.ChangesHandle_Snapshot)
		require.NoError(t, err)
		assert.True(t, hasHint)
		assert.Equal(t, engine.ChangesHandle_Snapshot, hint)

		hint, hasHint, err = mergeChangesHint(engine.ChangesHandle_Tail_done, true, engine.ChangesHandle_Tail_wip)
		require.NoError(t, err)
		assert.True(t, hasHint)
		assert.Equal(t, engine.ChangesHandle_Tail_wip, hint)
	})

	t.Run("clean pending batches", func(t *testing.T) {
		pending := &pendingPartitionChanges{
			data:      newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)}),
			tombstone: newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)}),
		}
		cleanPendingChanges(pending, mp)
		assert.Nil(t, pending.data)
		assert.Nil(t, pending.tombstone)
	})

	t.Run("tombstones participate in commit ordering", func(t *testing.T) {
		handle := &combinedChangesHandle{pending: []pendingPartitionChanges{
			{data: newChangesTestBatch(t, mp, []int64{2}, []types.TS{types.BuildTS(20, 0)})},
			{tombstone: newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)})},
		}}
		require.NoError(t, handle.pushPartitionFrontier(0))
		require.NoError(t, handle.pushPartitionFrontier(1))
		commitTS, ok := handle.nextCommitTS()
		require.True(t, ok)
		assert.Equal(t, types.BuildTS(10, 0), commitTS)
		cleanPendingChanges(&handle.pending[0], mp)
		cleanPendingChanges(&handle.pending[1], mp)
	})

	t.Run("fail cleans a returned tombstone", func(t *testing.T) {
		tombstone := newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)})
		expectedErr := errors.New("stream failed")
		handle := &combinedChangesHandle{}

		data, returnedTombstone, _, err := handle.fail(mp, nil, tombstone, expectedErr)
		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, data)
		assert.Nil(t, returnedTombstone)
	})

	t.Run("closing a partition is idempotent", func(t *testing.T) {
		child := &mockChangesHandle{}
		handle := &combinedChangesHandle{handles: []engine.ChangesHandle{child}}
		handle.ensurePending()
		require.NoError(t, handle.closePartition(0, mp))
		require.NoError(t, handle.closePartition(0, mp))
		assert.Equal(t, 1, child.closeCount)
	})
}

func TestCombinedChangesHandleTailManyCommitTimestamps(t *testing.T) {
	const rowCount = int(objectio.BlockMaxRows)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	handle, children := newMixedTimestampChangesHandle(t, mp, rowCount)

	tailDoneCount := 0
	for i := 0; i < rowCount; i++ {
		data, tombstone, hint, err := handle.Next(context.Background(), mp)
		require.NoError(t, err)
		require.NotNil(t, data)
		require.Nil(t, tombstone)
		if i == rowCount-1 {
			require.Equal(t, engine.ChangesHandle_Tail_done, hint)
			tailDoneCount++
		} else {
			require.Equal(t, engine.ChangesHandle_Tail_wip, hint)
		}
		require.Equal(t, 1, data.RowCount())
		require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](data.Vecs[0], 0))
		require.Equal(t, types.BuildTS(int64(i+1), 0), vector.GetFixedAtNoTypeCheck[types.TS](data.Vecs[1], 0))
		data.Clean(mp)
	}
	require.Equal(t, 1, tailDoneCount, "an 8K pure insert range must produce one sink boundary")

	data, tombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Nil(t, data)
	require.Nil(t, tombstone)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
	for _, child := range children {
		require.True(t, child.closed)
	}
}

func TestCombinedChangesHandleTailBoundsPureRanges(t *testing.T) {
	const rowCount = 2 * int(objectio.BlockMaxRows)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	handle, children := newMixedTimestampChangesHandle(t, mp, rowCount)
	tailDoneCount := 0
	for i := 0; i < rowCount; i++ {
		data, tombstone, hint, err := handle.Next(context.Background(), mp)
		require.NoError(t, err)
		require.NotNil(t, data)
		require.Nil(t, tombstone)
		if (i+1)%int(objectio.BlockMaxRows) == 0 {
			require.Equal(t, engine.ChangesHandle_Tail_done, hint)
			tailDoneCount++
		} else {
			require.Equal(t, engine.ChangesHandle_Tail_wip, hint)
		}
		data.Clean(mp)
	}
	require.Equal(t, 2, tailDoneCount)
	for _, child := range children {
		require.True(t, child.closed)
	}
}

func TestCombinedChangesHandleTailCoalescesOnlyPureOperations(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	t.Run("pure deletes coalesce", func(t *testing.T) {
		child := &mockChangesHandle{changes: []mockChange{{
			tombstone: newChangesTestBatch(t, mp, []int64{1, 2}, []types.TS{types.BuildTS(10, 0), types.BuildTS(20, 0)}),
			hint:      engine.ChangesHandle_Tail_done,
		}}}
		handle := &combinedChangesHandle{handles: []engine.ChangesHandle{child}, mp: mp}

		data, tombstone, hint, err := handle.Next(context.Background(), mp)
		require.NoError(t, err)
		require.Nil(t, data)
		require.NotNil(t, tombstone)
		require.Equal(t, engine.ChangesHandle_Tail_wip, hint)
		tombstone.Clean(mp)

		data, tombstone, hint, err = handle.Next(context.Background(), mp)
		require.NoError(t, err)
		require.Nil(t, data)
		require.NotNil(t, tombstone)
		require.Equal(t, engine.ChangesHandle_Tail_done, hint)
		tombstone.Clean(mp)
	})

	t.Run("insert followed by delete stays separate", func(t *testing.T) {
		child := &mockChangesHandle{changes: []mockChange{{
			data:      newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(10, 0)}),
			tombstone: newChangesTestBatch(t, mp, []int64{1}, []types.TS{types.BuildTS(20, 0)}),
			hint:      engine.ChangesHandle_Tail_done,
		}}}
		handle := &combinedChangesHandle{handles: []engine.ChangesHandle{child}, mp: mp}

		data, tombstone, hint, err := handle.Next(context.Background(), mp)
		require.NoError(t, err)
		require.NotNil(t, data)
		require.Nil(t, tombstone)
		require.Equal(t, engine.ChangesHandle_Tail_done, hint)
		data.Clean(mp)

		data, tombstone, hint, err = handle.Next(context.Background(), mp)
		require.NoError(t, err)
		require.Nil(t, data)
		require.NotNil(t, tombstone)
		require.Equal(t, engine.ChangesHandle_Tail_done, hint)
		tombstone.Clean(mp)
	})
}

func BenchmarkCombinedChangesHandleTail8KDistinctCommitTimestamps(b *testing.B) {
	const rowCount = 8192

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mp := mpool.MustNewZero()
		handle, _ := newMixedTimestampChangesHandle(b, mp, rowCount)
		b.StartTimer()
		for {
			data, tombstone, _, err := handle.Next(context.Background(), mp)
			if err != nil {
				b.Fatal(err)
			}
			if data == nil && tombstone == nil {
				break
			}
			if data != nil {
				data.Clean(mp)
			}
			if tombstone != nil {
				tombstone.Clean(mp)
			}
		}
		b.StopTimer()
		mpool.DeleteMPool(mp)
	}
}

func newMixedTimestampChangesHandle(
	t testing.TB,
	mp *mpool.MPool,
	rowCount int,
) (*combinedChangesHandle, []*mockChangesHandle) {
	t.Helper()
	const partitionCount = 8
	require.Zero(t, rowCount%partitionCount)

	rowsPerPartition := rowCount / partitionCount
	handles := make([]engine.ChangesHandle, partitionCount)
	children := make([]*mockChangesHandle, partitionCount)
	for partition := range handles {
		values := make([]int64, rowsPerPartition)
		timestamps := make([]types.TS, rowsPerPartition)
		for row := range values {
			sequence := row*partitionCount + partition
			values[row] = int64(sequence)
			timestamps[row] = types.BuildTS(int64(sequence+1), 0)
		}
		children[partition] = &mockChangesHandle{changes: []mockChange{{
			data: newChangesTestBatch(t, mp, values, timestamps),
			hint: engine.ChangesHandle_Tail_done,
		}}}
		handles[partition] = children[partition]
	}
	return &combinedChangesHandle{handles: handles, mp: mp}, children
}

func TestCombinedTxnTable_MergeObjects(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MergeObjects(
			context.Background(),
			[]objectio.ObjectStats{},
			1024,
		)
	})
}

func TestCombinedTxnTable_UpdateConstraint(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.UpdateConstraint(
			context.Background(),
			&engine.ConstraintDef{},
		)
	})
}

func TestCombinedTxnTable_TableRenameInTxn(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.TableRenameInTxn(
			context.Background(),
			[][]byte{},
		)
	})
}

func TestCombinedTxnTable_MaxAndMinValues(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MaxAndMinValues(context.Background())
	})
}

func TestCombinedTxnTable_Write(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot write data to partition primary table", func() {
		table.Write(context.Background(), &batch.Batch{})
	})
}

func TestCombinedTxnTable_Delete(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot delete data to partition primary table", func() {
		table.Delete(context.Background(), &batch.Batch{}, "")
	})
}

func TestCombinedTxnTable_BuildReaders(t *testing.T) {
	ctx := context.Background()

	t.Run("RelDataNilSuccess", func(t *testing.T) {
		reader1 := &mockReader{}
		reader2 := &mockReader{}
		rel1Called := false
		rel2Called := false

		mockRel1 := &mockRelation{
			buildReadersFunc: func(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error) {
				rel1Called = true
				assert.Nil(t, relData)
				assert.Equal(t, 1, num)
				return []engine.Reader{reader1}, nil
			},
		}
		mockRel2 := &mockRelation{
			buildReadersFunc: func(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error) {
				rel2Called = true
				assert.Nil(t, relData)
				return []engine.Reader{reader2}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.BuildReaders(ctx, nil, nil, nil, 1, 0, false, engine.Policy_CheckAll, engine.FilterHint{})
		assert.NoError(t, err)
		assert.Equal(t, []engine.Reader{reader1, reader2}, result)
		assert.True(t, rel1Called)
		assert.True(t, rel2Called)
	})

	t.Run("TablesFuncError", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.BuildReaders(ctx, nil, nil, nil, 1, 0, false, engine.Policy_CheckAll, engine.FilterHint{})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("RelationBuildReadersError", func(t *testing.T) {
		mockRel := &mockRelation{
			buildReadersFunc: func(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error) {
				return nil, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.BuildReaders(ctx, nil, nil, nil, 1, 0, false, engine.Policy_CheckAll, engine.FilterHint{})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("AllNilRelationsUseEmptyReaders", func(t *testing.T) {
		table := newCombinedTxnTable(
			nil,
			func() ([]engine.Relation, error) {
				return []engine.Relation{nil, nil}, nil
			},
			func(context.Context, engine.RangesParam) ([]engine.Relation, error) {
				return []engine.Relation{nil, nil}, nil
			},
			func(*batch.Batch, int32) ([]engine.Relation, error) {
				return nil, nil
			},
		)

		relData, err := table.Ranges(ctx, engine.RangesParam{})
		assert.NoError(t, err)

		result, err := table.BuildReaders(
			ctx,
			nil,
			nil,
			relData,
			3,
			0,
			false,
			engine.Policy_CheckAll,
			engine.FilterHint{},
		)
		assert.NoError(t, err)
		assert.Len(t, result, 3)
		for _, reader := range result {
			assert.IsType(t, new(readutil.EmptyReader), reader)
			end, err := reader.Read(ctx, nil, nil, nil, nil)
			assert.NoError(t, err)
			assert.True(t, end)
		}

		result, err = table.BuildReaders(
			ctx,
			nil,
			nil,
			nil,
			2,
			0,
			false,
			engine.Policy_CheckAll,
			engine.FilterHint{},
		)
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		for _, reader := range result {
			assert.IsType(t, new(readutil.EmptyReader), reader)
		}
	})
}

func TestCombinedTxnTableBuildReadersPreparesMembershipFilterOnce(t *testing.T) {
	payload := append([]byte{docfilter.TagSorted64}, make([]byte, 8)...)
	var (
		builder docfilter.MembershipFilter
		shares  []docfilter.MembershipFilter
	)

	build := func(
		_ context.Context,
		_ any,
		_ *plan.Expr,
		_ engine.RelData,
		_ int,
		_ int,
		_ bool,
		_ engine.TombstoneApplyPolicy,
		hint engine.FilterHint,
	) ([]engine.Reader, error) {
		require.Empty(t, hint.MembershipFilterBytes)
		filter, ok := hint.BF.(docfilter.MembershipFilter)
		require.True(t, ok)
		if builder == nil {
			builder = filter
		} else {
			require.Same(t, builder, filter)
		}
		share := filter.Share()
		shares = append(shares, share)
		return []engine.Reader{&mockReader{filter: share}}, nil
	}

	table := &combinedTxnTable{tablesFunc: func() ([]engine.Relation, error) {
		return []engine.Relation{
			&mockRelation{buildReadersFunc: build},
			&mockRelation{buildReadersFunc: build},
		}, nil
	}}

	readers, err := table.BuildReaders(
		context.Background(),
		nil,
		nil,
		nil,
		1,
		0,
		false,
		engine.Policy_CheckAll,
		engine.FilterHint{MembershipFilterBytes: payload},
	)
	require.NoError(t, err)
	require.Len(t, readers, 2)
	require.Len(t, shares, 2)
	require.True(t, shares[0].Valid())
	require.True(t, shares[1].Valid())

	require.NoError(t, readers[0].Close())
	require.True(t, shares[1].Valid())
	require.NoError(t, readers[1].Close())
	require.False(t, builder.Valid())
}

func TestCombinedTxnTable_GetColumMetadataScanInfo(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		// Create mock metadata scan info
		mockInfo1 := &plan.MetadataScanInfo{
			ColName:      "col1",
			ObjectName:   "obj1",
			IsHidden:     false,
			RowCnt:       100,
			NullCnt:      5,
			CompressSize: 1024,
			OriginSize:   2048,
		}
		mockInfo2 := &plan.MetadataScanInfo{
			ColName:      "col1",
			ObjectName:   "obj2",
			IsHidden:     false,
			RowCnt:       200,
			NullCnt:      10,
			CompressSize: 2048,
			OriginSize:   4096,
		}

		// Create mock relations that return metadata scan info
		mockRel1 := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return []*plan.MetadataScanInfo{mockInfo1}, nil
			},
		}
		mockRel2 := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return []*plan.MetadataScanInfo{mockInfo2}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", false)
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, mockInfo1, result[0])
		assert.Equal(t, mockInfo2, result[1])
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", false)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's GetColumMetadataScanInfo
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return nil, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", false)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", false)
		assert.NoError(t, err)
		assert.Len(t, result, 0)
	})

	// Test case 5: Multiple tables with mixed results
	t.Run("Multiple tables with mixed results", func(t *testing.T) {
		mockInfo1 := &plan.MetadataScanInfo{
			ColName:    "col1",
			ObjectName: "obj1",
			IsHidden:   false,
			RowCnt:     100,
		}
		mockInfo2 := &plan.MetadataScanInfo{
			ColName:    "col1",
			ObjectName: "obj2",
			IsHidden:   true,
			RowCnt:     200,
		}
		mockInfo3 := &plan.MetadataScanInfo{
			ColName:    "col1",
			ObjectName: "obj3",
			IsHidden:   false,
			RowCnt:     300,
		}

		mockRel1 := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return []*plan.MetadataScanInfo{mockInfo1}, nil
			},
		}
		mockRel2 := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return []*plan.MetadataScanInfo{mockInfo2, mockInfo3}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", true)
		assert.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, mockInfo1, result[0])
		assert.Equal(t, mockInfo2, result[1])
		assert.Equal(t, mockInfo3, result[2])
	})
}

func TestCombinedTxnTable_GetNonAppendableObjectStats(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		// Create mock object stats
		mockStats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats1, 1024)
		objectio.SetObjectStatsOriginSize(mockStats1, 2048)
		objectio.SetObjectStatsRowCnt(mockStats1, 100)

		mockStats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats2, 2048)
		objectio.SetObjectStatsOriginSize(mockStats2, 4096)
		objectio.SetObjectStatsRowCnt(mockStats2, 200)

		// Create mock relations that return object stats
		mockRel1 := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats1}, nil
			},
		}
		mockRel2 := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats2}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, *mockStats1, result[0])
		assert.Equal(t, *mockStats2, result[1])
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's GetNonAppendableObjectStats
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return nil, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.NoError(t, err)
		assert.Len(t, result, 0)
	})

	// Test case 5: Multiple tables with mixed results
	t.Run("Multiple tables with mixed results", func(t *testing.T) {
		mockStats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats1, 1024)
		objectio.SetObjectStatsRowCnt(mockStats1, 100)

		mockStats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats2, 2048)
		objectio.SetObjectStatsRowCnt(mockStats2, 200)

		mockStats3 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats3, 3072)
		objectio.SetObjectStatsRowCnt(mockStats3, 300)

		mockRel1 := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats1}, nil
			},
		}
		mockRel2 := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats2, *mockStats3}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, *mockStats1, result[0])
		assert.Equal(t, *mockStats2, result[1])
		assert.Equal(t, *mockStats3, result[2])
	})

	// Test case 6: Single table with multiple object stats
	t.Run("Single table with multiple object stats", func(t *testing.T) {
		mockStats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats1, 1024)
		objectio.SetObjectStatsRowCnt(mockStats1, 100)

		mockStats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats2, 2048)
		objectio.SetObjectStatsRowCnt(mockStats2, 200)

		mockRel := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats1, *mockStats2}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, *mockStats1, result[0])
		assert.Equal(t, *mockStats2, result[1])
	})
}

func TestCombinedTxnTable_ApproxObjectsNum(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			approxObjectsNumFunc: func(ctx context.Context) int {
				return 5
			},
		}
		mockRel2 := &mockRelation{
			approxObjectsNumFunc: func(ctx context.Context) int {
				return 10
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result := table.ApproxObjectsNum(context.Background())
		assert.Equal(t, 15, result)
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result := table.ApproxObjectsNum(context.Background())
		assert.Equal(t, 0, result)
	})

	// Test case 3: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result := table.ApproxObjectsNum(context.Background())
		assert.Equal(t, 0, result)
	})

	// Test case 4: Single table
	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			approxObjectsNumFunc: func(ctx context.Context) int {
				return 25
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result := table.ApproxObjectsNum(context.Background())
		assert.Equal(t, 25, result)
	})
}

func TestCombinedTxnTable_CollectTombstones(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		// Create a tombstone that will be modified after merge
		hasInMemory := true
		hasFile := false

		mockTombstone1 := &mockTombstoner{
			hasAnyInMemoryTombstoneFunc: func() bool { return hasInMemory },
			hasAnyTombstoneFileFunc:     func() bool { return hasFile },
			mergeFunc: func(other engine.Tombstoner) error {
				// After merge, the first tombstone should have both properties
				hasInMemory = true
				hasFile = true
				return nil
			},
		}
		mockTombstone2 := &mockTombstoner{
			hasAnyInMemoryTombstoneFunc: func() bool { return false },
			hasAnyTombstoneFileFunc:     func() bool { return true },
		}

		mockRel1 := &mockRelation{
			collectTombstonesFunc: func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
				return mockTombstone1, nil
			},
		}
		mockRel2 := &mockRelation{
			collectTombstonesFunc: func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
				return mockTombstone2, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// After merging, the first tombstone should have both properties
		assert.True(t, result.HasAnyInMemoryTombstone())
		assert.True(t, result.HasAnyTombstoneFile())
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's CollectTombstones
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			collectTombstonesFunc: func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
				return nil, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	// Test case 5: Single table
	t.Run("Single table", func(t *testing.T) {
		mockTombstone := &mockTombstoner{
			hasAnyInMemoryTombstoneFunc: func() bool { return true },
			hasAnyTombstoneFileFunc:     func() bool { return false },
		}

		mockRel := &mockRelation{
			collectTombstonesFunc: func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
				return mockTombstone, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.HasAnyInMemoryTombstone())
		assert.False(t, result.HasAnyTombstoneFile())
	})
}

func TestCombinedTxnTable_Size(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			sizeFunc: func(ctx context.Context, columnName string) (uint64, error) {
				return 1024, nil
			},
		}
		mockRel2 := &mockRelation{
			sizeFunc: func(ctx context.Context, columnName string) (uint64, error) {
				return 2048, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.NoError(t, err)
		assert.Equal(t, uint64(3072), result)
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.Error(t, err)
		assert.Equal(t, uint64(0), result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's Size
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			sizeFunc: func(ctx context.Context, columnName string) (uint64, error) {
				return 0, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.Error(t, err)
		assert.Equal(t, uint64(0), result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), result)
	})

	// Test case 5: Single table
	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			sizeFunc: func(ctx context.Context, columnName string) (uint64, error) {
				return 4096, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.NoError(t, err)
		assert.Equal(t, uint64(4096), result)
	})
}

func TestCombinedTxnTable_StarCount(t *testing.T) {
	ctx := context.Background()

	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			starCountFunc: func(ctx context.Context) (uint64, error) {
				return 100, nil
			},
		}
		mockRel2 := &mockRelation{
			starCountFunc: func(ctx context.Context) (uint64, error) {
				return 200, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		count, err := table.StarCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(300), count)
	})

	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		count, err := table.StarCount(ctx)
		assert.Error(t, err)
		assert.Equal(t, uint64(0), count)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			starCountFunc: func(ctx context.Context) (uint64, error) {
				return 0, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		count, err := table.StarCount(ctx)
		assert.Error(t, err)
		assert.Equal(t, uint64(0), count)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		count, err := table.StarCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	})

	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			starCountFunc: func(ctx context.Context) (uint64, error) {
				return 42, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		count, err := table.StarCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(42), count)
	})
}

func TestCombinedTxnTable_EstimateCommittedTombstoneCount(t *testing.T) {
	ctx := context.Background()

	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			estimateCommittedTombstoneCountFunc: func(ctx context.Context) (int, error) {
				return 10, nil
			},
		}
		mockRel2 := &mockRelation{
			estimateCommittedTombstoneCountFunc: func(ctx context.Context) (int, error) {
				return 20, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 30, count)
	})

	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			estimateCommittedTombstoneCountFunc: func(ctx context.Context) (int, error) {
				return 0, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			estimateCommittedTombstoneCountFunc: func(ctx context.Context) (int, error) {
				return 5, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 5, count)
	})
}

func TestCombinedTxnTable_Rows(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			rowsFunc: func(ctx context.Context) (uint64, error) {
				return 100, nil
			},
		}
		mockRel2 := &mockRelation{
			rowsFunc: func(ctx context.Context) (uint64, error) {
				return 200, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.Rows(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, uint64(300), result)
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.Rows(context.Background())
		assert.Error(t, err)
		assert.Equal(t, uint64(0), result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's Rows
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			rowsFunc: func(ctx context.Context) (uint64, error) {
				return 0, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.Rows(context.Background())
		assert.Error(t, err)
		assert.Equal(t, uint64(0), result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.Rows(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), result)
	})

	// Test case 5: Single table
	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			rowsFunc: func(ctx context.Context) (uint64, error) {
				return 500, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.Rows(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, uint64(500), result)
	})
}

func TestCombinedTxnTable_Stats(t *testing.T) {
	stats := &statsinfo.StatsInfo{
		BlockNumber:        1,
		ApproxObjectNumber: 2,
		TableCnt:           3,
	}
	table := newCombinedTxnTable(
		nil,
		func() ([]engine.Relation, error) {
			return []engine.Relation{
				nil,
				&mockRelation{
					statsFunc: func(context.Context, bool) (*statsinfo.StatsInfo, error) {
						return stats, nil
					},
				},
			}, nil
		},
		nil,
		nil,
	)

	result, err := table.Stats(context.Background(), false)
	assert.NoError(t, err)
	assert.Equal(t, stats.BlockNumber, result.BlockNumber)
	assert.Equal(t, stats.ApproxObjectNumber, result.ApproxObjectNumber)
	assert.Equal(t, stats.TableCnt, result.TableCnt)
}

// Test CombinedRelData panic methods
func TestCombinedRelData_GetType(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.GetType()
	})
}

func TestCombinedRelData_MarshalBinary(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.MarshalBinary()
	})
}

func TestCombinedRelData_UnmarshalBinary(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.UnmarshalBinary([]byte{})
	})
}

func TestCombinedRelData_GetTombstones(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.GetTombstones()
	})
}

func TestCombinedRelData_DataSlice(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.DataSlice(0, 1)
	})
}

func TestCombinedRelData_SetBlockInfo(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.SetBlockInfo(0, &objectio.BlockInfo{})
	})
}

func TestCombinedRelData_GetBlockInfo(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.GetBlockInfo(0)
	})
}

func TestCombinedRelData_AppendBlockInfo(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.AppendBlockInfo(&objectio.BlockInfo{})
	})
}

func TestCombinedRelData_AppendBlockInfoSlice(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.AppendBlockInfoSlice(objectio.BlockInfoSlice{})
	})
}

func TestCombinedRelData_Split(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.Split(0)
	})
}

// Test CombinedRelData non-panic methods
func TestCombinedRelData_String(t *testing.T) {
	data := &CombinedRelData{}
	assert.Equal(t, "PartitionedRelData", data.String())
}

func TestCombinedRelData_DataCnt(t *testing.T) {
	data := &CombinedRelData{
		cnt: 5,
	}
	assert.Equal(t, 5, data.DataCnt())
}

func TestCombinedRelData_GetBlockInfoSlice(t *testing.T) {
	data := &CombinedRelData{
		blocks: objectio.BlockInfoSlice{},
	}
	assert.Equal(t, objectio.BlockInfoSlice{}, data.GetBlockInfoSlice())
}

func TestCombinedRelData_AttachTombstones(t *testing.T) {
	data := &CombinedRelData{
		tables: []engine.RelData{},
	}
	// Should not panic when tables is empty
	assert.NoError(t, data.AttachTombstones(nil))
}

func TestCombinedRelData_BuildEmptyRelData(t *testing.T) {
	data := &CombinedRelData{
		tables: []engine.RelData{},
	}

	assert.PanicsWithValue(t, "BUG: no partitions", func() {
		data.BuildEmptyRelData(10)
	})
}

// Test newCombinedRelData
func TestNewCombinedRelData(t *testing.T) {
	data := newCombinedRelData()
	assert.NotNil(t, data)
	assert.Equal(t, 0, data.cnt)
	assert.Nil(t, data.blocks)
	assert.Nil(t, data.tables)
	assert.Nil(t, data.relations)
}

// Test add method with mock relation
func TestCombinedRelData_Add(t *testing.T) {
	data := newCombinedRelData()

	// Create a mock relation that returns a simple RelData
	mockRel := &mockRelation{
		rangesFunc: func(ctx context.Context, param engine.RangesParam) (engine.RelData, error) {
			return &mockRelData{
				dataCnt: 1,
				blocks:  objectio.BlockInfoSlice{},
			}, nil
		},
	}

	err := data.add(context.Background(), mockRel, engine.RangesParam{})
	assert.NoError(t, err)
	assert.Equal(t, 1, data.cnt)
	assert.Len(t, data.relations, 1)
	assert.Len(t, data.tables, 1)
}

// Mock implementations for testing

// Mock Tombstoner implementation
type mockTombstoner struct {
	hasAnyInMemoryTombstoneFunc func() bool
	hasAnyTombstoneFileFunc     func() bool
	mergeFunc                   func(other engine.Tombstoner) error
}

type mockChangesHandle struct {
	changes    []mockChange
	data       []*batch.Batch
	idx        int
	nextErr    error
	closed     bool
	closeErr   error
	closeCount int
}

func (m *mockChangesHandle) Next(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	if m.nextErr != nil {
		return nil, nil, engine.ChangesHandle_Tail_done, m.nextErr
	}
	if m.changes != nil {
		if m.idx >= len(m.changes) {
			return nil, nil, engine.ChangesHandle_Tail_done, nil
		}
		change := m.changes[m.idx]
		m.idx++
		return change.data, change.tombstone, change.hint, nil
	}
	if m.idx >= len(m.data) {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}
	bat := m.data[m.idx]
	m.idx++
	return bat, nil, engine.ChangesHandle_Tail_wip, nil
}

type mockChange struct {
	data      *batch.Batch
	tombstone *batch.Batch
	hint      engine.ChangesHandle_Hint
}

func newChangesTestBatch(t testing.TB, mp *mpool.MPool, values []int64, timestamps []types.TS) *batch.Batch {
	t.Helper()
	require.Len(t, values, len(timestamps))
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"value", "commit_ts"}
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	for i, value := range values {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], timestamps[i], false, mp))
	}
	bat.SetRowCount(len(values))
	return bat
}

func (m *mockChangesHandle) Close() error {
	m.closed = true
	m.closeCount++
	return m.closeErr
}

func (m *mockTombstoner) Type() engine.TombstoneType {
	return engine.TombstoneData
}

func (m *mockTombstoner) HasAnyInMemoryTombstone() bool {
	if m.hasAnyInMemoryTombstoneFunc != nil {
		return m.hasAnyInMemoryTombstoneFunc()
	}
	return false
}

func (m *mockTombstoner) HasAnyTombstoneFile() bool {
	if m.hasAnyTombstoneFileFunc != nil {
		return m.hasAnyTombstoneFileFunc()
	}
	return false
}

func (m *mockTombstoner) String() string {
	return "MockTombstoner"
}

func (m *mockTombstoner) StringWithPrefix(prefix string) string {
	return prefix + "MockTombstoner"
}

func (m *mockTombstoner) HasBlockTombstone(ctx context.Context, id *objectio.Blockid, fs fileservice.FileService) (bool, error) {
	return false, nil
}

func (m *mockTombstoner) MarshalBinaryWithBuffer(w *bytes.Buffer) error {
	return nil
}

func (m *mockTombstoner) UnmarshalBinary(buf []byte) error {
	return nil
}

func (m *mockTombstoner) PrefetchTombstones(ctx context.Context, srvId string, fs fileservice.FileService, bid []objectio.Blockid) {
}

func (m *mockTombstoner) ApplyInMemTombstones(bid *types.Blockid, rowsOffset []int64, deleted *objectio.Bitmap) (left []int64) {
	return rowsOffset
}

func (m *mockTombstoner) ApplyPersistedTombstones(ctx context.Context, fs fileservice.FileService, snapshot *types.TS, bid *types.Blockid, rowsOffset []int64, deletedMask *objectio.Bitmap) (left []int64, err error) {
	return rowsOffset, nil
}

func (m *mockTombstoner) Merge(other engine.Tombstoner) error {
	if m.mergeFunc != nil {
		return m.mergeFunc(other)
	}
	return nil
}

func (m *mockTombstoner) SortInMemory() {
}

type mockReader struct {
	filter docfilter.MembershipFilter
}

func (m *mockReader) Close() error {
	if m.filter != nil {
		m.filter.Free()
		m.filter = nil
	}
	return nil
}

func (m *mockReader) Read(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error) {
	return false, nil
}

func (m *mockReader) SetOrderBy([]*plan.OrderBySpec) {}

func (m *mockReader) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (m *mockReader) SetIndexParam(param *plan.IndexReaderParam) {}

func (m *mockReader) SetFilterZM(objectio.ZoneMap) {}

type mockRelation struct {
	rangesFunc                          func(ctx context.Context, param engine.RangesParam) (engine.RelData, error)
	getColumMetadataScanInfoFunc        func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error)
	getNonAppendableObjectStatsFunc     func(ctx context.Context) ([]objectio.ObjectStats, error)
	approxObjectsNumFunc                func(ctx context.Context) int
	collectTombstonesFunc               func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error)
	sizeFunc                            func(ctx context.Context, columnName string) (uint64, error)
	rowsFunc                            func(ctx context.Context) (uint64, error)
	statsFunc                           func(ctx context.Context, sync bool) (*statsinfo.StatsInfo, error)
	starCountFunc                       func(ctx context.Context) (uint64, error)
	estimateCommittedTombstoneCountFunc func(ctx context.Context) (int, error)
	collectChangesFunc                  func(ctx context.Context, from, to types.TS, skipDeletes bool, mp *mpool.MPool) (engine.ChangesHandle, error)
	buildReadersFunc                    func(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error)
}

func (m *mockRelation) Ranges(ctx context.Context, param engine.RangesParam) (engine.RelData, error) {
	return m.rangesFunc(ctx, param)
}

// Implement other required methods with empty implementations
func (m *mockRelation) BuildReaders(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error) {
	if m.buildReadersFunc != nil {
		return m.buildReadersFunc(ctx, proc, expr, relData, num, txnOffset, orderBy, policy, filterHint)
	}
	return nil, nil
}

func (m *mockRelation) BuildShardingReaders(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy) ([]engine.Reader, error) {
	return nil, nil
}

func (m *mockRelation) Rows(ctx context.Context) (uint64, error) {
	if m.rowsFunc != nil {
		return m.rowsFunc(ctx)
	}
	return 0, nil
}

func (m *mockRelation) Stats(ctx context.Context, sync bool) (*statsinfo.StatsInfo, error) {
	if m.statsFunc != nil {
		return m.statsFunc(ctx, sync)
	}
	return nil, nil
}

func (m *mockRelation) Size(ctx context.Context, columnName string) (uint64, error) {
	if m.sizeFunc != nil {
		return m.sizeFunc(ctx, columnName)
	}
	return 0, nil
}

func (m *mockRelation) CollectTombstones(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
	if m.collectTombstonesFunc != nil {
		return m.collectTombstonesFunc(ctx, txnOffset, policy)
	}
	return nil, nil
}

func (m *mockRelation) StarCount(ctx context.Context) (uint64, error) {
	if m.starCountFunc != nil {
		return m.starCountFunc(ctx)
	}
	return 0, nil
}

func (m *mockRelation) EstimateCommittedTombstoneCount(ctx context.Context) (int, error) {
	if m.estimateCommittedTombstoneCountFunc != nil {
		return m.estimateCommittedTombstoneCountFunc(ctx)
	}
	return 0, nil
}

func (m *mockRelation) CollectChanges(ctx context.Context, from, to types.TS, skipDeletes bool, mp *mpool.MPool) (engine.ChangesHandle, error) {
	if m.collectChangesFunc != nil {
		return m.collectChangesFunc(ctx, from, to, skipDeletes, mp)
	}
	return nil, nil
}

func (m *mockRelation) CollectObjectList(ctx context.Context, from, to types.TS, bat *batch.Batch, mp *mpool.MPool) error {
	return nil
}

func (m *mockRelation) ApproxObjectsNum(ctx context.Context) int {
	if m.approxObjectsNumFunc != nil {
		return m.approxObjectsNumFunc(ctx)
	}
	return 0
}

func (m *mockRelation) MergeObjects(ctx context.Context, objstats []objectio.ObjectStats, targetObjSize uint32) (*api.MergeCommitEntry, error) {
	return nil, nil
}

func (m *mockRelation) GetNonAppendableObjectStats(ctx context.Context) ([]objectio.ObjectStats, error) {
	if m.getNonAppendableObjectStatsFunc != nil {
		return m.getNonAppendableObjectStatsFunc(ctx)
	}
	return nil, nil
}

func (m *mockRelation) GetColumMetadataScanInfo(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
	if m.getColumMetadataScanInfoFunc != nil {
		return m.getColumMetadataScanInfoFunc(ctx, name, visitTombstone)
	}
	return nil, nil
}

func (m *mockRelation) UpdateConstraint(ctx context.Context, constraint *engine.ConstraintDef) error {
	return nil
}

func (m *mockRelation) AlterTable(ctx context.Context, c *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
	return nil
}

func (m *mockRelation) TableRenameInTxn(ctx context.Context, constraint [][]byte) error {
	return nil
}

func (m *mockRelation) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	return nil, nil, nil
}

func (m *mockRelation) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	return nil, nil
}

func (m *mockRelation) GetTableDef(ctx context.Context) *plan.TableDef {
	return nil
}

func (m *mockRelation) CopyTableDef(ctx context.Context) *plan.TableDef {
	return nil
}

func (m *mockRelation) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (m *mockRelation) AddTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (m *mockRelation) DelTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (m *mockRelation) GetTableID(ctx context.Context) uint64 {
	return 0
}

func (m *mockRelation) GetTableName() string {
	return ""
}

func (m *mockRelation) GetDBID(ctx context.Context) uint64 {
	return 0
}

func (m *mockRelation) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (m *mockRelation) GetEngineType() engine.EngineType {
	return engine.Disttae
}

func (m *mockRelation) GetProcess() any {
	return nil
}

func (m *mockRelation) PrimaryKeysMayBeModified(ctx context.Context, from types.TS, to types.TS, bat *batch.Batch, pkIndex int32, partitionIndex int32) (bool, error) {
	return false, nil
}

func (m *mockRelation) Write(ctx context.Context, bat *batch.Batch) error {
	return nil
}

func (m *mockRelation) Delete(ctx context.Context, bat *batch.Batch, s string) error {
	return nil
}

func (m *mockRelation) PrimaryKeysMayBeUpserted(ctx context.Context, from types.TS, to types.TS, bat *batch.Batch, pkIndex int32) (bool, error) {
	return false, nil
}

func (m *mockRelation) Reset(op client.TxnOperator) error {
	return nil
}

func (m *mockRelation) GetFlushTS(ctx context.Context) (types.TS, error) {
	return types.TS{}, nil
}

func (m *mockRelation) GetExtraInfo() *api.SchemaExtra {
	return nil
}

type mockRelData struct {
	dataCnt int
	blocks  objectio.BlockInfoSlice
}

func (m *mockRelData) GetType() engine.RelDataType {
	return engine.RelDataEmpty
}

func (m *mockRelData) String() string {
	return "MockRelData"
}

func (m *mockRelData) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *mockRelData) UnmarshalBinary(buf []byte) error {
	return nil
}

func (m *mockRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	return nil
}

func (m *mockRelData) GetTombstones() engine.Tombstoner {
	return nil
}

func (m *mockRelData) DataSlice(begin, end int) engine.RelData {
	return m
}

func (m *mockRelData) BuildEmptyRelData(preAllocSize int) engine.RelData {
	return m
}

func (m *mockRelData) DataCnt() int {
	return m.dataCnt
}

func (m *mockRelData) Split(i int) []engine.RelData {
	return nil
}

func (m *mockRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	return m.blocks
}

func (m *mockRelData) GetBlockInfo(i int) objectio.BlockInfo {
	return objectio.BlockInfo{}
}

func (m *mockRelData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
}

func (m *mockRelData) AppendBlockInfo(blk *objectio.BlockInfo) {
}

func (m *mockRelData) AppendBlockInfoSlice(objectio.BlockInfoSlice) {
}
