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

package blockio

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReaderSelectionColumnUses(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	output := batch.NewOffHeapWithSize(3)
	require.NoError(t, output.SetAllocationAccount(allocation))
	workspace := make([]*vector.Vector, 3)
	t.Cleanup(func() {
		output.Clean(mp)
		for _, v := range workspace {
			if v != nil {
				v.Free(mp)
			}
		}
	})
	for i := range output.Vecs {
		output.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), allocation)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixedList(output.Vecs[i], []int32{10, 20, 30, 40}, nil, mp))
	}
	output.SetRowCount(4)
	owned, unused := output.Vecs[1], output.Vecs[2]
	original := owned.GetData()
	request := &engine.ReaderFilterRequest{Workspace: workspace, OutputAllocation: allocation, ColumnUses: []engine.ReaderColumnUse{engine.ReaderColumnRetain, engine.ReaderColumnOwned, engine.ReaderColumnUnused}}
	require.NoError(t, publishReaderSelection(output, []int{0, 1, 2}, []int64{1, 3}, request, mp))
	require.Equal(t, 2, output.RowCount())
	require.True(t, output.Vecs[0].IsDictionary())
	require.NotNil(t, workspace[0])
	require.Nil(t, workspace[1])
	require.Nil(t, workspace[2])
	require.Same(t, owned, output.Vecs[1])
	require.Same(t, &original[0], &owned.GetData()[0])
	require.Equal(t, []int32{20, 40}, vector.MustFixedColNoTypeCheck[int32](owned))
	require.False(t, owned.HasBorrowedBacking())
	require.Same(t, unused, output.Vecs[2])
	require.Zero(t, unused.Length())
	require.Equal(t, int32(20), vector.GetFixedAtNoTypeCheck[int32](output.Vecs[0], 0))
	require.NoError(t, workspace[0].EnsureWritable(mp))
	vector.MustFixedColNoTypeCheck[int32](workspace[0])[1] = 99
	require.Equal(t, int32(20), vector.GetFixedAtNoTypeCheck[int32](output.Vecs[0], 0), "escaping view must retain immutable backing")
}

func TestReaderSelectionScratchFailureReleasesPrefix(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(uint64(2 * types.T_int32.TypeLen()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	output := batch.NewOffHeapWithSize(2)
	t.Cleanup(func() { output.Clean(mp) })
	for i := range output.Vecs {
		output.Vecs[i] = vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixedList(output.Vecs[i], []int32{10, 20, 30, 40}, nil, mp))
	}
	output.SetRowCount(4)
	request := &engine.ReaderFilterRequest{OutputAllocation: allocation}
	err = publishReaderSelection(output, []int{0, 1}, []int64{1, 3}, request, mp)
	require.True(t, errors.Is(err, mpool.ErrAllocationAccountCapacity))
	require.Zero(t, account.Snapshot().Used, "successfully gathered prefix must not leak")
	for _, v := range output.Vecs {
		require.Equal(t, []int32{10, 20, 30, 40}, vector.MustFixedColNoTypeCheck[int32](v))
	}
}
