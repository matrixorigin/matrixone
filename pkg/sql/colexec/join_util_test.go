// Copyright 2024 Matrix Origin
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

package colexec

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestBatches(t *testing.T) {
	var batches Batches
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	inputBatch := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(100000), proc.Mp())
	err := batches.CopyIntoBatches(inputBatch, proc)
	inputBatch.Clean(proc.Mp())
	require.NoError(t, err)
	require.Equal(t, 13, len(batches.Buf))
	require.Equal(t, 8192, batches.Buf[0].RowCount())
	require.Equal(t, 8192, batches.Buf[8].RowCount())
	require.Equal(t, 8192, batches.Buf[11].RowCount())
	require.Equal(t, 1696, batches.Buf[12].RowCount())
	for _, bat := range batches.Buf {
		for _, vec := range bat.Vecs {
			require.LessOrEqual(t, vec.Capacity(), DefaultBatchSize)
		}
	}
	batches.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())

	inputBatch = testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(10), proc.Mp())
	err = batches.CopyIntoBatches(inputBatch, proc)
	require.NoError(t, err)
	inputBatch.Clean(proc.Mp())
	inputBatch = testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(8192), proc.Mp())
	err = batches.CopyIntoBatches(inputBatch, proc)
	require.NoError(t, err)
	inputBatch.Clean(proc.Mp())
	inputBatch = testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(10), proc.Mp())
	err = batches.CopyIntoBatches(inputBatch, proc)
	require.NoError(t, err)
	inputBatch.Clean(proc.Mp())
	inputBatch = testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(8192), proc.Mp())
	err = batches.CopyIntoBatches(inputBatch, proc)
	require.NoError(t, err)
	inputBatch.Clean(proc.Mp())
	require.Equal(t, 3, len(batches.Buf))
	require.Equal(t, 8192, batches.Buf[0].RowCount())
	require.Equal(t, 8192, batches.Buf[1].RowCount())
	require.Equal(t, 20, batches.Buf[2].RowCount())

	rowCnt := batches.RowCount()
	bm := &bitmap.Bitmap{}
	bm.InitWithSize(int64(rowCnt))
	bm.AddRange(1000, 11000)
	batches.Shrink(bm, proc)
	require.Equal(t, rowCnt-10000, batches.RowCount())

	batches.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBatchesShrinkPreservesAllocationAndRollback(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	input := testutil.NewBatch(
		[]types.Type{types.T_int32.ToType()},
		true,
		DefaultBatchSize,
		proc.Mp(),
	)
	defer input.Clean(proc.Mp())

	measure := func(limit uint64, shrink bool) (uint64, error) {
		registry, err := mpool.NewAllocationAccountRegistry(1, 32)
		require.NoError(t, err)
		account, err := registry.Open(limit)
		require.NoError(t, err)
		selection, err := vector.NewAllocationAccountSelectionWithBitmaps(
			account, 1, 1, 2, 3, 4)
		require.NoError(t, err)
		var batches Batches
		require.NoError(t, batches.CopyIntoBatchesWithAllocation(input, proc, selection))
		before := account.Snapshot().Used
		require.Positive(t, before)
		var shrinkErr error
		if shrink {
			ignore := &bitmap.Bitmap{}
			ignore.InitWithSize(DefaultBatchSize)
			ignore.Add(0)
			shrinkErr = batches.Shrink(ignore, proc)
			if shrinkErr == nil {
				require.Equal(t, DefaultBatchSize-1, batches.RowCount())
				for _, bat := range batches.Buf {
					require.Same(t, selection, bat.AllocationAccountSelection())
				}
			} else {
				require.Equal(t, 1, ignore.Count(), "failed shrink restores ignore-row checkpoint")
				require.Equal(t, DefaultBatchSize, batches.RowCount())
				require.Equal(t, before, account.Snapshot().Used)
			}
		}
		batches.Clean(proc.Mp())
		require.Zero(t, account.Snapshot().Used)
		_, _, err = registry.CompleteTerminal(account)
		require.NoError(t, err)
		return before, shrinkErr
	}

	used, err := measure(1<<20, false)
	require.NoError(t, err)
	_, err = measure(used, true)
	require.Error(t, err)
	require.True(t, mpool.IsRetryableAllocationCapacity(err))
	_, err = measure(1<<20, true)
	require.NoError(t, err)
}
