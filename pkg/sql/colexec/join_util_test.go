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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

type testAppendCapacityController struct {
	limit atomic.Uint64
	used  atomic.Uint64
}

func (c *testAppendCapacityController) AcquireAllocationCapacity(size uint64) error {
	for {
		used := c.used.Load()
		limit := c.limit.Load()
		if size > limit || used > limit-size {
			return mpool.ErrAllocationAccountCapacity
		}
		if c.used.CompareAndSwap(used, used+size) {
			return nil
		}
	}
}

func (c *testAppendCapacityController) ReleaseAllocationCapacity(size uint64) {
	for {
		used := c.used.Load()
		if size > used {
			panic("test allocation capacity underflow")
		}
		if c.used.CompareAndSwap(used, used-size) {
			return
		}
	}
}

func BenchmarkCopyIntoBatchesPartialTail(b *testing.B) {
	const rowsPerInput = 128
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	input := testutil.NewBatch(
		[]types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
		},
		true,
		rowsPerInput,
		proc.Mp(),
	)
	defer input.Clean(proc.Mp())

	b.ResetTimer()
	for range b.N {
		var batches Batches
		for rows := 0; rows < DefaultBatchSize; rows += rowsPerInput {
			if err := batches.CopyIntoBatches(input, proc); err != nil {
				b.Fatal(err)
			}
		}
		batches.Clean(proc.Mp())
	}
}

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
		selection, err := vector.NewAllocationAccountSelection(
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

func TestCopyIntoBatchesAcceptsEquivalentAllocationSelection(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	first, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	second, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	require.NotSame(t, first, second)
	input := testutil.NewBatch(
		[]types.Type{types.T_int64.ToType()},
		true,
		32,
		proc.Mp(),
	)
	defer input.Clean(proc.Mp())

	var batches Batches
	require.NoError(t, batches.CopyIntoBatchesWithAllocation(input, proc, first))
	require.NoError(t, batches.CopyIntoBatchesWithAllocation(input, proc, second))
	require.Len(t, batches.Buf, 1)
	require.Equal(t, 64, batches.RowCount())
	require.Same(t, first, batches.Buf[0].AllocationAccountSelection())

	batches.Clean(proc.Mp())
	require.Zero(t, account.Snapshot().Used)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestCopyIntoBatchesAllocationFailureRollsBackPartialTail(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	controller := &testAppendCapacityController{}
	controller.limit.Store(1 << 60)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<60, controller)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	typesInTail := []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}
	initial := testutil.NewBatch(typesInTail, true, 1, proc.Mp())
	defer initial.Clean(proc.Mp())
	var batches Batches
	require.NoError(t, batches.CopyIntoBatchesWithAllocation(initial, proc, selection))
	require.Len(t, batches.Buf, 1)
	require.Equal(t, 1, batches.RowCount())
	firstBefore := append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](batches.Buf[0].Vecs[0])...)
	secondBefore := append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](batches.Buf[0].Vecs[1])...)

	const appendRows = 128
	oldCapacity := cap(batches.Buf[0].Vecs[0].GetData())
	requiredBytes := (batches.RowCount() + appendRows) * types.T_int64.ToType().TypeSize()
	newCapacity, ok := mpool.GrowCapacity(int64(oldCapacity), int64(requiredBytes))
	require.True(t, ok)
	require.Greater(t, newCapacity, int64(oldCapacity))
	controller.limit.Store(controller.used.Load() + uint64(newCapacity))

	more := testutil.NewBatch(typesInTail, true, appendRows, proc.Mp())
	defer more.Clean(proc.Mp())
	err = batches.CopyIntoBatchesWithAllocation(more, proc, selection)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Len(t, batches.Buf, 1)
	require.Equal(t, 1, batches.RowCount())
	require.Equal(t, firstBefore, vector.MustFixedColNoTypeCheck[int64](batches.Buf[0].Vecs[0]))
	require.Equal(t, secondBefore, vector.MustFixedColNoTypeCheck[int64](batches.Buf[0].Vecs[1]))

	controller.limit.Store(1 << 60)
	require.NoError(t, batches.CopyIntoBatchesWithAllocation(more, proc, selection))
	require.Equal(t, 1+appendRows, batches.RowCount())
	batches.Clean(proc.Mp())
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, controller.used.Load())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestCopyIntoBatchesFailureRollsBackEarlierTailChunk(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	controller := &testAppendCapacityController{}
	controller.limit.Store(1 << 60)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<60, controller)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	typesInTail := []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}
	initial := testutil.NewBatch(typesInTail, true, DefaultBatchSize-2, proc.Mp())
	defer initial.Clean(proc.Mp())
	var batches Batches
	require.NoError(t, batches.CopyIntoBatchesWithAllocation(initial, proc, selection))
	firstBefore := append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](batches.Buf[0].Vecs[0])...)
	secondBefore := append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](batches.Buf[0].Vecs[1])...)

	// Filling the last two rows needs no growth. Reject creation of the next
	// batch, after that first chunk has already succeeded.
	controller.limit.Store(controller.used.Load())
	more := testutil.NewBatch(typesInTail, true, 128, proc.Mp())
	defer more.Clean(proc.Mp())
	err = batches.CopyIntoBatchesWithAllocation(more, proc, selection)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Len(t, batches.Buf, 1)
	require.Equal(t, DefaultBatchSize-2, batches.RowCount())
	require.Equal(t, firstBefore, vector.MustFixedColNoTypeCheck[int64](batches.Buf[0].Vecs[0]))
	require.Equal(t, secondBefore, vector.MustFixedColNoTypeCheck[int64](batches.Buf[0].Vecs[1]))

	controller.limit.Store(1 << 60)
	require.NoError(t, batches.CopyIntoBatchesWithAllocation(more, proc, selection))
	require.Equal(t, DefaultBatchSize-2+128, batches.RowCount())
	batches.Clean(proc.Mp())
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, controller.used.Load())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}
