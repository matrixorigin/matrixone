// Copyright 2021 Matrix Origin
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

package batch

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const (
	TestBatchMaxRow = 8192
)

func newInt64TestBatch(t *testing.T, mp *mpool.MPool, values ...int64) *Batch {
	t.Helper()
	bat := NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], values, nil, mp))
	bat.SetRowCount(len(values))
	return bat
}

func int64Rows(bat *Batch) []int64 {
	return vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])
}

func seqSels(n int) []int32 {
	sels := make([]int32, n)
	for i := range sels {
		sels[i] = int32(i)
	}
	return sels
}

// requireFullExceptLast checks the set's shape: every batch but the last full.
func requireFullExceptLast(t *testing.T, bs *BatchSet, rows int) {
	t.Helper()
	require.Equal(t, rows, bs.RowCount())
	require.Equal(t, (rows+bs.maxRows-1)/bs.maxRows, bs.Length())
	for i := 0; i < bs.Length()-1; i++ {
		require.Equal(t, bs.maxRows, bs.Get(i).RowCount(), "batch %d", i)
	}
}

func TestBatchSetAppendFillsTailThenSplits(t *testing.T) {
	mp := mpool.MustNewZero()
	input := newInt64TestBatch(t, mp, 10, 11, 12, 13, 14, 15, 16)
	bs := NewBatchSet(3)
	t.Cleanup(func() {
		bs.Clean(mp)
		input.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	_, err := bs.Append(mp, input, 1, 2, nil)
	require.NoError(t, err)
	_, err = bs.Append(mp, input, 3, 4, nil)
	require.NoError(t, err)
	require.Equal(t, 2, bs.Length())
	require.Equal(t, []int64{11, 12, 13}, int64Rows(bs.Get(0)))
	require.Equal(t, []int64{14, 15, 16}, int64Rows(bs.Get(1)))

	bs.Clean(mp)
	_, err = bs.Append(mp, input, 0, input.RowCount(), nil)
	require.NoError(t, err)
	requireFullExceptLast(t, bs, 7)
	require.Equal(t, []int64{16}, int64Rows(bs.Get(2)))

	_, err = bs.Append(mp, input, 0, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 3, bs.Length(), "appending no rows changes nothing")
}

func TestBatchSetAppendSels(t *testing.T) {
	mp := mpool.MustNewZero()
	input := newInt64TestBatch(t, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	bs := NewBatchSet(4)
	t.Cleanup(func() {
		bs.Clean(mp)
		input.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	_, err := bs.AppendSels(mp, input, nil, nil)
	require.NoError(t, err)
	require.Zero(t, bs.Length())
	_, err = bs.AppendSels(mp, input, []int32{9, 7, 5}, nil)
	require.NoError(t, err)
	_, err = bs.AppendSels(mp, input, []int32{0, 2, 4, 6, 8}, nil)
	require.NoError(t, err)
	requireFullExceptLast(t, bs, 8)
	require.Equal(t, []int64{9, 7, 5, 0}, int64Rows(bs.Get(0)))
	require.Equal(t, []int64{2, 4, 6, 8}, int64Rows(bs.Get(1)))

	// A selection of every row copies the batch in order.
	_, err = bs.AppendSels(mp, input, seqSels(10), nil)
	require.NoError(t, err)
	requireFullExceptLast(t, bs, 18)
	require.Equal(t, []int64{0, 1, 2, 3}, int64Rows(bs.Get(2)))

	require.Panics(t, func() { _, _ = bs.AppendSels(mp, input, seqSels(11), nil) })
}

func TestBatchSetLargeAppendsKeepShape(t *testing.T) {
	mp := mpool.MustNewZero()
	input := makeTestBatch(20000, mp)
	bs := NewBatchSet(TestBatchMaxRow)
	t.Cleanup(func() {
		bs.Clean(mp)
		input.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	_, err := bs.AppendSels(mp, input, seqSels(5000), nil)
	require.NoError(t, err)
	_, err = bs.AppendSels(mp, input, seqSels(15000), nil)
	require.NoError(t, err)
	requireFullExceptLast(t, bs, 20000)
	_, err = bs.Append(mp, input, 1, 19999, nil)
	require.NoError(t, err)
	requireFullExceptLast(t, bs, 39999)
}

// Copies charge the pool the caller passes, never the input's. Rows copied
// through another pool close a partial tail, and ReadyDelta predicts that.
func TestBatchSetCopiesThroughCallerPool(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(2, 32)
	require.NoError(t, err)
	destination, err := registry.Open(1 << 20)
	require.NoError(t, err)
	source, err := registry.Open(1 << 20)
	require.NoError(t, err)
	dstView, err := mp.WithAccount(destination, mpool.AllocationOwnerSet, 9, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)
	srcView, err := mp.WithAccount(source, mpool.AllocationOwnerSet, 9, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)
	bs := NewBatchSet(4)
	var inputs []*Batch
	var inputPools []*mpool.MPool
	popped := make([]*Batch, 0, 1)
	t.Cleanup(func() {
		for _, bat := range popped {
			bat.Clean(bat.Vecs[0].MPool())
		}
		for _, bat := range bs.TakeBatches() {
			bat.Clean(bat.Vecs[0].MPool())
		}
		for i, input := range inputs {
			input.Clean(inputPools[i])
		}
		for _, account := range []*mpool.AllocationAccount{destination, source} {
			require.Zero(t, account.Seal().Used)
			_, err := registry.Finalize(account)
			require.NoError(t, err)
		}
		require.Zero(t, mp.CurrNB())
	})
	newInput := func(alloc *mpool.MPool, values ...int64) *Batch {
		input := newInt64TestBatch(t, alloc, values...)
		inputs = append(inputs, input)
		inputPools = append(inputPools, alloc)
		return input
	}

	first := newInput(mp, 1, 2)
	require.Zero(t, bs.ReadyDelta(first, dstView, 2))
	_, err = bs.Append(dstView, first, 0, 2, nil)
	require.NoError(t, err)
	require.Positive(t, destination.Snapshot().Used)

	second := newInput(srcView, 3, 4)
	sourceUsed := source.Snapshot().Used
	require.Equal(t, 1, bs.ReadyDelta(second, dstView, 2))
	_, err = bs.AppendSels(dstView, second, []int32{0, 1}, nil)
	require.NoError(t, err)
	require.Equal(t, sourceUsed, source.Snapshot().Used, "copies must not charge the input's account")
	require.Equal(t, 1, bs.ReadyCount())
	full := bs.PopFront()
	popped = append(popped, full)
	require.Equal(t, []int64{1, 2, 3, 4}, int64Rows(full))

	// Through another pool, the partial tail closes and the rows start anew.
	third := newInput(mp, 5, 6, 7)
	_, err = bs.Append(dstView, third, 0, 1, nil)
	require.NoError(t, err)
	require.Equal(t, 1, bs.ReadyDelta(third, mp, 2))
	_, err = bs.Append(mp, third, 1, 2, nil)
	require.NoError(t, err)
	require.Equal(t, 2, bs.Length())
	require.Equal(t, 1, bs.ReadyCount())
	require.Equal(t, []int64{5}, int64Rows(bs.Get(0)))
	require.Equal(t, []int64{6, 7}, int64Rows(bs.Get(1)))
}

func TestBatchSetCopyFailureLeavesBatchOwned(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	view, err := mp.WithAccount(account, mpool.AllocationOwnerSet, 9, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)
	bs := NewBatchSet(4)
	input := newInt64TestBatch(t, mp, 1, 2)
	t.Cleanup(func() {
		bs.Clean(view)
		input.Clean(mp)
		require.Zero(t, account.Seal().Used)
		_, err := registry.Finalize(account)
		require.NoError(t, err)
		require.Zero(t, mp.CurrNB())
	})

	_, err = bs.Append(view, input, 0, 2, nil)
	require.Error(t, err)
	require.Equal(t, []int64{1, 2}, int64Rows(input))
}

func TestBatchSetSortedMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		add  func(bs *BatchSet, mp *mpool.MPool, src *Batch) error
	}{
		{name: "append", add: func(bs *BatchSet, mp *mpool.MPool, src *Batch) error {
			_, err := bs.Append(mp, src, 0, src.RowCount(), nil)
			return err
		}},
		{name: "sels", add: func(bs *BatchSet, mp *mpool.MPool, src *Batch) error {
			_, err := bs.AppendSels(mp, src, []int32{0, 1}, nil)
			return err
		}},
		{name: "push overflow", add: func(bs *BatchSet, mp *mpool.MPool, src *Batch) error {
			return bs.Push(mp, src)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			bs := NewBatchSet(8)
			sorted := func(values ...int64) *Batch {
				bat := newInt64TestBatch(t, mp, values...)
				bat.Vecs[0].SetSorted(true)
				return bat
			}
			first := sorted(10, 11)
			_, err := bs.Append(mp, first, 0, 2, nil)
			require.NoError(t, err)
			first.Clean(mp)
			require.True(t, bs.Get(0).Vecs[0].GetSorted(), "a run of a sorted column is sorted")

			second := sorted(1, 2, 3, 4, 5, 6, 7)
			require.NoError(t, tc.add(bs, mp, second))
			if tc.name != "push overflow" {
				second.Clean(mp)
			}
			require.False(t, bs.Get(0).Vecs[0].GetSorted(), "concatenated rows have no order guarantee")

			bs.Clean(mp)
			unsorted := sorted(3, 1, 2)
			_, err = bs.AppendSels(mp, unsorted, []int32{1, 0}, nil)
			require.NoError(t, err)
			require.False(t, bs.Get(0).Vecs[0].GetSorted(), "selected rows need not be sorted")
			unsorted.Clean(mp)
			bs.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestBatchSetPush(t *testing.T) {
	mp := mpool.MustNewZero()
	bs := NewBatchSet(TestBatchMaxRow)

	// An empty batch is freed, not kept.
	empty := newInt64TestBatch(t, mp)
	require.NoError(t, bs.Push(mp, empty))
	require.Zero(t, bs.Length())

	// The first batch is adopted as is; the next joins its partial tail.
	first := makeTestBatch(10, mp)
	require.NoError(t, bs.Push(mp, first))
	require.Same(t, first, bs.Get(0))
	require.NoError(t, bs.Push(mp, makeTestBatch(10, mp)))
	requireFullExceptLast(t, bs, 20)

	// Rows beyond the tail's room split into new batches.
	require.NoError(t, bs.Push(mp, makeTestBatch(2*TestBatchMaxRow+1, mp)))
	requireFullExceptLast(t, bs, 2*TestBatchMaxRow+21)

	// After a full tail, a batch is adopted again.
	bs.Clean(mp)
	require.NoError(t, bs.Push(mp, makeTestBatch(TestBatchMaxRow, mp)))
	last := makeTestBatch(3, mp)
	require.NoError(t, bs.Push(mp, last))
	require.Same(t, last, bs.Get(1))

	bs.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestBatchSetReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	other := mpool.MustNewZero()
	src := makeTestBatch(TestBatchMaxRow, mp)
	t.Cleanup(func() {
		src.Clean(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, other.CurrNB())
	})

	t.Run("taken for a new batch", func(t *testing.T) {
		bs := NewBatchSet(TestBatchMaxRow)
		reuse := makeTestBatch(5, mp)
		consumed, err := bs.AppendSels(mp, src, []int32{0, 1, 2}, reuse)
		require.NoError(t, err)
		require.True(t, consumed)
		require.Same(t, reuse, bs.Get(0))
		require.Equal(t, 3, bs.RowCount(), "reuse is emptied first")
		bs.Clean(mp)
	})

	t.Run("not taken while the tail has room", func(t *testing.T) {
		bs := NewBatchSet(TestBatchMaxRow)
		_, err := bs.Append(mp, src, 0, 3, nil)
		require.NoError(t, err)
		reuse := makeTestBatch(5, mp)
		consumed, err := bs.Append(mp, src, 0, 3, reuse)
		require.NoError(t, err)
		require.False(t, consumed)
		reuse.Clean(mp)
		bs.Clean(mp)
	})

	t.Run("taken when the rows overflow the tail", func(t *testing.T) {
		bs := NewBatchSet(TestBatchMaxRow)
		_, err := bs.Append(mp, src, 0, 3, nil)
		require.NoError(t, err)
		reuse := makeTestBatch(5, mp)
		consumed, err := bs.Append(mp, src, 0, TestBatchMaxRow-1, reuse)
		require.NoError(t, err)
		require.True(t, consumed)
		require.Same(t, reuse, bs.Get(1))
		requireFullExceptLast(t, bs, TestBatchMaxRow+2)
		bs.Clean(mp)
	})

	t.Run("not taken from another pool", func(t *testing.T) {
		bs := NewBatchSet(TestBatchMaxRow)
		reuse := makeTestBatch(5, other)
		consumed, err := bs.Append(mp, src, 0, 3, reuse)
		require.NoError(t, err)
		require.False(t, consumed)
		reuse.Clean(other)
		bs.Clean(mp)
	})
}

func TestBatchSetReadiness(t *testing.T) {
	mp := mpool.MustNewZero()
	bs := NewBatchSet(TestBatchMaxRow)
	src := makeTestBatch(TestBatchMaxRow, mp)
	t.Cleanup(func() {
		src.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})
	require.Zero(t, bs.ReadyCount())
	require.Zero(t, bs.ReadyDelta(src, mp, 1))
	require.Equal(t, 1, bs.ReadyDelta(src, mp, TestBatchMaxRow))
	require.Zero(t, bs.ReadyDelta(src, mp, 0))

	_, err := bs.AppendSels(mp, src, []int32{0}, nil)
	require.NoError(t, err)
	require.Zero(t, bs.ReadyCount())
	require.Less(t, bs.Get(0).Allocated(), TestBatchMaxRow, "a sparse tail must not preallocate a full batch")
	require.Equal(t, 1, bs.ReadyDelta(src, mp, TestBatchMaxRow-1))
	require.Equal(t, 2, bs.ReadyDelta(src, mp, 2*TestBatchMaxRow-1))

	bs.Clean(mp)
	_, err = bs.Append(mp, src, 0, TestBatchMaxRow, nil)
	require.NoError(t, err)
	require.Equal(t, 1, bs.ReadyCount(), "a full last batch is ready")
	require.Equal(t, 1, bs.ReadyDelta(src, mp, TestBatchMaxRow))
	bs.Clean(mp)
}

func TestBatchSetTakePopAndGet(t *testing.T) {
	mp := mpool.MustNewZero()
	bs := NewBatchSet(2)
	src := newInt64TestBatch(t, mp, 1, 2, 3, 4, 5)
	_, err := bs.Append(mp, src, 0, 5, nil)
	require.NoError(t, err)
	src.Clean(mp)
	require.Nil(t, bs.Get(-1))
	require.Nil(t, bs.Get(3))

	last := bs.Pop()
	require.Equal(t, []int64{5}, int64Rows(last))
	last.Clean(mp)
	first := bs.PopFront()
	require.Equal(t, []int64{1, 2}, int64Rows(first))
	first.Clean(mp)

	batches := bs.TakeBatches()
	require.Len(t, batches, 1)
	require.Zero(t, bs.Length())
	require.Nil(t, bs.Pop())
	require.Nil(t, bs.PopFront())
	for _, bat := range batches {
		bat.Clean(mp)
	}
	require.Zero(t, mp.CurrNB())
	require.Panics(t, func() { NewBatchSet(0) })
}

func makeTestBatch(max int, mp *mpool.MPool) *Batch {
	bat := NewWithSize(2)
	bat.Vecs[0] = makeTestVec(max, mp)
	bat.Vecs[1] = makeConstTestVec(max, mp)
	bat.rowCount = bat.Vecs[0].Length()
	return bat
}

func makeTestVec(max int, mp *mpool.MPool) *vector.Vector {
	typ := types.T_int32.ToType()
	vec := vector.NewVec(typ, mp)
	for i := 0; i < max; i++ {
		v := i
		if err := vector.AppendFixed(vec, int32(v), false, mp); err != nil {
			vec.Free(mp)
			return nil
		}
	}
	return vec
}

func makeConstTestVec(max int, mp *mpool.MPool) *vector.Vector {
	typ := types.T_int32.ToType()
	val := int32(10)
	vec, _ := vector.NewConstFixed(typ, val, max, mp)
	return vec
}

func TestBatchSetEmptyAppendKeepsSortedTail(t *testing.T) {
	mp := mpool.MustNewZero()
	src := newInt64TestBatch(t, mp, 1, 2, 3)
	src.Vecs[0].SetSorted(true)
	bs := NewBatchSet(8)
	_, err := bs.Append(mp, src, 0, 2, nil)
	require.NoError(t, err)
	_, err = bs.Append(mp, src, 2, 0, nil)
	require.NoError(t, err)
	_, err = bs.AppendSels(mp, src, nil, nil)
	require.NoError(t, err)
	require.True(t, bs.Get(0).Vecs[0].GetSorted())

	var selection bitmap.Bitmap
	require.NoError(t, selection.InitWithSize(2, mp))
	selection.Add(1)
	selected := newInt64TestBatch(t, mp, 1, 2)
	selected.SetSelection(&selection)
	_, err = bs.Append(mp, selected, 0, 1, nil)
	require.Error(t, err, "a range needs compacted rows")
	selected.ClearSelection()
	selection.Free()
	selected.Clean(mp)
	src.Clean(mp)
	bs.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

// Shuffle's pattern: small selections into a long tail, recycling each full
// batch as the next reuse buffer.
func BenchmarkBatchSetAppendSels(b *testing.B) {
	mp := mpool.MustNewZero()
	src := NewWithSize(2)
	src.Vecs[0] = makeTestVec(1024, mp)
	src.Vecs[1] = makeTestVec(1024, mp)
	src.SetRowCount(1024)
	sels := make([]int32, 64)
	for i := range sels {
		sels[i] = int32(i * 16)
	}
	bs := NewBatchSet(TestBatchMaxRow)
	var spare *Batch
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		took, err := bs.AppendSels(mp, src, sels, spare)
		if err != nil {
			b.Fatal(err)
		}
		if took {
			spare = nil
		}
		if bs.ReadyCount() > 0 {
			spare = bs.PopFront()
		}
	}
	b.StopTimer()
	if spare != nil {
		spare.Clean(mp)
	}
	bs.Clean(mp)
	src.Clean(mp)
}

// A failed copy into the last batch leaves it owned by the set and freeable.
func TestBatchSetTailCopyFailure(t *testing.T) {
	values := make([]int64, 2000)
	for _, tc := range []struct {
		name string
		add  func(bs *BatchSet, mp *mpool.MPool, src *Batch) error
	}{
		{name: "range", add: func(bs *BatchSet, mp *mpool.MPool, src *Batch) error {
			_, err := bs.Append(mp, src, 0, 1000, nil)
			return err
		}},
		{name: "sels", add: func(bs *BatchSet, mp *mpool.MPool, src *Batch) error {
			_, err := bs.AppendSels(mp, src, seqSels(1000), nil)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, err := mpool.NewAllocationAccountRegistry(1, 16)
			require.NoError(t, err)
			account, err := registry.Open(1024)
			require.NoError(t, err)
			view, err := mp.WithAccount(account, mpool.AllocationOwnerSet, 9, mpool.AllocationCapacityClassDefault)
			require.NoError(t, err)
			src := newInt64TestBatch(t, mp, values...)
			bs := NewBatchSet(TestBatchMaxRow)
			_, err = bs.Append(view, src, 0, 1, nil)
			require.NoError(t, err)
			require.Error(t, tc.add(bs, view, src))
			require.Equal(t, 1, bs.Length())
			bs.Clean(view)
			src.Clean(mp)
			require.Zero(t, account.Seal().Used)
			_, err = registry.Finalize(account)
			require.NoError(t, err)
			require.Zero(t, mp.CurrNB())
		})
	}
}
