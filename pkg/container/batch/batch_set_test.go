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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const (
	TestBatchMaxRow = 8192
)

func TestBatchSetPush(t *testing.T) {
	var err error
	var bat1, bat2 *Batch
	mp := mpool.MustNewZero()
	queue := NewBatchSet(TestBatchMaxRow)

	//empty input
	bat1 = NewWithSize(1)
	err = queue.Push(mp, bat1)
	require.NoError(t, err)
	require.Nil(t, queue.Get(0))
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	//simple test
	bat1 = makeTestBatch(10, mp)
	err = queue.Push(mp, bat1)
	require.NoError(t, err)
	require.Equal(t, 1, queue.Length())
	require.Equal(t, 10, queue.RowCount())
	require.Equal(t, 10, queue.Get(0).rowCount)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount < DefaultBatchMaxRow
	bat1 = makeTestBatch(10, mp)
	bat2 = makeTestBatch(10, mp)
	_ = queue.Push(mp, bat1)
	err = queue.Push(mp, bat2)
	require.NoError(t, err)
	require.Equal(t, 1, queue.Length())
	require.Equal(t, 20, queue.RowCount())
	require.Equal(t, 20, queue.Get(0).rowCount)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount > DefaultBatchMaxRow
	// but bat1.rowCount + bat2.rowCount - DefaultBatchMaxRow < DefaultBatchMaxRow
	bat1 = makeTestBatch(3, mp)
	bat2 = makeTestBatch(8192, mp)
	_ = queue.Push(mp, bat1)
	err = queue.Push(mp, bat2)
	require.NoError(t, err)
	require.Equal(t, 2, queue.Length())
	require.Equal(t, 8195, queue.RowCount())
	require.Equal(t, 8192, queue.Get(0).rowCount)
	require.Equal(t, 3, queue.Get(1).rowCount)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount > DefaultBatchMaxRow
	// but bat1.rowCount + bat2.rowCount - DefaultBatchMaxRow > DefaultBatchMaxRow
	bat1 = makeTestBatch(3, mp)
	bat2 = makeTestBatch(8192*2+1, mp)
	_ = queue.Push(mp, bat1)
	err = queue.Push(mp, bat2)
	require.NoError(t, err)
	require.Equal(t, 3, queue.Length())
	require.Equal(t, 8192*2+1+3, queue.RowCount())
	require.Equal(t, 8192, queue.Get(0).rowCount)
	require.Equal(t, 8192, queue.Get(1).rowCount)
	require.Equal(t, 4, queue.Get(2).rowCount)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestBatchSetExtend(t *testing.T) {
	var err error
	var bat1, bat2 *Batch
	mp := mpool.MustNewZero()
	queue := NewBatchSet(TestBatchMaxRow)

	//empty input
	bat1 = NewWithSize(1)
	_, err = queue.Extend(mp, bat1, nil)
	bat1.Clean(mp)
	require.NoError(t, err)
	require.Nil(t, queue.Get(0))
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	//simple test
	bat1 = makeTestBatch(10, mp)
	_, err = queue.Extend(mp, bat1, nil)
	bat1.Clean(mp)
	require.NoError(t, err)
	require.Equal(t, 1, queue.Length())
	require.Equal(t, 10, queue.RowCount())
	require.Equal(t, 10, queue.Get(0).rowCount)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount < DefaultBatchMaxRow
	bat1 = makeTestBatch(10, mp)
	bat2 = makeTestBatch(10, mp)
	_, _ = queue.Extend(mp, bat1, nil)
	_, err = queue.Extend(mp, bat2, nil)
	require.NoError(t, err)
	bat1.Clean(mp)
	bat2.Clean(mp)
	require.Equal(t, 1, queue.Length())
	require.Equal(t, 20, queue.RowCount())
	require.Equal(t, 20, queue.Get(0).rowCount)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount > DefaultBatchMaxRow
	// but bat1.rowCount + bat2.rowCount - DefaultBatchMaxRow < DefaultBatchMaxRow
	bat1 = makeTestBatch(3, mp)
	bat2 = makeTestBatch(8192, mp)
	_, _ = queue.Extend(mp, bat1, nil)
	_, err = queue.Extend(mp, bat2, nil)
	require.NoError(t, err)
	bat1.Clean(mp)
	bat2.Clean(mp)
	require.Equal(t, 2, queue.Length())
	require.Equal(t, 8195, queue.RowCount())
	require.Equal(t, 8192, queue.Get(0).rowCount)
	require.Equal(t, 3, queue.Get(1).rowCount)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount > DefaultBatchMaxRow
	// but bat1.rowCount + bat2.rowCount - DefaultBatchMaxRow > DefaultBatchMaxRow
	bat1 = makeTestBatch(3, mp)
	bat2 = makeTestBatch(8192*2+1, mp)
	_, _ = queue.Extend(mp, bat1, nil)
	_, err = queue.Extend(mp, bat2, nil)
	require.NoError(t, err)
	bat1.Clean(mp)
	bat2.Clean(mp)
	require.Equal(t, 3, queue.Length())
	require.Equal(t, 8192*2+1+3, queue.RowCount())
	require.Equal(t, 8192, queue.Get(0).rowCount)
	require.Equal(t, 8192, queue.Get(1).rowCount)
	require.Equal(t, 4, queue.Get(2).rowCount)

	bat := queue.Pop()
	require.Equal(t, 4, bat.rowCount)
	bat.Clean(mp)

	bat = queue.PopFront()
	require.Equal(t, 8192, bat.rowCount)
	bat.Clean(mp)

	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestBatchSetUnion(t *testing.T) {
	mp := mpool.MustNewZero()
	queue := NewBatchSet(TestBatchMaxRow)

	bat1 := makeTestBatch(50, mp)
	bat2 := makeTestBatch(8192, mp)

	_, err := queue.Union(mp, bat1, []int32{}, nil)
	require.NoError(t, err)

	_, err = queue.Union(mp, bat1, []int32{1, 2, 3, 4, 5}, nil)
	require.NoError(t, err)
	require.Equal(t, 5, queue.RowCount())

	_, err = queue.Union(mp, bat1, []int32{1, 2, 3, 4, 5}, nil)
	require.NoError(t, err)
	require.Equal(t, 10, queue.RowCount())
	require.Equal(t, 1, queue.Length())

	var sels []int32
	for i := 0; i < 8192; i++ {
		sels = append(sels, int32(i))
	}
	_, err = queue.Union(mp, bat2, sels, nil)
	require.NoError(t, err)
	require.Equal(t, 8192+10, queue.RowCount())
	require.Equal(t, 2, queue.Length())

	sels = nil
	for i := 0; i < 8182; i++ {
		sels = append(sels, int32(i))
	}
	_, err = queue.Union(mp, bat2, sels, nil)
	require.NoError(t, err)
	require.Equal(t, 8192+10+8182, queue.RowCount())
	require.Equal(t, 2, queue.Length())

	_, err = queue.Union(mp, bat1, []int32{1, 2, 3, 4, 5}, nil)
	require.NoError(t, err)
	require.Equal(t, 8192+10+8182+5, queue.RowCount())
	require.Equal(t, 3, queue.Length())

	sels = nil
	for i := 0; i < 8190; i++ {
		sels = append(sels, int32(i))
	}
	_, err = queue.Union(mp, bat2, sels, nil)
	require.NoError(t, err)
	require.Equal(t, 8192+10+8182+5+8190, queue.RowCount())
	require.Equal(t, 4, queue.Length())

	bat1.Clean(mp)
	bat2.Clean(mp)
	queue.Clean(mp)
}

func TestBatchSetUnionLargeSels(t *testing.T) {
	mp := mpool.MustNewZero()
	queue := NewBatchSet(TestBatchMaxRow)

	// Test case 1: Union with selsLen > batchMaxRow when queue.Length() == 0
	bat1 := makeTestBatch(20000, mp) // Create a large batch
	var sels []int32
	for i := 0; i < 15000; i++ {
		sels = append(sels, int32(i))
	}
	_, err := queue.Union(mp, bat1, sels, nil)
	require.NoError(t, err)

	// Should create multiple batches since selsLen (15000) > batchMaxRow (8192)
	expectedBatches1 := (15000 + TestBatchMaxRow - 1) / TestBatchMaxRow // Ceiling division
	require.Equal(t, expectedBatches1, queue.Length())
	require.Equal(t, 15000, queue.RowCount())

	// Verify each batch doesn't exceed batchMaxRow
	for i := 0; i < queue.Length(); i++ {
		bat := queue.Get(i)
		require.NotNil(t, bat)
		require.LessOrEqual(t, bat.rowCount, TestBatchMaxRow)
		if i < queue.Length()-1 {
			// All batches except the last should be exactly batchMaxRow
			require.Equal(t, TestBatchMaxRow, bat.rowCount)
		}
	}

	bat1.Clean(mp)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// Test case 2: Union with selsLen == batchMaxRow when queue.Length() == 0
	queue = NewBatchSet(TestBatchMaxRow)
	bat2 := makeTestBatch(TestBatchMaxRow, mp)
	sels = nil
	for i := 0; i < TestBatchMaxRow; i++ {
		sels = append(sels, int32(i))
	}
	_, err = queue.Union(mp, bat2, sels, nil)
	require.NoError(t, err)
	require.Equal(t, 1, queue.Length())
	require.Equal(t, TestBatchMaxRow, queue.RowCount())
	require.Equal(t, TestBatchMaxRow, queue.Get(0).rowCount)

	bat2.Clean(mp)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// Test case 3: Union with selsLen > batchMaxRow when lastBat is already full
	queue = NewBatchSet(TestBatchMaxRow)
	bat3 := makeTestBatch(TestBatchMaxRow, mp)
	sels = nil
	for i := 0; i < TestBatchMaxRow; i++ {
		sels = append(sels, int32(i))
	}
	// Fill first batch to full
	_, err = queue.Union(mp, bat3, sels, nil)
	require.NoError(t, err)
	require.Equal(t, 1, queue.Length())
	require.Equal(t, TestBatchMaxRow, queue.Get(0).rowCount)

	// Now Union with large sels again
	bat4 := makeTestBatch(20000, mp)
	sels = nil
	for i := 0; i < 10000; i++ {
		sels = append(sels, int32(i))
	}
	_, err = queue.Union(mp, bat4, sels, nil)
	require.NoError(t, err)
	// Should create new batches since lastBat is full
	// First batch is full (8192), remaining 10000 sels should create 2 batches: 8192 + 1808
	expectedBatches3 := 1 + (10000+TestBatchMaxRow-1)/TestBatchMaxRow
	require.Equal(t, expectedBatches3, queue.Length())
	require.Equal(t, TestBatchMaxRow+10000, queue.RowCount())
	require.Equal(t, TestBatchMaxRow, queue.Get(0).rowCount)
	require.Equal(t, TestBatchMaxRow, queue.Get(1).rowCount)
	require.Equal(t, 10000-TestBatchMaxRow, queue.Get(2).rowCount) // 10000 - 8192 = 1808

	bat3.Clean(mp)
	bat4.Clean(mp)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// Test case 4: Union with selsLen > batchMaxRow when lastBat has some rows
	queue = NewBatchSet(TestBatchMaxRow)
	bat5 := makeTestBatch(10000, mp)
	sels = nil
	for i := 0; i < 5000; i++ {
		sels = append(sels, int32(i))
	}
	// First Union: creates batch with 5000 rows
	_, err = queue.Union(mp, bat5, sels, nil)
	require.NoError(t, err)
	require.Equal(t, 1, queue.Length())
	require.Equal(t, 5000, queue.Get(0).rowCount)

	// Second Union: with 10000 sels, should fill first batch to 8192, then create new batch
	sels = nil
	for i := 0; i < 10000; i++ {
		sels = append(sels, int32(i))
	}
	_, err = queue.Union(mp, bat5, sels, nil)
	require.NoError(t, err)
	// First batch should be filled to 8192 (5000 + 3192), second batch has remaining 6808
	require.Equal(t, 2, queue.Length())
	require.Equal(t, 5000+10000, queue.RowCount())
	require.Equal(t, TestBatchMaxRow, queue.Get(0).rowCount)
	require.Equal(t, 10000-3192, queue.Get(1).rowCount) // 10000 - (8192-5000) = 6808

	bat5.Clean(mp)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchSetExtendReuseBuf verifies that Extend consumes reuseBuf (consumed=true)
// in all three code paths: empty set, last-batch-full, and inBatch-full.
func TestBatchSetExtendReuseBuf(t *testing.T) {
	mp := mpool.MustNewZero()

	// Path 1: empty set — reuseBuf should be consumed as the new batch
	queue := NewBatchSet(TestBatchMaxRow)
	src := makeTestBatch(10, mp)
	reuse := makeTestBatch(0, mp)
	reuse.rowCount = 0
	consumed, err := queue.Extend(mp, src, reuse)
	require.NoError(t, err)
	require.True(t, consumed)
	require.Equal(t, 1, queue.Length())
	require.Equal(t, 10, queue.RowCount())
	src.Clean(mp)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// Path 2: last batch is full — reuseBuf consumed as new batch
	queue = NewBatchSet(TestBatchMaxRow)
	full := makeTestBatch(TestBatchMaxRow, mp)
	extra := makeTestBatch(5, mp)
	reuse = makeTestBatch(5, mp)
	_, _ = queue.Extend(mp, full, nil)
	full.Clean(mp)
	consumed, err = queue.Extend(mp, extra, reuse)
	require.NoError(t, err)
	require.True(t, consumed)
	require.Equal(t, 2, queue.Length())
	require.Equal(t, TestBatchMaxRow+5, queue.RowCount())
	extra.Clean(mp)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// Path 3: inBatch is full — reuseBuf consumed, last partial batch pushed back
	queue = NewBatchSet(TestBatchMaxRow)
	partial := makeTestBatch(3, mp)
	fullIn := makeTestBatch(TestBatchMaxRow, mp)
	reuse = makeTestBatch(3, mp)
	_, _ = queue.Extend(mp, partial, nil)
	partial.Clean(mp)
	consumed, err = queue.Extend(mp, fullIn, reuse)
	require.NoError(t, err)
	require.True(t, consumed)
	require.Equal(t, 2, queue.Length())
	require.Equal(t, TestBatchMaxRow+3, queue.RowCount())
	fullIn.Clean(mp)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchSetUnionReuseBuf verifies that Union consumes reuseBuf when provided.
func TestBatchSetUnionReuseBuf(t *testing.T) {
	mp := mpool.MustNewZero()

	// Empty set: reuseBuf consumed for the first new batch
	queue := NewBatchSet(TestBatchMaxRow)
	src := makeTestBatch(20, mp)
	reuse := makeTestBatch(5, mp)
	sels := []int32{0, 1, 2, 3, 4}
	consumed, err := queue.Union(mp, src, sels, reuse)
	require.NoError(t, err)
	require.True(t, consumed)
	require.Equal(t, 1, queue.Length())
	require.Equal(t, 5, queue.RowCount())
	src.Clean(mp)
	queue.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchSetTakeBatches verifies TakeBatches transfers ownership and leaves set empty.
func TestBatchSetTakeBatches(t *testing.T) {
	mp := mpool.MustNewZero()
	queue := NewBatchSet(TestBatchMaxRow)
	bat := makeTestBatch(10, mp)
	_ = queue.Push(mp, bat)
	require.Equal(t, 1, queue.Length())

	batches := queue.TakeBatches()
	require.Equal(t, 1, len(batches))
	require.Equal(t, 0, queue.Length())

	for _, b := range batches {
		b.Clean(mp)
	}
	require.Equal(t, int64(0), mp.CurrNB())
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
	vec := vector.NewVec(typ)
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
