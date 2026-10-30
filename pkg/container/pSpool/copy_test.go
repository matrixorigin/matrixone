// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pSpool

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func TestOneBatchMemoryCacheSelection(t *testing.T) {
	for _, test := range []struct {
		name               string
		buffers            []int
		dataSize, areaSize int
		dataCap, areaCap   int
		remaining          int
	}{
		{"data_first_exact", []int{64, 128, 256}, 128, 64, 128, 64, 1},
		{"area_first_exact", []int{64, 128, 256}, 64, 128, 64, 128, 1},
		{"data_only_exact", []int{64, 128}, 64, 0, 64, 0, 1},
		{"area_only_exact", []int{64, 128}, 0, 64, 0, 64, 1},
		{"equal_requests", []int{64, 64, 128}, 64, 64, 64, 64, 1},
		{"larger_best_fit", []int{64, 128, 256}, 100, 200, 128, 256, 1},
		{"undersized_fallback", []int{64, 128}, 256, 0, 128, 0, 1},
		{"zero_requests", []int{64, 128}, 0, 0, 0, 0, 2},
		{"empty_cache", nil, 64, 128, 0, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
			})
			cache := oneBatchMemoryCache{}
			t.Cleanup(func() {
				for i := range cache.buffers {
					cache.buffers[i].Free(mp)
				}
			})
			for _, size := range test.buffers {
				func() {
					owner := vector.NewOffHeapVecWithType(types.T_int8.ToType())
					defer owner.Free(mp)
					require.NoError(t, owner.PreExtend(size, mp))
					cache.buffers = append(cache.buffers, vector.DetachVectorData(owner))
				}()
			}
			vec := vector.NewOffHeapVecWithType(types.T_text.ToType())
			t.Cleanup(func() { vec.Free(mp) })
			require.NoError(t, cache.setSuitableDataAreaToVector(test.dataSize, test.areaSize, vec))
			require.Equal(t, test.dataCap, cap(vec.GetData()))
			require.Equal(t, test.areaCap, cap(vec.GetArea()))
			require.Len(t, cache.buffers, test.remaining)
		})
	}
}

func TestCachedBatchStableShapeRetention(t *testing.T) {
	const rows = 64
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	source := newWideSpoolSource(t, mp, rows)
	sourceBytes := mp.CurrNB()
	cache := initCachedBatch(mp, 1)
	t.Cleanup(cache.free)

	retained := make([]int64, 0, 6)
	for iteration := range 6 {
		copied, useCache, cacheID, err := cache.GetCopiedBatch(source)
		require.NoError(t, err)
		func() {
			defer cache.CacheBatch(useCache, cacheID, copied)
			require.Equal(t, rows, copied.RowCount())
			require.Equal(t, vector.MustFixedColNoTypeCheck[int64](source.Vecs[0]),
				vector.MustFixedColNoTypeCheck[int64](copied.Vecs[0]))
			for column := 1; column < len(source.Vecs); column++ {
				for row := range rows {
					require.Equal(t, source.Vecs[column].GetBytesAt(row), copied.Vecs[column].GetBytesAt(row))
				}
			}
		}()
		retained = append(retained, mp.CurrNB()-sourceBytes)
		t.Logf("iteration=%d retained=%d buffers=%d", iteration+1, retained[iteration], len(cache.buffer.bytesCache[cacheID].buffers))
	}
	for _, size := range retained[1:] {
		require.Equal(t, retained[0], size, "identical batch shapes must reuse their warmed backing capacity")
	}
}

func TestCachedBatchRetainsPrivateReaderPayload(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	original := newWideSpoolSource(t, mp, 4)
	producer := batch.NewOffHeapWithSize(3)
	t.Cleanup(func() { producer.Clean(mp) })
	var err error
	producer.Vecs[0], err = original.Vecs[0].DupOffHeap(mp)
	require.NoError(t, err)
	payloadBytes := 0
	for column := 1; column < 3; column++ {
		producer.Vecs[column] = vector.NewOffHeapVecWithType(*original.Vecs[column].GetType())
		require.NoError(t, producer.Vecs[column].CopyWithReadonlyArea(original.Vecs[column], mp))
		payloadBytes += len(producer.Vecs[column].GetArea())
	}
	producer.SetRowCount(original.RowCount())
	expected := bytes.Clone(original.Vecs[2].GetBytesAt(0))
	cache := initCachedBatch(mp, 2)
	t.Cleanup(cache.free)
	before := mp.CurrNB()
	first, useFirst, firstID, err := cache.GetCopiedBatch(producer)
	require.NoError(t, err)
	defer cache.CacheBatch(useFirst, firstID, first)
	second, useSecond, secondID, err := cache.GetCopiedBatch(producer)
	require.NoError(t, err)
	defer cache.CacheBatch(useSecond, secondID, second)
	require.True(t, useFirst)
	require.True(t, useSecond)
	require.Less(t, mp.CurrNB()-before, int64(payloadBytes), "two queued snapshots must not allocate another payload")
	for column := 1; column < 3; column++ {
		require.Same(t, &producer.Vecs[column].GetArea()[0], &first.Vecs[column].GetArea()[0])
		require.Same(t, &producer.Vecs[column].GetArea()[0], &second.Vecs[column].GetArea()[0])
		require.NotSame(t, &producer.Vecs[column].GetData()[0], &first.Vecs[column].GetData()[0])
	}
	producer.Clean(mp)
	original.Clean(mp)
	require.Equal(t, expected, first.Vecs[2].GetBytesAt(0))
	require.Equal(t, expected, second.Vecs[2].GetBytesAt(0))
	require.NoError(t, second.Vecs[2].MaterializeOwned(mp))
	second.Vecs[2].GetBytesAt(0)[0] ^= 1
	require.Equal(t, expected, first.Vecs[2].GetBytesAt(0))
}

func newWideSpoolSource(tb testing.TB, mp *mpool.MPool, rows int) *batch.Batch {
	tb.Helper()
	source := batch.NewOffHeapWithSize(3)
	tb.Cleanup(func() { source.Clean(mp) })
	source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(tb, vector.AppendFixedList(source.Vecs[0], make([]int64, rows), nil, mp))
	for i, size := range []int{3072, 16384} {
		typ := types.T_text.ToType()
		if i == 0 {
			typ = types.T_array_float32.ToType()
			typ.Width = 768
		}
		vec := vector.NewOffHeapVecWithType(typ)
		source.Vecs[i+1] = vec
		require.NoError(tb, vec.PreExtendWithArea(rows, rows*size, mp))
		payload := bytes.Repeat([]byte{byte(i + 1)}, size)
		for range rows {
			require.NoError(tb, vector.AppendBytes(vec, payload, false, mp))
		}
	}
	source.SetRowCount(rows)
	return source
}
