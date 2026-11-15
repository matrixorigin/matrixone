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

func newSpoolSizeBatch(tb testing.TB, mp *mpool.MPool, sizes []int, alias bool) *batch.Batch {
	tb.Helper()
	source := batch.NewWithSize(len(sizes))
	tb.Cleanup(func() { source.Clean(mp) })
	for i, size := range sizes {
		if size == -2 {
			var err error
			source.Vecs[i], err = vector.NewConstBytes(types.T_text.ToType(), []byte("constant"), 1, mp)
			require.NoError(tb, err)
			continue
		}
		if size < 0 {
			continue
		}
		source.Vecs[i] = vector.NewOffHeapVecWithType(types.T_text.ToType())
		require.NoError(tb, vector.AppendBytes(source.Vecs[i], bytes.Repeat([]byte{byte(i + 1)}, size), false, mp))
	}
	if alias {
		source.Vecs = append(source.Vecs, source.Vecs[len(sizes)-1])
	}
	source.SetRowCount(1)
	return source
}

func TestCachedBatchCopiesMixedShapes(t *testing.T) {
	for _, test := range []struct {
		name                       string
		columns                    int
		largeBytes                 int
		largeFirst, alias          bool
		descriptorSlack, decorated bool
	}{
		{"small_first_exact", 2, 8192, false, false, false, false},
		{"small_first_spare_capacity", 2, 8191, false, false, false, false},
		{"large_first_control", 2, 8192, true, false, false, false},
		{"large_first_spare_capacity", 2, 8191, true, false, false, false},
		{"aliased_large_column", 2, 8192, false, true, false, false},
		{"wide_batch", 40, 8192, false, false, false, false},
		{"spare_descriptor_capacity", 2, 8192, false, false, true, false},
		{"nil_null_and_constant", 5, 8192, false, false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sourceMP := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { defer mpool.DeleteMPool(sourceMP); require.Zero(t, sourceMP.CurrNB()) })
			seedSizes, nextSizes := make([]int, test.columns), make([]int, test.columns)
			large, small := test.columns-1, 0
			if test.largeFirst {
				large, small = 0, test.columns-1
			}
			seedSizes[large], nextSizes[large], nextSizes[small] = 8192, test.largeBytes, 1024
			if test.decorated {
				seedSizes[1], nextSizes[1], seedSizes[2], nextSizes[2] = -2, -2, -1, -1
			}
			seed := newSpoolSizeBatch(t, sourceMP, seedSizes, test.alias)
			source := newSpoolSizeBatch(t, sourceMP, nextSizes, test.alias)
			if test.decorated {
				seed.Vecs[3].SetNull(0)
				source.Vecs[3].SetNull(0)
			}
			if test.descriptorSlack {
				require.NoError(t, source.Vecs[small].PreExtend(512, sourceMP))
			}
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
			cache := initCachedBatch(mp, 1)
			t.Cleanup(cache.free)
			copyAndReturn := func(src *batch.Batch, checkMemory bool) {
				copied, useCache, id, err := cache.GetCopiedBatch(src)
				require.NoError(t, err)
				defer cache.CacheBatch(useCache, id, copied)
				require.Equal(t, src.RowCount(), copied.RowCount())
				var required int64
				for i, vec := range src.Vecs {
					if vec == nil {
						require.Nil(t, copied.Vecs[i])
						continue
					}
					require.Equal(t, vec.GetBytesAt(0), copied.Vecs[i].GetBytesAt(0))
					require.Equal(t, vec.IsNull(0), copied.Vecs[i].IsNull(0))
					if test.alias && i == len(src.Vecs)-1 {
						require.Same(t, copied.Vecs[i-1], copied.Vecs[i])
					} else {
						required += int64(vec.Length()*vec.GetType().TypeSize() + cap(vec.GetArea()))
					}
				}
				if checkMemory {
					require.LessOrEqual(t, mp.CurrNB(), required,
						"small columns must not consume the only cached backing needed by a later large column")
				}
			}
			copyAndReturn(seed, false)
			copyAndReturn(source, true)
			copyAndReturn(source, true)
		})
	}
}

func TestCachedBatchMixedShapeFailureReturnsBacking(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(sourceMP); require.Zero(t, sourceMP.CurrNB()) })
	seed := newSpoolSizeBatch(t, sourceMP, []int{64, 0, 8192}, true)
	source := newSpoolSizeBatch(t, sourceMP, []int{64, 1024, 8192}, true)
	const limit = 1 << 20
	mp, err := mpool.NewMPool(t.Name(), limit, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	cache := initCachedBatch(mp, 1)
	t.Cleanup(cache.free)
	copied, useCache, id, err := cache.GetCopiedBatch(seed)
	require.NoError(t, err)
	cache.CacheBatch(useCache, id, copied)
	retained := mp.CurrNB()
	filler, err := mp.Alloc(limit-int(retained)-512, true)
	require.NoError(t, err)
	t.Cleanup(func() {
		if filler != nil {
			mp.Free(filler)
		}
	})
	for range 2 {
		copied, useCache, id, err = cache.GetCopiedBatch(source)
		if err == nil {
			cache.CacheBatch(useCache, id, copied)
		}
		require.Error(t, err)
		require.Len(t, cache.buffer.readyToUse, 1)
		require.LessOrEqual(t, mp.CurrNB(), int64(len(filler))+retained)
	}
	mp.Free(filler)
	filler = nil
	copied, useCache, id, err = cache.GetCopiedBatch(source)
	require.NoError(t, err)
	defer cache.CacheBatch(useCache, id, copied)
	for i, vec := range source.Vecs {
		require.Equal(t, vec.GetBytesAt(0), copied.Vecs[i].GetBytesAt(0))
	}
	require.Same(t, copied.Vecs[2], copied.Vecs[3])
}

func TestCachedBatchPrefersSmallestSufficientBacking(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(sourceMP); require.Zero(t, sourceMP.CurrNB()) })
	seed := newSpoolSizeBatch(t, sourceMP, []int{64, 128, 256}, false)
	source := newSpoolSizeBatch(t, sourceMP, []int{50, 100, 0}, false)
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	cache := initCachedBatch(mp, 1)
	t.Cleanup(cache.free)
	copied, use, id, err := cache.GetCopiedBatch(seed)
	require.NoError(t, err)
	small, medium := cap(copied.Vecs[0].GetArea()), cap(copied.Vecs[1].GetArea())
	cache.CacheBatch(use, id, copied)
	before := mp.CurrNB()
	copied, use, id, err = cache.GetCopiedBatch(source)
	require.NoError(t, err)
	defer cache.CacheBatch(use, id, copied)
	require.Equal(t, small, cap(copied.Vecs[0].GetArea()))
	require.Equal(t, medium, cap(copied.Vecs[1].GetArea()))
	require.Zero(t, cap(copied.Vecs[2].GetArea()))
	require.Equal(t, before, mp.CurrNB())
	for i, vec := range source.Vecs {
		require.Equal(t, vec.GetBytesAt(0), copied.Vecs[i].GetBytesAt(0))
	}
}

func TestCachedBatchPreservesAliasesAcrossReservation(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(sourceMP); require.Zero(t, sourceMP.CurrNB()) })
	seed := newSpoolSizeBatch(t, sourceMP, []int{64, 0, 8192, -2, -1}, false)
	source := newSpoolSizeBatch(t, sourceMP, []int{64, 1024, 8192, -2, -1}, false)
	width := len(seed.Vecs)
	for _, b := range []*batch.Batch{seed, source} {
		b.Vecs = append(b.Vecs, b.Vecs...)
	}
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	cache := initCachedBatch(mp, 1)
	t.Cleanup(cache.free)
	copyAndReturn := func(src *batch.Batch) {
		copied, use, id, err := cache.GetCopiedBatch(src)
		require.NoError(t, err)
		defer cache.CacheBatch(use, id, copied)
		require.Equal(t, src.RowCount(), copied.RowCount())
		for i, vec := range src.Vecs {
			if vec == nil {
				require.Nil(t, copied.Vecs[i])
				continue
			}
			require.Equal(t, vec.GetBytesAt(0), copied.Vecs[i].GetBytesAt(0))
			if i >= width {
				require.Same(t, copied.Vecs[i-width], copied.Vecs[i])
			}
		}
	}
	copyAndReturn(seed)
	copyAndReturn(source)
	copyAndReturn(source)
}
