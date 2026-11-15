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

func copySpoolBatchAtPeak(t *testing.T, cache *cachedBatch, source *batch.Batch) (copied *batch.Batch, peak uint64) {
	t.Helper()
	epoch := cache.mp.StartResourcePeakEpoch()
	require.NotNil(t, epoch)
	defer func() {
		var ok bool
		peak, ok = cache.mp.EndResourcePeakEpoch(epoch)
		require.True(t, ok)
	}()
	copied, use, id, err := cache.GetCopiedBatch(source)
	if err == nil {
		t.Cleanup(func() { cache.CacheBatch(use, id, copied) })
	}
	require.NoError(t, err)
	return copied, 0
}

func TestCachedBatchGrowthReleasesEarlierBacking(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(sourceMP); require.Zero(t, sourceMP.CurrNB()) })
	makeSource := func(sizes ...int) *batch.Batch {
		source := batch.NewWithSize(len(sizes))
		t.Cleanup(func() { source.Clean(sourceMP) })
		for i, size := range sizes {
			source.Vecs[i] = vector.NewOffHeapVecWithType(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(source.Vecs[i], bytes.Repeat([]byte{byte(i + 1)}, size), false, sourceMP))
		}
		source.SetRowCount(1)
		return source
	}
	seed, source := makeSource(1024, 0), makeSource(2048, 4096)
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	cache := initCachedBatch(mp, 1)
	t.Cleanup(cache.free)
	copied, use, id, err := cache.GetCopiedBatch(seed)
	require.NoError(t, err)
	cache.CacheBatch(use, id, copied)
	copied, peak := copySpoolBatchAtPeak(t, cache, source)
	for i, vec := range source.Vecs {
		require.Equal(t, vec.GetBytesAt(0), copied.Vecs[i].GetBytesAt(0))
	}
	// Giving the old 1KiB area to the first column releases it during its
	// 2KiB growth. Holding it until the later 4KiB copy adds needless overlap.
	require.LessOrEqual(t, peak, uint64(mp.CurrNB()), "earlier growth must release old backing before later allocations")
}

func TestCachedBatchCreatesBorrowedSnapshotInCopyOrder(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(sourceMP); require.Zero(t, sourceMP.CurrNB()) })
	const rows = 512
	fill := func(vec *vector.Vector, size int) {
		require.NoError(t, vec.PreExtendWithArea(rows, size, sourceMP))
		value := bytes.Repeat([]byte("x"), size)
		for row := range rows {
			data := value
			if row != 0 {
				data = nil
			}
			require.NoError(t, vector.AppendBytes(vec, data, false, sourceMP))
		}
	}
	original := vector.NewOffHeapVecWithType(types.T_text.ToType())
	t.Cleanup(func() { original.Free(sourceMP) })
	fill(original, 128)
	private := vector.NewOffHeapVecWithType(types.T_text.ToType())
	t.Cleanup(func() { private.Free(sourceMP) })
	require.NoError(t, private.CopyWithReadonlyArea(original, sourceMP))
	makeSource := func(size int) *batch.Batch {
		source := batch.NewWithSize(2)
		t.Cleanup(func() { source.Clean(sourceMP) })
		source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_text.ToType())
		fill(source.Vecs[0], size)
		var err error
		source.Vecs[1], err = private.RetainedReadonlyViewWithMP(sourceMP)
		require.NoError(t, err)
		source.SetRowCount(rows)
		return source
	}
	seed, source := makeSource(8192), makeSource(13568)
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	cache := initCachedBatch(mp, 1)
	t.Cleanup(cache.free)
	copied, use, id, err := cache.GetCopiedBatch(seed)
	require.NoError(t, err)
	cache.CacheBatch(use, id, copied)
	copied, peak := copySpoolBatchAtPeak(t, cache, source)
	for i, vec := range source.Vecs {
		for row := range rows {
			require.Equal(t, vec.GetBytesAt(row), copied.Vecs[i].GetBytesAt(row))
		}
	}
	// The ordinary column must finish growing and releasing its old area
	// before the later borrowed column allocates its descriptor snapshot.
	require.LessOrEqual(t, peak, uint64(mp.CurrNB()), "borrowed descriptors must not overlap an earlier column's old and new areas")
}
