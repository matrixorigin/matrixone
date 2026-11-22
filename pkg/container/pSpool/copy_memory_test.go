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
