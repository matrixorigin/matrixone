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

package fifocache

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func TestReuseHistoryBoundsAndDeletion(t *testing.T) {
	capacity := int64(4096)
	metadata := int64(600)
	cache := NewDataCache(func() int64 { return capacity }, nil, nil, nil)
	cache.SetReuseHistoryCapacity(func() int64 { return metadata })
	first := fscache.CacheKey{Path: "a", Sz: 16}
	second := fscache.CacheKey{Path: "b", Sz: 16}
	third := fscache.CacheKey{Path: "c", Sz: 16}
	require.False(t, cache.ObserveReuse(first, 1024))
	require.True(t, cache.ObserveReuse(first, 1024))
	require.Zero(t, cache.Used(), "history must retain no payload")
	require.False(t, cache.ObserveReuse(second, 1024))
	require.False(t, cache.ObserveReuse(third, 1024))
	require.LessOrEqual(t, cache.fifo.observationMetadataUsed, metadata)
	require.False(t, cache.ObserveReuse(first, 1024), "metadata bound forgets oldest")
	cache.DeletePaths(context.Background(), []string{"a"})
	require.False(t, cache.ObserveReuse(first, 1024))
	capacity = 512
	metadata = 256
	require.False(t, cache.ObserveReuse(third, 1024))
	require.LessOrEqual(t, cache.fifo.observationBytes, capacity-capacity/10)
	cache.ClearReuseHistory()
	require.Zero(t, cache.fifo.observationMetadataUsed)
	require.Zero(t, cache.fifo.observationBytes)
}

func TestReuseHistoryConcurrentObservation(t *testing.T) {
	cache := NewDataCache(fscache.ConstCapacity(1<<20), nil, nil, nil)
	cache.SetReuseHistoryCapacity(fscache.ConstCapacity(1024))
	key := fscache.CacheKey{Path: "shared", Sz: 16}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 32 {
				cache.ObserveReuse(key, 4096)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(4096), cache.fifo.observationBytes)
	require.Equal(t, int64(262), cache.fifo.observationMetadataUsed)
	cache.ClearReuseHistory()
}

func TestObservationQuotaPreservesReplacementGhosts(t *testing.T) {
	ctx := context.Background()
	cache := NewDataCache(fscache.ConstCapacity(4096), nil, nil, nil)
	cache.SetReuseHistoryCapacity(fscache.ConstCapacity(600))
	hot := fscache.CacheKey{Path: "hot", Sz: 1024}
	_, err := cache.Set(ctx, hot, testBytes(make([]byte, 1024)))
	require.NoError(t, err)
	cache.EvictToTargetWithWait(ctx, 0)
	require.Equal(t, int64(1024), cache.fifo.ghostSize)
	for i := range int64(16) {
		cache.ObserveReuse(fscache.CacheKey{Path: "stream", Offset: i, Sz: 16}, 256)
	}
	require.Equal(t, int64(1024), cache.fifo.ghostSize)
	require.True(t, cache.ObserveReuse(hot, 1024), "observation pressure must not forget an ordinary replacement ghost")
	observed := fscache.CacheKey{Path: "promoted", Sz: 16}
	require.False(t, cache.ObserveReuse(observed, 256))
	_, err = cache.Set(ctx, observed, testBytes(make([]byte, 256)))
	require.NoError(t, err)
	for i := range int64(16) {
		cache.ObserveReuse(fscache.CacheKey{Path: "stream", Offset: i + 100, Sz: 16}, 256)
	}
	data, ok := cache.Get(ctx, observed)
	require.True(t, ok, "trimming stale observation must preserve the promoted payload")
	data.Release()
}

func TestReuseHistoryConcurrentPublication(t *testing.T) {
	ctx := context.Background()
	cache := NewDataCache(fscache.ConstCapacity(1<<20), nil, nil, nil)
	cache.SetReuseHistoryCapacity(fscache.ConstCapacity(4096))
	cache.SetAdmissionTarget(func(capacity int64) (int64, bool) { return capacity / 2, true })
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 64 {
				key := fscache.CacheKey{Path: "shared", Offset: int64((worker + i) % 16), Sz: 128}
				cache.ObserveReuse(key, 128)
				if _, err := cache.Set(ctx, key, testBytes(make([]byte, 128))); err != nil {
					errors <- err
					return
				}
				if i%3 == 0 {
					cache.DeletePaths(ctx, []string{"shared"})
				}
				if i%4 == 0 {
					cache.EvictToTargetWithWait(ctx, 0)
				}
				if i%5 == 0 {
					cache.ClearReuseHistory()
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	cache.EvictToTargetWithWait(ctx, 0)
	cache.ClearReuseHistory()
	require.Zero(t, cache.fifo.observationMetadataUsed)
	require.Zero(t, cache.fifo.observationBytes)
	require.Zero(t, cache.fifo.ghostSize)
}
