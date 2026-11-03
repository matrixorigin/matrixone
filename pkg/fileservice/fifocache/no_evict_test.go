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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func TestSetWithoutEvictionPreservesResidentAndGhost(t *testing.T) {
	ctx := context.Background()
	var capacity atomic.Int64
	capacity.Store(128)
	cache := NewDataCache(capacity.Load, nil, nil, nil)
	a := fscache.CacheKey{Path: "a", Sz: 64}
	b := fscache.CacheKey{Path: "b", Sz: 64}
	for _, key := range []fscache.CacheKey{a, b} {
		inserted, err := cache.SetWithoutEviction(ctx, key, testBytes(make([]byte, 64)), nil)
		require.NoError(t, err)
		require.True(t, inserted)
	}
	_, err := cache.SetWithoutEviction(ctx, fscache.CacheKey{Path: "tail", Sz: 64}, testBytes(make([]byte, 64)), nil)
	require.ErrorIs(t, err, fscache.ErrCacheAdmissionRejected)
	require.Equal(t, int64(128), cache.Used())
	cache.ForceEvictWithWait(ctx, 64)
	_, exists := cache.Get(ctx, a)
	require.False(t, exists)
	capacity.Store(64)
	_, err = cache.SetWithoutEviction(ctx, a, testBytes(make([]byte, 64)), nil)
	require.ErrorIs(t, err, fscache.ErrCacheAdmissionRejected)
	_, exists = cache.Get(ctx, b)
	require.True(t, exists, "a ghost hit must not evict the remaining resident")
	require.Equal(t, int64(64), cache.Used())
	capacity.Store(128)
	inserted, err := cache.SetWithoutEviction(ctx, a, testBytes(make([]byte, 64)), nil)
	require.NoError(t, err)
	require.True(t, inserted)
	shard := &cache.fifo.shards[cache.fifo.keyShardFunc(a)%numShards]
	shard.Lock()
	queue := shard.values[a].queue
	shard.Unlock()
	require.Equal(t, uint8(cacheItemQueue2), queue, "ordinary ghost promotion is preserved")
}

func TestSetWithoutEvictionRejectsPublicationConditions(t *testing.T) {
	ctx := context.Background()
	cache := NewDataCache(fscache.ConstCapacity(128), nil, nil, nil)
	key := fscache.CacheKey{Path: "value", Sz: 64}
	for _, reason := range []string{"backing", "pressure", "cancel"} {
		t.Run(reason, func(t *testing.T) {
			callCtx := ctx
			var admit func() bool
			switch reason {
			case "backing":
				admit = func() bool { return false }
			case "pressure":
				cache.SetAdmissionTarget(func(capacity int64) (int64, bool) { return capacity, true })
				t.Cleanup(func() { cache.SetAdmissionTarget(nil) })
			case "cancel":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				callCtx = canceled
			}
			_, err := cache.SetWithoutEviction(callCtx, key, testBytes(make([]byte, 64)), admit)
			if reason == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, fscache.ErrCacheAdmissionRejected)
			}
			require.Zero(t, cache.Used())
			_, exists := cache.Get(ctx, key)
			require.False(t, exists)
		})
	}
}
