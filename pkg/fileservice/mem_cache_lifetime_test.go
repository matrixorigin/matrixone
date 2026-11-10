// Copyright 2026 Matrix Origin
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

package fileservice

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

// Concurrent updates, reads, deletes and evictions of a MemCache over its own allocator's
// buffers never let a reader see a buffer freed or reused for another key: each key's bytes
// carry its own pattern, checked on every hit. A retain after the last release or a second
// release panics in Bytes. Once flushed, the cache holds and reserves nothing.
func TestMemCacheConcurrentBufferLifetime(t *testing.T) {
	for _, notify := range []bool{false, true} {
		t.Run(fmt.Sprintf("callbacks=%v", notify), func(t *testing.T) {
			ctx := context.Background()
			var callbacks *CacheCallbacks
			if notify {
				noop := func(fscache.CacheKey, fscache.Data) {}
				callbacks = &CacheCallbacks{PostSet: []CacheCallbackFunc{noop}, PostEvict: []CacheCallbackFunc{noop}, PostGet: []CacheCallbackFunc{noop}}
			}
			cache := NewMemCache(fscache.ConstCapacity(256<<10), callbacks, nil, "")
			defer cache.Close(ctx)

			const paths, keys = 8, 64
			size := func(k int) int { return 512 + k*37 }
			pattern := func(p, k int) byte { return byte(p*keys + k + 1) }
			var hits atomic.Int64
			var wg sync.WaitGroup
			errs := make(chan error, 1)
			report := func(err error) {
				select {
				case errs <- err:
				default:
				}
			}
			for g := 0; g < 16; g++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					r := rand.New(rand.NewSource(seed))
					for i := 0; i < 4000; i++ {
						p, k := r.Intn(paths), r.Intn(keys)
						path := fmt.Sprintf("p%d", p)
						switch op := r.Intn(20); {
						case op < 8:
							data := cache.AllocateCacheData(ctx, size(k))
							b := data.Bytes()
							for j := range b {
								b[j] = pattern(p, k)
							}
							vec := &IOVector{FilePath: path, Entries: []IOEntry{{Offset: int64(k), Size: int64(size(k)), CachedData: data}}}
							if err := cache.Update(ctx, vec, false); err != nil {
								report(err)
								return
							}
							vec.Release()
						case op < 18:
							vec := &IOVector{FilePath: path, Entries: []IOEntry{{Offset: int64(k), Size: int64(size(k))}}}
							if err := cache.Read(ctx, vec); err != nil {
								report(err)
								return
							}
							if vec.Entries[0].done {
								hits.Add(1)
								b := vec.Entries[0].CachedData.Bytes()
								if len(b) != size(k) {
									report(fmt.Errorf("%s/%d: %d bytes, want %d", path, k, len(b), size(k)))
								}
								for j := range b {
									if b[j] != pattern(p, k) {
										report(fmt.Errorf("%s/%d: byte %d is %d, want %d", path, k, j, b[j], pattern(p, k)))
										break
									}
								}
							}
							vec.Release()
						case op == 18:
							if err := cache.DeletePaths(ctx, []string{path}); err != nil {
								report(err)
								return
							}
						default:
							cache.EvictToTarget(ctx, int64(r.Intn(256<<10)))
						}
					}
				}(int64(g))
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			require.Greater(t, hits.Load(), int64(1000), "reads hit cached buffers")
			cache.Flush(ctx)
			require.Zero(t, cache.cache.Used())
			require.Zero(t, cache.reservedBytes.Load(), "every reservation was committed or released")
		})
	}
}

// Only data that still holds a reservation is charged under the reservation lock: a Set of
// other data does not wait for it.
func TestMemCacheSetTakesReservationLockOnlyForReservedData(t *testing.T) {
	ctx := context.Background()
	cache := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	defer cache.Close(ctx)
	cache.capacityMu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := cache.cache.Set(ctx, fscache.CacheKey{Path: "plain", Sz: 3}, NewBytes([]byte("abc")))
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a Set of data without a reservation waited for the reservation lock")
	}
	cache.capacityMu.Unlock()

	reserved := cache.AllocateCacheData(ctx, 3)
	require.True(t, reserved.(*Bytes).CacheReservationPending())
	_, err := cache.cache.Set(ctx, fscache.CacheKey{Path: "reserved", Sz: 3}, reserved)
	require.NoError(t, err)
	require.False(t, reserved.(*Bytes).CacheReservationPending(), "caching commits the reservation")
	reserved.Release()
}
