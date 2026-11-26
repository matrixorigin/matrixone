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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCachingReadDoesNotJoinUncachedFlight(t *testing.T) {
	for _, tc := range []struct {
		name       string
		skipLookup bool
	}{
		{name: "baseline_direct_caching_read", skipLookup: true},
		{name: "caching_read_with_shared_lookup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const offset, size = 100, 8
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fs, storage, data := newPrefetchRangeFS(t, 0, 1024)
			<-fs.diskCache.load.done
			failedOpen := &failOnceStorage{ObjectStorage: storage, fail: offset, gate: make(chan struct{})}
			fs.server.raw = failedOpen
			release := sync.OnceFunc(func() { close(failedOpen.gate) })
			defer release()
			type result struct {
				data string
				err  error
			}
			start := func(policy Policy) <-chan result {
				done := make(chan result, 1)
				go func() {
					vector := &IOVector{FilePath: "object", Policy: policy | ExactColumnsRead | SkipMemoryCache,
						Entries: []IOEntry{{Offset: offset, Size: size}}}
					defer vector.Release()
					err := fs.Read(ctx, vector)
					done <- result{data: string(vector.Entries[0].Data), err: err}
				}()
				return done
			}

			path := fs.diskCache.objectKey("object")
			first := start(SkipDiskCacheWrites)
			require.Eventually(t, func() bool { return failedOpen.failed.Load() && memoryWaiters(fs, path) == 1 },
				time.Second, time.Millisecond, "the uncached flight must be waiting for its first storage open")

			var policy Policy
			if tc.skipLookup {
				policy = SkipDiskCacheReads
			}
			second := start(policy)
			require.Eventually(t, func() bool { return len(second) == 1 || memoryWaiters(fs, path) == 2 },
				time.Second, time.Millisecond, "the second read must either complete or join the stalled flight")
			release()
			require.ErrorContains(t, (<-first).err, "probe: storage failed")
			got := <-second
			require.NoError(t, got.err, "the caching read should open storage independently of the non-caching flight")
			require.Equal(t, string(data[offset:offset+size]), got.data)
			require.EqualValues(t, 1, storage.reads.Load(), "the independent storage open succeeds")
		})
	}
}

func TestAsyncDiskPrefetchSurvivesIndexDownload(t *testing.T) {
	for _, tc := range []struct {
		name    string
		overlap bool
	}{
		{name: "baseline_index_published_first"},
		{name: "disk_prefetch_during_index_download", overlap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				fs, _, data := newIndexObjectFS(t, true)
				fs.SetAsyncUpdate(true)
				storage := newBlackBoxStorage(fs.server.raw, func(string, int64) bool { return true })
				fs.server.raw = storage
				resume := sync.OnceFunc(func() { close(storage.gate) })
				defer resume()

				req := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
				indexCtx := WithFileServicePolicy(ctx, IndexFileObject)
				require.NoError(t, fs.PrefetchObject(indexCtx, req))
				synctest.Wait()
				current, _, attempts := storage.counts("object")
				require.Equal(t, 1, current, "the index download holds the object path")
				require.Equal(t, 1, attempts)
				if !tc.overlap {
					resume()
					waitReviewPrefetchIdle(t, fs)
					fs.diskCache.Flush(ctx)
				}

				require.NoError(t, fs.PrefetchObject(ctx, req))
				synctest.Wait()
				resume()
				waitReviewPrefetchIdle(t, fs)
				fs.diskCache.Flush(ctx)

				require.True(t, fs.indexObjectHeld("object"))
				_, _, attempts = storage.counts("object")
				require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))),
					"the disk cache stayed empty after %d storage open(s), despite a prefetch for each cache", attempts)
				require.Equal(t, 2, attempts)
			})
		})
	}
}
