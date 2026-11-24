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
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexPrefetchWithDirectDiskDownload(t *testing.T) {
	for _, tc := range []struct {
		name    string
		overlap bool
	}{
		{name: "after_direct_disk_download"},
		{name: "during_direct_disk_download", overlap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				fs, storage, data := newIndexObjectFS(t, true)
				publishing := make(chan struct{})
				publish := make(chan struct{})
				release := sync.OnceFunc(func() { close(publish) })
				defer release()
				fs.diskCache.hooks.beforeFilePublication = sync.OnceFunc(func() {
					close(publishing)
					<-publish
				})

				diskPrefetch := make(chan error, 1)
				go func() { diskPrefetch <- fs.PrefetchFile(ctx, "object") }()
				<-publishing
				if !tc.overlap {
					release()
					require.NoError(t, <-diskPrefetch)
				}

				require.NoError(t, fs.PrefetchObject(WithFileServicePolicy(ctx, IndexFileObject), ObjectPrefetch{
					FilePath: "object", Size: int64(len(data)),
				}))
				if tc.overlap {
					// Let the index prefetch reach its wait or complete before publishing
					// the other tier. A worker that defers the fill can then proceed.
					synctest.Wait()
					release()
					require.NoError(t, <-diskPrefetch)
				}
				waitPrefetcherIdle(t, fs)

				require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
				require.True(t, fs.indexObjectHeld("object"),
					"index prefetch must warm its own tier after the direct disk fill (backend reads: %d)", storage.reads.Load())
				require.EqualValues(t, 2, storage.reads.Load())
			})
		})
	}
}

func TestIndexRangePrefetchHonorsCacheWritePolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ranged bool
		policy Policy
	}{
		{name: "whole_prefetch_skips_all_cache_writes", policy: SkipCacheWrites},
		{name: "whole_prefetch_with_default_writes"},
		{name: "range_prefetch_skips_all_cache_writes", ranged: true, policy: SkipCacheWrites},
		{name: "range_prefetch_with_default_writes", ranged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, storage, data := newIndexObjectFS(t, true)
			ctx := WithFileServicePolicy(context.Background(), IndexFileObject|tc.policy)
			req := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
			if tc.ranged {
				req.Size = 4096
				req.Ranges = []PrefetchRange{{Offset: 64 << 10, Size: req.Size}}
			}
			require.NoError(t, fs.PrefetchObject(ctx, req))
			waitPrefetcherIdle(t, fs)
			require.Zero(t, fs.diskCache.cache.Used(),
				"index-object bytes must stay out of the table-data tier (backend reads: %d)", storage.reads.Load())
			if tc.policy.Any(SkipCacheWrites) {
				require.False(t, fs.indexObjectHeld("object"), "skipping all cache writes must leave both tiers empty")
			} else if !tc.ranged {
				require.True(t, fs.indexObjectHeld("object"), "a whole prefetch with writes enabled warms the index tier")
			}
		})
	}
}

func TestIndexObjectIdleTTLStartsAfterReadReleasesHold(t *testing.T) {
	for _, tc := range []struct {
		name     string
		readTime time.Duration
	}{
		{name: "read_finishes_before_idle_ttl"},
		{name: "read_outlasts_idle_ttl", readTime: 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fs, storage, data := newIndexObjectFS(t, true)
				cacheIndexObject(t, fs, "object", data)
				cache := fs.indexFileCache
				cache.idleTTL = 2 * time.Second
				ctx := context.Background()
				var received bytes.Buffer
				vec := &IOVector{
					FilePath: "object", Policy: IndexFileObject | SkipMemoryCache,
					Entries: []IOEntry{{
						Offset: 100, Size: 64, Data: make([]byte, 64),
						WriterForRead: readShareWriterFunc(func(data []byte) (int, error) {
							// Advance virtual time while the cached entry is held by the read.
							time.Sleep(tc.readTime)
							return received.Write(data)
						}),
					}},
				}
				defer vec.Release()
				require.NoError(t, fs.Read(ctx, vec))
				require.Equal(t, data[100:164], received.Bytes())
				require.Zero(t, storage.reads.Load())
				removed, err := cache.collectIdle(ctx, time.Now())
				require.NoError(t, err)
				require.Zero(t, removed, "an index object just released by a successful read is not idle")
			})
		})
	}
}

func TestExactReadDoesNotWidenRange(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy Policy
	}{
		{name: "exact_columns_read", policy: ExactColumnsRead},
		{name: "exact_read_on_its_own", policy: ExactRead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, storage, data := newIndexObjectFS(t, false)
			var offset atomic.Int64
			tracked := newBlackBoxStorage(storage, func(_ string, from int64) bool {
				offset.Store(from)
				return false
			})
			fs.storage = tracked
			require.Equal(t, data[100:164], readIndexObjectRange(t, fs, tc.policy, 100, 64))
			require.EqualValues(t, 1, storage.reads.Load())
			require.EqualValues(t, 100, offset.Load(), "an exact read opens storage at the requested offset")
		})
	}
}

func TestIndexReadKeepsDownloadTierAcrossEntries(t *testing.T) {
	for _, tc := range []struct {
		name              string
		startDiskPrefetch bool
	}{
		{name: "index_publication_only"},
		{name: "table_download_replaces_index_download", startDiskPrefetch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				fs, _, data := newIndexObjectFS(t, true)
				indexCtx := WithFileServicePolicy(ctx, IndexFileObject)

				indexPublishing, allowIndexPublish := make(chan struct{}), make(chan struct{})
				releaseIndex := sync.OnceFunc(func() { close(allowIndexPublish) })
				defer releaseIndex()
				fs.diskCache.hooks.beforeFilePublication = sync.OnceFunc(func() {
					close(indexPublishing)
					<-allowIndexPublish
				})
				diskFailure := errors.New("table download source failed")
				probe := &probeStorage{
					ObjectStorage: fs.storage, at: 0, gate: make(chan struct{}),
					answer: func(r io.ReadCloser) (io.ReadCloser, error) {
						_ = r.Close()
						return nil, diskFailure
					},
				}
				probe.hit.Store(true) // Leave the initial index download unchanged.
				fs.storage = probe
				releaseDisk := sync.OnceFunc(func() { close(probe.gate) })
				defer releaseDisk()

				indexPrefetch := make(chan error, 1)
				go func() { indexPrefetch <- fs.PrefetchFile(indexCtx, "object") }()
				<-indexPublishing

				firstEntry, resumeRead := make(chan struct{}), make(chan struct{})
				releaseRead := sync.OnceFunc(func() { close(resumeRead) })
				defer releaseRead()
				var received bytes.Buffer
				vec := &IOVector{
					FilePath: "object", Policy: IndexFileObject | SkipDiskCacheReads | SkipMemoryCache,
					Entries: []IOEntry{
						{Offset: 0, Size: 64, Data: make([]byte, 64), WriterForRead: readShareWriterFunc(func(b []byte) (int, error) {
							close(firstEntry)
							<-resumeRead
							return received.Write(b)
						})},
						{Offset: 64, Size: 64, Data: make([]byte, 64)},
					},
				}
				result := make(chan error, 1)
				go func() { result <- fs.Read(ctx, vec) }()
				<-firstEntry
				releaseIndex()
				require.NoError(t, <-indexPrefetch)
				require.True(t, fs.indexObjectHeld("object"))

				diskPrefetch := make(chan error, 1)
				if tc.startDiskPrefetch {
					probe.hit.Store(false)
					go func() { diskPrefetch <- fs.PrefetchFile(ctx, "object") }()
					synctest.Wait()
					require.True(t, probe.hit.Load(), "the replacement table-data download reached storage")
				}
				releaseRead()
				synctest.Wait()
				if tc.startDiskPrefetch {
					releaseDisk()
					require.ErrorIs(t, <-diskPrefetch, diskFailure)
				}
				readErr := <-result
				defer vec.Release()
				require.NoError(t, readErr, "a read skipping the table-data tier should use the published index entry")
				require.Equal(t, data[:64], received.Bytes())
				require.Equal(t, data[64:128], vec.Entries[1].Data)
			})
		})
	}
}

func TestDeferredPrefetchStaysJoinableDuringPromotion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		overlap bool
	}{
		{name: "after_deferred_request_is_requeued"},
		{name: "while_deferred_request_is_requeued", overlap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				firstStarted, finishFirst := make(chan struct{}), make(chan struct{})
				checkingCache, resumePromotion := make(chan struct{}), make(chan struct{})
				deferredStarted, finishDeferred := make(chan struct{}), make(chan struct{})
				releaseFirst := sync.OnceFunc(func() { close(finishFirst) })
				releasePromotion := sync.OnceFunc(func() { close(resumePromotion) })
				var promoting atomic.Bool
				wantDeferred := func(req ObjectPrefetch) bool { return req.Ranges[0].Offset == 16 }
				p := newPrefetcher(1, func() int64 { return 2 }, func(req ObjectPrefetch) bool {
					if wantDeferred(req) && promoting.Load() {
						close(checkingCache)
						<-resumePromotion
					}
					return false
				}, func(_ context.Context, req ObjectPrefetch) {
					if wantDeferred(req) {
						close(deferredStarted)
						<-finishDeferred
					} else {
						close(firstStarted)
						<-finishFirst
					}
				})
				defer p.close(ctx)
				defer releaseFirst()
				defer releasePromotion()
				defer close(finishDeferred)

				p.add(ObjectPrefetch{FilePath: "object", Size: 1, Ranges: []PrefetchRange{{Size: 1}}})
				<-firstStarted
				p.add(ObjectPrefetch{FilePath: "object", Size: 1, Ranges: []PrefetchRange{{Offset: 16, Size: 1}}})
				promoting.Store(true)
				releaseFirst()
				if tc.overlap {
					select {
					case <-checkingCache:
					case <-deferredStarted:
					}
				} else {
					releasePromotion()
					<-deferredStarted
				}

				_, leave, joined := p.join("object", wantDeferred)
				if leave != nil {
					defer leave()
				}
				require.True(t, joined, "a reader should find the already-admitted deferred request")
			})
		})
	}
}
