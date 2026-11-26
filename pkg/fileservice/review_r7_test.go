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

func waitReviewPrefetchIdle(t *testing.T, fs *S3FS) {
	t.Helper()
	waitFillsIdle(t, fs.diskCache)
}

func TestPrefetchDiskTierAfterIndex(t *testing.T) {
	for _, tc := range []struct {
		name    string
		overlap bool
	}{
		{name: "after_index_download"},
		{name: "during_index_download", overlap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, _, data := newIndexObjectFS(t, true)
			req := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
			storage := newBlackBoxStorage(fs.server.raw, func(string, int64) bool { return true })
			fs.server.raw = storage
			resume := sync.OnceFunc(func() { close(storage.gate) })
			defer resume()

			require.NoError(t, fs.PrefetchObject(WithFileServicePolicy(ctx, IndexFileObject), req))
			require.Eventually(t, func() bool {
				current, _, _ := storage.counts(fs.pathToKey(req.FilePath))
				return current == 1
			}, 5*time.Second, time.Millisecond)
			if !tc.overlap {
				resume()
				waitReviewPrefetchIdle(t, fs)
			}
			require.NoError(t, fs.PrefetchObject(ctx, req))
			resume()
			waitReviewPrefetchIdle(t, fs)

			require.True(t, fs.indexObjectHeld(req.FilePath), "the index prefetch should fill the index-file cache")
			_, _, reads := storage.counts(fs.pathToKey(req.FilePath))
			require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath(req.FilePath, int64(len(data)))),
				"the disk prefetch should fill the disk cache (storage opens: %d)", reads)
			require.Equal(t, 2, reads)
		})
	}
}

func TestPrefetchBothQueuedTiers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		overlap bool
	}{
		{name: "separate_queue_entries"},
		{name: "same_object_queued_in_both_tiers", overlap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, storage, data := newIndexObjectFS(t, true)
			resume := busyFills(t, fs)

			req := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
			indexReq := req
			if !tc.overlap {
				indexReq.FilePath = "another-object"
				require.NoError(t, fs.Write(ctx, IOVector{FilePath: indexReq.FilePath, Policy: SkipAllCache,
					Entries: []IOEntry{{Size: req.Size, Data: data}}}))
			}
			require.NoError(t, fs.PrefetchObject(ctx, req))
			require.True(t, fs.diskCache.isQueued(req.FilePath))
			require.NoError(t, fs.PrefetchObject(WithFileServicePolicy(ctx, IndexFileObject), indexReq))
			require.True(t, fs.diskCache.isQueued(indexReq.FilePath))
			resume()
			waitReviewPrefetchIdle(t, fs)

			require.True(t, fs.indexObjectHeld(indexReq.FilePath), "the index prefetch should fill the index-file cache")
			require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath(req.FilePath, int64(len(data)))),
				"the disk prefetch should fill the disk cache (storage opens: %d)", storage.reads.Load())
			require.EqualValues(t, 2, storage.reads.Load())
		})
	}
}

func TestIndexPrefetchAfterAsyncDiskDownload(t *testing.T) {
	for _, tc := range []struct {
		name    string
		overlap bool
	}{
		{name: "after_disk_publication"},
		{name: "while_disk_publication_is_pending", overlap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				fs, _, data := newIndexObjectFS(t, true)
				fs.SetAsyncUpdate(true)
				storage := newBlackBoxStorage(fs.server.raw, func(string, int64) bool { return true })
				fs.server.raw = storage
				resumeStorage := sync.OnceFunc(func() { close(storage.gate) })
				defer resumeStorage()
				publicationEntered, publish := make(chan struct{}), make(chan struct{})
				resumePublication := sync.OnceFunc(func() { close(publish) })
				defer resumePublication()
				fs.diskCache.hooks.beforeFilePublication = sync.OnceFunc(func() {
					close(publicationEntered)
					<-publish
				})

				req := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
				indexCtx := WithFileServicePolicy(ctx, IndexFileObject)
				require.NoError(t, fs.PrefetchObject(ctx, req))
				require.Eventually(t, func() bool {
					current, _, _ := storage.counts(fs.pathToKey(req.FilePath))
					return current == 1
				}, 5*time.Second, time.Millisecond)
				if tc.overlap {
					require.NoError(t, fs.PrefetchObject(indexCtx, req))
				}
				resumeStorage()
				select {
				case <-publicationEntered:
				case <-time.After(5 * time.Second):
					t.Fatal("the disk cache did not reach publication")
				}
				// Let the deferred prefetch reach its wait or complete before resuming publication.
				synctest.Wait()
				resumePublication()
				waitReviewPrefetchIdle(t, fs)
				fs.diskCache.Flush(ctx)
				require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath(req.FilePath, int64(len(data)))))
				if !tc.overlap {
					require.NoError(t, fs.PrefetchObject(indexCtx, req))
					waitReviewPrefetchIdle(t, fs)
					fs.diskCache.Flush(ctx)
				}

				_, _, reads := storage.counts(fs.pathToKey(req.FilePath))
				require.True(t, fs.indexObjectHeld(req.FilePath),
					"the index prefetch should survive the disk cache's asynchronous publication (storage opens: %d)", reads)
				require.Equal(t, 2, reads)
			})
		})
	}
}

func TestQueuedRangeMergeChargesOnlyUniqueBytes(t *testing.T) {
	for _, tc := range []struct {
		name         string
		secondOffset int64
	}{
		{name: "adjacent_requests", secondOffset: 128},
		{name: "overlapping_requests", secondOffset: 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const total = 192
			ctx := context.Background()
			fs, storage, _ := newIndexObjectFS(t, false)
			resume := busyFills(t, fs)

			for _, r := range []PrefetchRange{{Size: 128}, {Offset: tc.secondOffset, Size: total - tc.secondOffset}} {
				require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: r.Size, Ranges: []PrefetchRange{r}}))
			}
			resume()
			waitReviewPrefetchIdle(t, fs)

			d := fs.diskCache
			key := d.objectKey("object")
			_, prefixCached := d.ranges.covering(key, 0, 128)
			require.True(t, prefixCached)
			_, unionCached := d.ranges.covering(key, 0, total)
			require.True(t, unionCached, "all %d unique bytes are cached; cached ranges: %+v", total, d.ranges.of(key))
			require.EqualValues(t, 2, storage.reads.Load(), "the bytes the first fill does not hold get a fill of their own")
		})
	}
}
