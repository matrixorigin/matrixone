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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexPrefetchWarmsIndexTierWithWarmDiskCache(t *testing.T) {
	for _, queued := range []bool{false, true} {
		name := "direct_prefetch"
		if queued {
			name = "queued_prefetch"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fs, _, data := newIndexObjectFS(t, true)
			require.NoError(t, fs.PrefetchFile(ctx, "object"))
			require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
			_, cached := fs.indexObjectCached("object")
			require.False(t, cached)

			ctx = WithFileServicePolicy(ctx, IndexFileObject)
			if queued {
				require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
				require.Eventually(t, func() bool { return fs.prefetcher.known.Load() == 0 }, 5*time.Second, time.Millisecond)
			} else {
				require.NoError(t, fs.PrefetchFile(ctx, "object"))
			}
			_, cached = fs.indexObjectCached("object")
			require.True(t, cached, "an index prefetch should populate its own cache tier")
		})
	}
}

func TestFilePrefetchRetriesKnownObjectEnd(t *testing.T) {
	for _, tc := range []struct {
		name      string
		overshoot int64
	}{
		{name: "request_ends_at_eof"},
		{name: "request_ends_past_eof", overshoot: 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, storage, data := newPrefetchRangeFS(t, 0, 256<<10)
			fs.diskCache.memoryPrefetchMax = 128 << 10
			<-fs.diskCache.load.done
			r := PrefetchRange{Size: int64(len(data)) + tc.overshoot}
			req := ObjectPrefetch{FilePath: "object", Size: r.Size, Ranges: []PrefetchRange{r}}

			submitAndWait := func() {
				t.Helper()
				require.NoError(t, fs.PrefetchObject(ctx, req))
				require.Eventually(t, func() bool { return fs.prefetcher.known.Load() == 0 }, 5*time.Second, time.Millisecond)
			}
			submitAndWait()
			require.EqualValues(t, 1, storage.reads.Load())
			if tc.overshoot > 0 {
				size, ok := fs.diskCache.prefetched.size(fs.diskCache.pathForFile(req.FilePath))
				require.True(t, ok)
				require.EqualValues(t, len(data), size, "the first download should learn the real size")
			}
			submitAndWait()

			_, cached := fs.diskCache.ranges.covering(fs.diskCache.rangeObjectKey(req.FilePath), 0, int64(len(data)))
			require.True(t, cached, "the retry should cache the range through the known EOF")
		})
	}
}

func TestPrefetchDeferredDownloadOrder(t *testing.T) {
	for _, tc := range []struct {
		name        string
		deferredDue int64
		want        string
	}{
		{name: "queued_download_due_first", deferredDue: 30, want: "bystander"},
		{name: "deferred_download_due_first", deferredDue: 10, want: "object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			gate := newGatedRun("object", "bystander")
			p := newPrefetcher(1, func() int64 { return 2 }, func(ObjectPrefetch) bool { return false }, gate.run)
			defer p.close(context.Background())
			defer gate.releaseAll()
			next := func() string {
				t.Helper()
				select {
				case path := <-gate.started:
					return path
				case <-ctx.Done():
					t.Fatal(ctx.Err())
					return ""
				}
			}

			p.add(ObjectPrefetch{FilePath: "object", Size: 1, Ranges: []PrefetchRange{{Size: 1}}})
			require.Equal(t, "object", next())
			p.add(ObjectPrefetch{FilePath: "object", Size: 1, Due: tc.deferredDue, Ranges: []PrefetchRange{{Offset: 2, Size: 1}}})
			p.add(ObjectPrefetch{FilePath: "bystander", Size: 1, Due: 20})
			p.checkCharges(t)
			require.True(t, p.isQueued("bystander"))
			close(gate.release["object"])

			require.Equal(t, tc.want, next(), "the next worker should take the download due first")
		})
	}
}

func TestOversizedPrefetchMergePreservesQueue(t *testing.T) {
	for _, path := range []string{"new", "queued", "running"} {
		t.Run(path, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			gate := newGatedRun("running")
			p := newPrefetcher(1, func() int64 { return 4 }, func(ObjectPrefetch) bool { return false }, gate.run)
			defer p.close(context.Background())
			defer gate.releaseAll()

			p.add(ObjectPrefetch{FilePath: "running", Size: 1, Ranges: []PrefetchRange{{Size: 1}}})
			select {
			case started := <-gate.started:
				require.Equal(t, "running", started)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			p.add(ObjectPrefetch{FilePath: "queued", Size: 1, Ranges: []PrefetchRange{{Size: 1}}})
			ended := new(PrefetchScan)
			p.add(ObjectPrefetch{FilePath: "ended", Size: 2, Scan: ended})
			ended.End()

			p.add(ObjectPrefetch{FilePath: path, Size: 5, Ranges: []PrefetchRange{{Offset: 2, Size: 5}}})
			p.checkCharges(t)
			require.True(t, p.isQueued("ended"), "an oversized request must leave the queue unchanged")
			require.True(t, p.isQueued("queued"))
		})
	}
}

func TestDeleteRemovesQueuedPrefetchByObject(t *testing.T) {
	for _, qualify := range []bool{false, true} {
		name := "same_spelling"
		if qualify {
			name = "service_qualified_delete"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fs, _, data := newPrefetchRangeFS(t, 0, 1024)
			<-fs.diskCache.load.done
			fs.prefetcher.close(ctx)
			gate := newGatedRun("blocker")
			fs.prefetcher = newPrefetcher(1, fs.prefetchQueueLimit(), fs.haveObject, gate.run)
			defer gate.releaseAll()
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "blocker", Size: 1}))
			select {
			case path := <-gate.started:
				require.Equal(t, "blocker", path)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
			require.True(t, fs.prefetcher.isQueued("object"))

			path := "object"
			if qualify {
				path = JoinPath(fs.Name(), path)
			}
			require.NoError(t, fs.Delete(ctx, path))
			require.False(t, fs.prefetcher.isQueued("object"), "deleting an object should free its queued prefetch bytes")
		})
	}
}
