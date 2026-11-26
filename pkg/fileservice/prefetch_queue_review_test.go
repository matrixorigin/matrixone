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
			require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))))
			_, cached := fs.indexObjectCached("object")
			require.False(t, cached)

			ctx = WithFileServicePolicy(ctx, IndexFileObject)
			if queued {
				require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
				waitFillsIdle(t, fs.diskCache)
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
				waitFillsIdle(t, fs.diskCache)
			}
			submitAndWait()
			require.EqualValues(t, 1, storage.reads.Load())
			if tc.overshoot > 0 {
				size, ok := fs.diskCache.prefetched.size(fs.diskCache.objectKey(req.FilePath))
				require.True(t, ok)
				require.EqualValues(t, len(data), size, "the first download should learn the real size")
			}
			submitAndWait()

			_, cached := fs.diskCache.ranges.covering(fs.diskCache.objectKey(req.FilePath), 0, int64(len(data)))
			require.True(t, cached, "the retry should cache the range through the known EOF")
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
			busyFills(t, fs)
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
			require.True(t, fs.diskCache.isQueued("object"))

			path := "object"
			if qualify {
				path = JoinPath(fs.Name(), path)
			}
			require.NoError(t, fs.Delete(ctx, path))
			require.False(t, fs.diskCache.isQueued("object"), "deleting an object should free its queued prefetch bytes")
		})
	}
}
