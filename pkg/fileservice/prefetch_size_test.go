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

	"github.com/stretchr/testify/require"
)

// The prefetcher downloads the whole object at the size the prefetch told, without asking
// object storage for it.
func TestS3FSPrefetchObjectDownloadsWhole(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	fs.diskCache.prefetched = prefetchedObjects{}
	stats := &statCountingStorage{ObjectStorage: fs.storage}
	fs.storage = stats
	fs.prefetchObject(ObjectPrefetch{FilePath: "object", Size: int64(len(data))})
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
	require.Equal(t, int64(2), storage.reads.Load(), "the whole object in two parts")
	require.Zero(t, stats.stats.Load())
}

// A read of an object whose download is queued reads its range, and the queued download
// goes first.
func TestS3FSReadOfQueuedObjectReadsRange(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: int64(len(data)), Due: 1 << 62}))
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Equal(t, int64(10), storage.bytes.Load(), "a range read of the entry")
	require.True(t, fs.prefetcher.isQueued("object"))
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
}

func TestPrefetchedObjectsBound(t *testing.T) {
	var o prefetchedObjects
	_, ok := o.size("a")
	require.False(t, ok)
	o.store("a", 10)
	o.store("a", 10)
	size, ok := o.size("a")
	require.True(t, ok)
	require.Equal(t, int64(10), size)
	require.Equal(t, int64(1), o.n.Load())
	o.n.Store(prefetchedObjectsMax)
	o.store("b", 20)
	require.Equal(t, int64(1), o.n.Load(), "past the bound the sizes are dropped")
	_, ok = o.size("a")
	require.False(t, ok)
}
