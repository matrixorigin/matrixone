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
	fs.prefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: int64(len(data))})
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
	require.Equal(t, int64(2), storage.reads.Load(), "the whole object in two parts")
	require.Zero(t, stats.stats.Load())
}

// A read of an object whose download is queued reads its range, and the queued download
// is dropped.
func TestS3FSReadOfQueuedObjectReadsRange(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: int64(len(data)), Due: 1 << 62}))
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Equal(t, int64(10), storage.bytes.Load(), "a range read of the entry")
	require.False(t, fs.prefetcher.isQueued("object"))
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
}

// A prefetch of a cached object marks it used: the disk cache evicts first the objects no
// prefetch asked for.
func TestPrefetchOfCachedObjectMarksItUsed(t *testing.T) {
	fs, _, _ := newObjectSplitFS(t, objectSplitConfig{parts: 1}, 1)
	d := fs.diskCache
	ctx := context.Background()
	a, b := d.pathForFile("a"), d.pathForFile("b")
	d.cache.Set(ctx, a, struct{}{}, 1)
	d.cache.Set(ctx, b, struct{}{}, 1)
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "a", Size: 1}))
	require.False(t, fs.prefetcher.isQueued("a"), "a cached object is not queued")
	d.cache.ForceEvict(ctx, 1)
	require.True(t, d.cache.Contains(a))
	require.False(t, d.cache.Contains(b))
}

// A prefetch of ranges of a cached object leaves it where it is in the eviction order.
func TestRangePrefetchOfCachedObjectLeavesItsOrder(t *testing.T) {
	fs, _, _ := newObjectSplitFS(t, objectSplitConfig{parts: 1}, 1)
	d := fs.diskCache
	ctx := context.Background()
	a, b := d.pathForFile("a"), d.pathForFile("b")
	d.cache.Set(ctx, a, struct{}{}, 1)
	d.cache.Set(ctx, b, struct{}{}, 1)
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "a", Size: 1, Ranges: []PrefetchRange{{Offset: 0, Size: 1}}}))
	require.False(t, fs.prefetcher.isQueued("a"), "a cached object is not queued")
	d.cache.ForceEvict(ctx, 1)
	require.False(t, d.cache.Contains(a))
	require.True(t, d.cache.Contains(b))
}

// A prefetch of ranges downloads each range on its own, not the prefetch ranges around it,
// and caches no whole object; once they are cached, a prefetch of them is not queued.
func TestS3FSPrefetchRanges(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 300<<10)
	ctx := context.Background()
	req := ObjectPrefetch{
		FilePath: "object",
		Size:     30,
		Ranges:   []PrefetchRange{{Offset: 100, Size: 10}, {Offset: 200 << 10, Size: 20}},
	}
	fs.prefetchObject(ctx, req)
	require.Equal(t, int64(2), storage.reads.Load())
	require.Equal(t, int64(30), storage.bytes.Load(), "not the bytes between the ranges")
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
	require.True(t, fs.haveObject(req))

	got := readRangeOf(t, fs, ColumnPrefetch, 200<<10+5, 10)
	require.Equal(t, data[200<<10+5:200<<10+15], got)
	require.Equal(t, int64(2), storage.reads.Load(), "a read within a range is served from the cache")

	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.NoError(t, fs.PrefetchObject(ctx, req))
	require.False(t, fs.prefetcher.isQueued("object"), "its ranges are cached")
	whole := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
	require.False(t, fs.haveObject(whole), "a whole request wants the object whatever ranges are cached")
	other := req
	other.Ranges = []PrefetchRange{{Offset: 1000, Size: 10}}
	require.NoError(t, fs.PrefetchObject(ctx, other))
	require.True(t, fs.prefetcher.isQueued("object"))
}

// Adjacent ranges not cached are read in one read; cached ones are not read.
func TestS3FSPrefetchRangesJoinsAdjacent(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 300<<10)
	ctx := context.Background()
	a, b, c := PrefetchRange{Offset: 1000, Size: 100}, PrefetchRange{Offset: 1100, Size: 50}, PrefetchRange{Offset: 4000, Size: 10}
	fs.prefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 160, Ranges: []PrefetchRange{c, b, a}})
	require.Equal(t, int64(2), storage.reads.Load(), "a and b in one read, c in another")
	require.Equal(t, int64(160), storage.bytes.Load())
	require.Equal(t, data[1050:1120], readRangeOf(t, fs, ColumnPrefetch, 1050, 70))
	require.Equal(t, int64(2), storage.reads.Load(), "a read across a and b is served from the cache")

	d := PrefetchRange{Offset: 1150, Size: 20}
	fs.prefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 180, Ranges: []PrefetchRange{a, b, d}})
	require.Equal(t, int64(3), storage.reads.Load(), "only d is read")
	require.Equal(t, int64(180), storage.bytes.Load())
}

// A download at a size other than the object's caches nothing.
func TestS3FSPrefetchObjectOfWrongSizeCachesNothing(t *testing.T) {
	for _, delta := range []int{-1000, 1000} {
		fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
		fs.diskCache.prefetched = prefetchedObjects{}
		fs.prefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: int64(len(data) + delta)})
		require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")), "size off by %d", delta)
	}
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

// A column read of an object whose column ranges are queued downloads those ranges and is
// served from them; a queued download of the whole object stays queued.
func TestS3FSColumnReadTakesQueuedColumns(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 300<<10)
	ctx := context.Background()
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	req := ObjectPrefetch{
		FilePath: "object",
		Size:     30,
		Due:      1 << 62,
		Ranges:   []PrefetchRange{{Offset: 100, Size: 10}, {Offset: 200 << 10, Size: 20}},
	}
	require.NoError(t, fs.PrefetchObject(ctx, req))
	require.True(t, fs.prefetcher.isQueued("object"))

	require.Equal(t, data[200<<10+5:200<<10+15], readRangeOf(t, fs, ColumnPrefetch, 200<<10+5, 10))
	require.False(t, fs.prefetcher.isQueued("object"), "the read took the queued columns")
	require.Equal(t, int64(2), storage.reads.Load())
	require.Equal(t, int64(30), storage.bytes.Load(), "the column ranges, not the bytes around them")
	require.Equal(t, data[102:107], readRangeOf(t, fs, ColumnPrefetch, 102, 5))
	require.Equal(t, int64(2), storage.reads.Load(), "served from the column ranges")

	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data)), Due: 1 << 62}))
	readRangeOf(t, fs, ColumnPrefetch, 150<<10, 10)
	require.True(t, fs.prefetcher.isQueued("object"), "a whole-object download is not taken")
}

// A column read whose entries have bytes between them reads each one, however close, and
// never the bytes between; entries that touch are read as one range.
func TestS3FSColumnReadOfSparseEntriesReadsThemApart(t *testing.T) {
	read := func(fs *S3FS, entries ...IOEntry) *IOVector {
		vec := &IOVector{FilePath: "object", Policy: ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache, Entries: entries}
		require.NoError(t, fs.Read(context.Background(), vec))
		t.Cleanup(vec.Release)
		return vec
	}
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 300<<10)
	vec := read(fs, IOEntry{Offset: 100, Size: 10}, IOEntry{Offset: 200 << 10, Size: 10})
	require.Equal(t, data[100:110], vec.Entries[0].Data)
	require.Equal(t, data[200<<10:200<<10+10], vec.Entries[1].Data)
	require.Equal(t, int64(2), storage.reads.Load())
	require.Equal(t, int64(20), storage.bytes.Load(), "not the bytes between the entries")

	fs, storage, data = newPrefetchRangeFS(t, 256<<10, 300<<10)
	vec = read(fs, IOEntry{Offset: 1000, Size: 10}, IOEntry{Offset: 1010, Size: 20})
	require.Equal(t, data[1010:1030], vec.Entries[1].Data)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Equal(t, int64(30), storage.bytes.Load())
}

// An exact read reads only its entry, and leaves a queued download of the object's column
// ranges to the prefetcher.
func TestS3FSExactReadLeavesQueuedColumns(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 64<<10, 300<<10)
	ctx := context.Background()
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
		FilePath: "object", Size: 10, Due: 1 << 62, Ranges: []PrefetchRange{{Offset: 0, Size: 10}},
	}))
	require.Equal(t, data[200<<10:200<<10+10], readRangeOf(t, fs, ExactRead, 200<<10, 10))
	require.Equal(t, int64(10), storage.bytes.Load())
	require.True(t, fs.prefetcher.isQueued("object"), "the column ranges stay queued")
}
