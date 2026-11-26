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
	"os"
	"path/filepath"
	"testing"
	"time"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/stretchr/testify/require"
)

// A fill downloads the whole object at the size the prefetch told, without asking
// object storage for it.
func TestS3FSPrefetchObjectDownloadsWhole(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	fs.diskCache.prefetched = prefetchedObjects{}
	stats := &statCountingStorage{ObjectStorage: fs.server.raw}
	fs.server.raw = stats
	fs.prefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: int64(len(data))})
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))))
	require.Equal(t, int64(2), storage.reads.Load(), "the whole object in two parts")
	require.Zero(t, stats.stats.Load())
}

// A read of an object whose download waits, while the background fills and all background
// room are held, joins that download and promotes it: it is read once, in two parts from
// the read's block, serves the read and is cached whole.
func TestS3FSReadOfQueuedObjectJoinsItsDownload(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	busyFills(t, fs)
	defer occupy(t, fs.server, backgroundRequest, fs.server.backgroundMax-1)()
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: int64(len(data)), Due: 1 << 62}))
	require.True(t, fs.diskCache.isQueued("object"))
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.False(t, fs.diskCache.isQueued("object"))
	require.Eventually(t, func() bool { return fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))) },
		10*time.Second, time.Millisecond)
	require.Equal(t, int64(2), storage.reads.Load(), "the whole object in two parts, read once")
	require.ElementsMatch(t, []int64{0, 192 << 10}, storage.starts)
}

// A prefetch of a cached object marks it used: the disk cache evicts first the objects no
// prefetch asked for.
func TestPrefetchOfCachedObjectMarksItUsed(t *testing.T) {
	fs, _, _ := newObjectSplitFS(t, objectSplitConfig{parts: 1}, 1)
	d := fs.diskCache
	ctx := context.Background()
	<-d.load.done
	a, b := d.wholePath("a", 1), d.wholePath("b", 1)
	for _, p := range []string{a, b} {
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte{1}, 0o644))
		d.cache.Set(ctx, p, struct{}{}, 1)
	}
	busyFills(t, fs)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "a", Size: 1}))
	require.False(t, fs.diskCache.isQueued("a"), "a cached object is not queued")
	d.cache.ForceEvict(ctx, 1)
	require.True(t, d.cache.Contains(a))
	require.False(t, d.cache.Contains(b))
}

// A prefetch of ranges of a cached object leaves it where it is in the eviction order.
func TestRangePrefetchOfCachedObjectLeavesItsOrder(t *testing.T) {
	fs, _, _ := newObjectSplitFS(t, objectSplitConfig{parts: 1}, 1)
	d := fs.diskCache
	ctx := context.Background()
	<-d.load.done
	a, b := d.wholePath("a", 1), d.wholePath("b", 1)
	for _, p := range []string{a, b} {
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte{1}, 0o644))
		d.cache.Set(ctx, p, struct{}{}, 1)
	}
	busyFills(t, fs)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "a", Size: 1, Ranges: []PrefetchRange{{Offset: 0, Size: 1}}}))
	require.False(t, fs.diskCache.isQueued("a"), "a cached object is not queued")
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
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))))
	require.True(t, fs.haveObject(req))

	got := readRangeOf(t, fs, ColumnPrefetch, 200<<10+5, 10)
	require.Equal(t, data[200<<10+5:200<<10+15], got)
	require.Equal(t, int64(2), storage.reads.Load(), "a read within a range is served from the cache")

	busyFills(t, fs)
	require.NoError(t, fs.PrefetchObject(ctx, req))
	require.False(t, fs.diskCache.isQueued("object"), "its ranges are cached")
	whole := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
	require.False(t, fs.haveObject(whole), "a whole request wants the object whatever ranges are cached")
	other := req
	other.Ranges = []PrefetchRange{{Offset: 1000, Size: 10}}
	require.NoError(t, fs.PrefetchObject(ctx, other))
	require.True(t, fs.diskCache.isQueued("object"))
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
		require.Empty(t, fs.diskCache.objectFiles("object"), "size off by %d", delta)
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
	require.Len(t, o.m, 1)
	for i := len(o.m); i < prefetchedObjectsMax; i++ {
		o.store(fmt.Sprint(i), 1)
	}
	o.store("b", 20)
	require.Len(t, o.m, 1, "past the bound the sizes are dropped")
	_, ok = o.size("a")
	require.False(t, ok)
}

// A column read of an object whose column ranges are queued joins the fill holding its
// bytes, which starts as foreground, and is served from it; the fill of another range
// stays queued until a read of it joins it. A column read joins a queued fill of the
// whole object.
func TestS3FSColumnReadJoinsQueuedColumns(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 300<<10)
	ctx := context.Background()
	busyFills(t, fs)
	req := ObjectPrefetch{
		FilePath: "object",
		Size:     30,
		Due:      1 << 62,
		Ranges:   []PrefetchRange{{Offset: 100, Size: 10}, {Offset: 200 << 10, Size: 20}},
	}
	require.NoError(t, fs.PrefetchObject(ctx, req))
	require.True(t, fs.diskCache.isQueued("object"))

	cache := &publicationVectorCache{}
	vec := &IOVector{FilePath: "object", Policy: ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache,
		Caches: []IOVectorCache{cache}, Entries: []IOEntry{{Offset: 200<<10 + 5, Size: 10}}}
	require.NoError(t, fs.Read(ctx, vec))
	waitObjectsEnd(fs.diskCache, "object")
	require.Equal(t, data[200<<10+5:200<<10+15], vec.Entries[0].Data)
	vec.Release()
	require.Equal(t, int64(1), cache.updates.Load(), "the read updates the caches it missed")
	require.True(t, fs.diskCache.isQueued("object"), "the fill of the other range stays queued")
	require.Equal(t, int64(1), storage.reads.Load())
	require.Equal(t, int64(20), storage.bytes.Load(), "the column range, not the bytes around it")
	require.Equal(t, data[102:107], readRangeOf(t, fs, ColumnPrefetch, 102, 5))
	require.False(t, fs.diskCache.isQueued("object"))
	require.Equal(t, int64(2), storage.reads.Load())
	require.Equal(t, int64(30), storage.bytes.Load(), "the column ranges, not the bytes around them")
	require.Equal(t, data[103:106], readRangeOf(t, fs, ColumnPrefetch, 103, 3))
	require.Equal(t, int64(2), storage.reads.Load(), "served from the column ranges")

	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data)), Due: 1 << 62}))
	require.Equal(t, data[150<<10:150<<10+10], readRangeOf(t, fs, ColumnPrefetch, 150<<10, 10))
	require.False(t, fs.diskCache.isQueued("object"), "a column read joins a whole-object download")
	require.Eventually(t, func() bool { return fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))) },
		10*time.Second, time.Millisecond)
}

// A prefetch of ranges of more bytes than a MemoryPrefetchObject downloads them to their
// cache files as FilePrefetchObjects, not the object.
func TestS3FSPrefetchPastMemoryMaxDownloadsFiles(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 300<<10)
	fs.diskCache.memoryPrefetchMax = 20
	ranges := []PrefetchRange{{Offset: 100, Size: 10}, {Offset: 200 << 10, Size: 20}}
	fs.prefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: 30, Ranges: ranges})
	require.Equal(t, int64(30), storage.bytes.Load(), "only the ranges")
	key := fs.diskCache.objectKey("object")
	for _, r := range ranges {
		_, cached := fs.diskCache.ranges.covering(key, r.Offset, r.Size)
		require.True(t, cached)
	}
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))))
	require.Empty(t, fs.diskCache.prefetchObjects.m, "no MemoryPrefetchObject")
	require.Equal(t, data[200<<10+5:200<<10+15], readRangeOf(t, fs, ColumnPrefetch, 200<<10+5, 10))
	require.Equal(t, int64(30), storage.bytes.Load(), "served from the cached range")
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
// ranges to their fill.
func TestS3FSExactReadLeavesQueuedColumns(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 64<<10, 300<<10)
	ctx := context.Background()
	busyFills(t, fs)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
		FilePath: "object", Size: 10, Due: 1 << 62, Ranges: []PrefetchRange{{Offset: 0, Size: 10}},
	}))
	require.Equal(t, data[200<<10:200<<10+10], readRangeOf(t, fs, ExactRead, 200<<10, 10))
	require.Equal(t, int64(10), storage.bytes.Load())
	require.True(t, fs.diskCache.isQueued("object"), "the column ranges stay queued")
}

// A column prefetch past the bytes of the flights under way reads its ranges one by one,
// and those reads do not wait for the prefetch itself.
func TestS3FSPrefetchPastFlightBytesFinishes(t *testing.T) {
	fs, storage, _ := newPrefetchRangeFS(t, 256<<10, 300<<10)
	fs.diskCache.prefetchFlightBytes = 1
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{
		FilePath: "object", Size: 30, Due: 1 << 62,
		Ranges: []PrefetchRange{{Offset: 100, Size: 10}, {Offset: 200 << 10, Size: 20}},
	}))
	require.Eventually(t, func() bool { return !fs.diskCache.fillsUnderWay("object") && !fs.diskCache.isQueued("object") },
		10*time.Second, time.Millisecond)
	require.Equal(t, int64(2), storage.reads.Load())
}

// A column prefetch whose range read fails caches nothing and counts as failed, not
// downloaded.
func TestS3FSColumnPrefetchFailureCountsFailed(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 256<<10, 300<<10)
	storage := &failingRangeStorage{ObjectStorage: fs.server.raw}
	storage.fail.Store(true)
	fs.server.raw = storage
	downloaded := promtestutil.ToFloat64(metric.FSPrefetchDownloadedCounter)
	failed := promtestutil.ToFloat64(metric.FSPrefetchFailedCounter)
	fs.prefetchObject(context.Background(), ObjectPrefetch{
		FilePath: "object", Size: 10, Ranges: []PrefetchRange{{Offset: 100, Size: 10}},
	})
	require.Equal(t, downloaded, promtestutil.ToFloat64(metric.FSPrefetchDownloadedCounter))
	require.Equal(t, failed+1, promtestutil.ToFloat64(metric.FSPrefetchFailedCounter))
	_, cached := fs.diskCache.ranges.covering(fs.diskCache.objectKey("object"), 100, 10)
	require.False(t, cached)
}

// A column prefetch past the background share of the flight budget waits, as a queued
// fill holding nothing, for the flights under way to end, then reads its ranges as
// flights; it reads nothing meanwhile.
func TestS3FSColumnPrefetchWaitsForFlightBytes(t *testing.T) {
	fs, storage, _ := newPrefetchRangeFS(t, 256<<10, 300<<10)
	fs.diskCache.prefetchFlightBytes = 2 * flightBufSize(10)
	held := &heldStorage{ObjectStorage: fs.server.raw, read: make(chan struct{}), released: make(chan struct{})}
	fs.server.raw = held
	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 10, Ranges: []PrefetchRange{{Offset: 100, Size: 10}}}))
	<-held.read
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 20, Ranges: []PrefetchRange{{Offset: 200 << 10, Size: 20}}}))
	require.True(t, fs.diskCache.isQueued("object"), "the second prefetch waits for room")
	require.Equal(t, int64(1), storage.reads.Load())
	f := &fs.diskCache.prefetchObjects
	f.mu.Lock()
	require.Equal(t, flightBufSize(10), f.bytes, "the waiting fill holds no buffer")
	f.mu.Unlock()

	close(held.released)
	waitFillsEnd(fs.diskCache, "object")
	require.Equal(t, int64(2), storage.reads.Load())
	_, cached := fs.diskCache.ranges.covering(fs.diskCache.objectKey("object"), 200<<10, 20)
	require.True(t, cached)
}

// A column read the disk cache holds returns from it while a download of other columns of
// its object stays queued, without waiting to join that download.
func TestS3FSCachedColumnReadDoesNotWaitForQueuedColumns(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 300<<10)
	ctx := context.Background()
	fs.prefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 10, Ranges: []PrefetchRange{{Offset: 100, Size: 10}}})
	reads := storage.reads.Load()
	busyFills(t, fs)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
		FilePath: "object", Size: 20, Due: 1 << 62, Ranges: []PrefetchRange{{Offset: 200 << 10, Size: 20}},
	}))
	require.True(t, fs.diskCache.isQueued("object"))

	got := make(chan []byte, 1)
	go func() { got <- readRangeOf(t, fs, ColumnPrefetch, 102, 5) }()
	select {
	case b := <-got:
		require.Equal(t, data[102:107], b)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the cached read waited for the queued download")
	}
	require.Equal(t, reads, storage.reads.Load(), "served from the disk cache")
	require.True(t, fs.diskCache.isQueued("object"), "the other columns stay queued")
}

// A column read the caches do not hold reads its bytes without waiting for a queued
// download of other columns of its object, which stays queued.
func TestS3FSColumnReadLeavesQueuedOtherColumns(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 300<<10)
	ctx := context.Background()
	busyFills(t, fs)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
		FilePath: "object", Size: 20, Due: 1 << 62, Ranges: []PrefetchRange{{Offset: 200 << 10, Size: 20}},
	}))
	require.True(t, fs.diskCache.isQueued("object"))

	got := make(chan []byte, 1)
	go func() { got <- readRangeOf(t, fs, ColumnPrefetch, 102, 5) }()
	select {
	case b := <-got:
		require.Equal(t, data[102:107], b)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the read waited for a download of other columns")
	}
	require.NotZero(t, storage.reads.Load())
	require.True(t, fs.diskCache.isQueued("object"), "the other columns stay queued")
}

// A read that skips the disk cache reads its bytes without waiting for a queued download
// of its object, which stays queued.
func TestS3FSCacheBypassReadLeavesQueuedObject(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 300<<10)
	ctx := context.Background()
	busyFills(t, fs)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data)), Due: 1 << 62}))
	require.True(t, fs.diskCache.isQueued("object"))

	got := make(chan error, 1)
	vec := &IOVector{FilePath: "object", Policy: SkipAllCache, Entries: []IOEntry{{Offset: 100, Size: 10}}}
	go func() { got <- fs.Read(ctx, vec) }()
	select {
	case err := <-got:
		require.NoError(t, err)
		require.Equal(t, data[100:110], vec.Entries[0].Data)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the read waited for the queued download")
	}
	require.NotZero(t, storage.reads.Load())
	require.True(t, fs.diskCache.isQueued("object"), "the download stays queued")
}
