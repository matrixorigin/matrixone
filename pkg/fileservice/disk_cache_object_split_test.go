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
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

type failingRangeStorage struct {
	ObjectStorage
	fail atomic.Bool
}

func (f *failingRangeStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	if f.fail.Load() && min != nil && *min > 0 {
		return nil, errors.New("injected range read failure")
	}
	return f.ObjectStorage.Read(ctx, key, min, max)
}

type statCountingStorage struct {
	ObjectStorage
	stats atomic.Int64
}

func (s *statCountingStorage) Stat(ctx context.Context, key string) (int64, error) {
	s.stats.Add(1)
	return s.ObjectStorage.Stat(ctx, key)
}

func newObjectSplitFS(t *testing.T, split objectSplitConfig, size int) (*S3FS, *countingStorage, []byte) {
	fs, storage, data := newPrefetchRangeFS(t, 0, size)
	fs.diskCache.objectSplit = split
	fs.diskCache.prefetched.store(fs.diskCache.objectKey("object"), int64(size))
	return fs, storage, data
}

func readFullObjectOf(fs *S3FS, offset, size int64) ([]byte, error) {
	vec := &IOVector{FilePath: "object", Policy: SkipMemoryCache, Entries: []IOEntry{{Offset: offset, Size: size}}}
	err := fs.Read(context.Background(), vec)
	data := slices.Clone(vec.Entries[0].Data)
	vec.Release()
	return data, err
}

// The download of an object of at least the split size reads its 64KiB-aligned halves
// at once and caches the whole object.
func TestS3FSObjectSplitDownloadReadsParts(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10+17)
	got, err := readFullObjectOf(fs, 250<<10, 10)
	require.NoError(t, err)
	require.Equal(t, data[250<<10:250<<10+10], got)
	require.Equal(t, int64(2), storage.reads.Load())
	require.ElementsMatch(t, []int64{0, 192 << 10}, storage.starts)
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))))

	got, err = readFullObjectOf(fs, 0, int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Equal(t, int64(2), storage.reads.Load(), "the cached whole object serves later reads")
}

// An object smaller than the split size downloads as one range.
func TestS3FSObjectSplitSkipsSmallObjects(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 1 << 20}, 300<<10)
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Empty(t, storage.starts, "an open-ended read of the object")
}

// With the most downloads in flight that a split allows, a large object downloads as one
// range.
func TestS3FSObjectSplitSkipsSharedLink(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	d := fs.diskCache
	for i := 0; i < DefaultDiskPrefetchObjectSplitDownloads; i++ {
		d.prefetchObjects.downloads.n.Add(1)
	}
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Empty(t, storage.starts, "an open-ended read of the object")
}

// A download of an object no prefetch told the size of reads it as one range, and never
// asks object storage for its size.
func TestS3FSObjectSplitNeedsKnownSize(t *testing.T) {
	fs, counting, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	fs.diskCache.prefetched = prefetchedObjects{}
	storage := &statCountingStorage{ObjectStorage: fs.storage}
	fs.storage = storage
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Equal(t, int64(1), counting.reads.Load())
	require.Empty(t, counting.starts, "an open-ended read of the object")
	require.Zero(t, storage.stats.Load())
}

// A failed part fails the download, which publishes no whole object.
func TestS3FSObjectSplitFailedPartPublishesNothing(t *testing.T) {
	fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	failing := &failingRangeStorage{ObjectStorage: fs.storage}
	failing.fail.Store(true)
	fs.storage = failing
	_, err := readFullObjectOf(fs, 10, 10)
	require.Error(t, err)
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))))

	failing.fail.Store(false)
	got, err := readFullObjectOf(fs, 10, 10)
	require.NoError(t, err)
	require.Equal(t, data[10:20], got)
}

// A reader of a part waits only for that part.
func TestObjectDownloadWaitsForItsPart(t *testing.T) {
	ctx := context.Background()
	o := newObjectDownload("", false, []int64{0, 4, 8})
	_, wait := o.estimate(0, 1)
	require.Equal(t, time.Duration(math.MaxInt64), wait, "a part that has not begun is out of reach")
	o.begin(0)
	o.begin(1)
	_, wait = o.estimate(0, 1)
	require.Positive(t, wait, "a part begun but not yet read is up to a latency away")
	require.LessOrEqual(t, wait, rangeReadCosts.firstByte()+time.Duration(float64(time.Second)/downloadRates.rate()))
	o.advance(1, 4)
	written, ok := o.waitFor(ctx, 4, 8, time.Millisecond)
	require.True(t, ok)
	require.Equal(t, int64(8), written)
	behind, wait := o.estimate(4, 8)
	require.Zero(t, behind)
	require.Zero(t, wait)

	_, ok = o.waitFor(ctx, 0, 1, time.Millisecond)
	require.False(t, ok, "the first part has no byte")
	behind, _ = o.estimate(0, 8)
	require.Equal(t, int64(4), behind)

	o.advance(0, 2)
	written, ok = o.waitFor(ctx, 0, 2, time.Millisecond)
	require.True(t, ok)
	require.Equal(t, int64(2), written, "the bytes written from the start stop at the gap")
	o.advance(0, 2)
	o.end(true)
	written, ok = o.waitFor(ctx, 0, -1, time.Millisecond)
	require.True(t, ok)
	require.Equal(t, int64(8), written)
}

func TestObjectSplitBounds(t *testing.T) {
	split := objectSplitConfig{parts: 2, min: 1 << 20}
	require.Nil(t, split.bounds(1<<20-1, 0))
	require.Equal(t, []int64{0, 704 << 10, 1<<20 + 300<<10}, split.bounds(1<<20+300<<10, 0))
	require.Equal(t, []int64{0, 384 << 10, 768 << 10, 1152 << 10, 1<<20 + 300<<10},
		objectSplitConfig{parts: 4, min: 1 << 20}.bounds(1<<20+300<<10, 0))
	require.Nil(t, objectSplitConfig{parts: 1, min: 0}.bounds(64<<20, 0))
	require.Equal(t, []int64{0, 1 << 20, 1<<20 + 300<<10}, split.bounds(1<<20+300<<10, 1<<20+1000),
		"a part starts at the 64KiB block holding the read's first byte")
	require.Equal(t, []int64{0, 704 << 10, 1<<20 + 300<<10}, split.bounds(1<<20+300<<10, 1000), "inside the first block")
	require.Equal(t, []int64{0, 704 << 10, 1<<20 + 300<<10}, split.bounds(1<<20+300<<10, 2<<20), "past the object")
	four := objectSplitConfig{parts: 4, min: 1 << 20}
	require.Equal(t, []int64{0, 384 << 10, 768 << 10, 1 << 20, 1<<20 + 300<<10}, four.bounds(1<<20+300<<10, 1<<20+1000))
	require.Equal(t, []int64{0, 448 << 10, 768 << 10, 1152 << 10, 1<<20 + 300<<10}, four.bounds(1<<20+300<<10, 448<<10))
	require.Equal(t, []int64{0, 128 << 10, 768 << 10, 1152 << 10, 1<<20 + 300<<10}, four.bounds(1<<20+300<<10, 128<<10))
	require.Equal(t, []int64{0, 384 << 10, 768 << 10, 1152 << 10, 1<<20 + 300<<10}, four.bounds(1<<20+300<<10, 768<<10))
	require.Equal(t, []int64{0, 64 << 10, 128 << 10}, objectSplitConfig{parts: 2, min: 0}.bounds(128<<10, 64<<10))
	require.Equal(t, []int64{0, 64 << 10, 100 << 10}, objectSplitConfig{parts: 4, min: 0}.bounds(100<<10, 64<<10))
}

func TestDiskObjectSplitConfig(t *testing.T) {
	require.Equal(t, objectSplitConfig{parts: 2, min: 16 << 20, downloads: 8}, (&CacheConfig{}).diskObjectSplit())
	require.Equal(t, objectSplitConfig{parts: 1, min: 16 << 20, downloads: 8}, (&CacheConfig{DiskPrefetchObjectSplit: ptrTo(0)}).diskObjectSplit())
	require.Equal(t, objectSplitConfig{parts: 4, min: 1 << 20, downloads: 3}, (&CacheConfig{
		DiskPrefetchObjectSplit:          ptrTo(9),
		DiskPrefetchObjectSplitMin:       ptrTo[toml.ByteSize](1 << 20),
		DiskPrefetchObjectSplitDownloads: ptrTo(3),
	}).diskObjectSplit())
}

// disk-prefetch-memory-max defaults to 32 MiB and is at least 8 MiB.
func TestDiskPrefetchMemoryMaxConfig(t *testing.T) {
	require.Equal(t, int64(32<<20), (&CacheConfig{}).diskPrefetchMemoryMax())
	require.Equal(t, int64(64<<20), (&CacheConfig{DiskPrefetchMemoryMax: ptrTo[toml.ByteSize](64 << 20)}).diskPrefetchMemoryMax())
	require.Equal(t, int64(8<<20), (&CacheConfig{DiskPrefetchMemoryMax: ptrTo[toml.ByteSize](1)}).diskPrefetchMemoryMax())
	require.Equal(t, int64(32<<20), (&CacheConfig{DiskPrefetchMemoryMax: ptrTo[toml.ByteSize](0)}).diskPrefetchMemoryMax())
}

// A full-object read does not wait for the read that holds its merge.
func TestS3FSFullObjectReadSkipsMergeWait(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	originalShortWait := shortIOWaitDuration
	shortIOWaitDuration = 10 * time.Second
	t.Cleanup(func() { shortIOWaitDuration = originalShortWait })
	key := fs.readMergeKey(&IOVector{FilePath: "object", Policy: SkipMemoryCache, Entries: []IOEntry{{Offset: 100, Size: 10}}})
	require.True(t, key.FullObject)
	done, wait := fs.ioMerger.Merge(key, maxIOWaitDuration)
	require.NotNil(t, done)
	require.Nil(t, wait)
	defer done()

	t0 := time.Now()
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Less(t, time.Since(t0), 2*time.Second)
}

// The parts of a split download each hold a connection while they read: with one
// connection free they read one after the other, and each releases it once it ends.
func TestS3FSDownloadConnsReservedPerPart(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	d := fs.diskCache
	d.conns.max = 2
	d.conns.held.Store(1)
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Equal(t, int64(2), storage.reads.Load())
	require.ElementsMatch(t, []int64{0, 192 << 10}, storage.starts, "the split stays with one connection free")
	require.Equal(t, int64(1), d.conns.held.Load(), "its connections are released")
	require.True(t, d.cache.Contains(d.wholePath("object", int64(len(data)))))
}

// A download with no connection free waits for one, and downloads once one is released.
func TestS3FSDownloadWaitsForConn(t *testing.T) {
	fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	d := fs.diskCache
	d.conns.max = 1
	d.conns.held.Store(1)
	done := make(chan []byte, 1)
	go func() {
		got, err := readFullObjectOf(fs, 100, 10)
		require.NoError(t, err)
		done <- got
	}()
	require.Eventually(t, func() bool {
		d.conns.mu.Lock()
		defer d.conns.mu.Unlock()
		return d.conns.freed.ch != nil
	}, 10*time.Second, time.Millisecond, "the download waits for a connection")
	require.False(t, d.cache.Contains(d.wholePath("object", int64(len(data)))))
	require.False(t, d.isUpdating(d.objectKey("object")), "waiting for a connection holds no update of the path")
	d.conns.release()
	require.Equal(t, data[100:110], <-done)
	require.True(t, d.cache.Contains(d.wholePath("object", int64(len(data)))))
	require.Zero(t, d.conns.held.Load())
}

// waitingForConn reports whether a whole-object download of the object at diskPath waits
// for a connection.
func waitingForConn(d *DiskCache, diskPath string) bool {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	for o := range f.waiting {
		if p, ok := o.(*filePrefetchObject); ok && p.whole && p.diskPath == diskPath && p.state == objectWaitingConn {
			return true
		}
	}
	return false
}

// A read whose whole-object download waits for a connection when its object is deleted,
// or the cache closes, reads its range and caches nothing.
func TestS3FSReadWaitingForConnectionEndsOnDeleteOrClose(t *testing.T) {
	for _, end := range []string{"delete", "close"} {
		t.Run(end, func(t *testing.T) {
			fs, _, data := newObjectSplitFS(t, objectSplitConfig{}, 300<<10)
			fs.storage = keepOnDeleteStorage{fs.storage}
			d := fs.diskCache
			d.conns.max = 1
			d.conns.held.Store(1)
			diskPath := d.objectKey("object")
			done := make(chan []byte, 1)
			go func() {
				got, err := readFullObjectOf(fs, 100, 10)
				require.NoError(t, err)
				done <- got
			}()
			require.Eventually(t, func() bool { return waitingForConn(d, diskPath) }, 10*time.Second, time.Millisecond)
			if end == "delete" {
				require.NoError(t, fs.Delete(context.Background(), "object"))
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				d.closePrefetches(ctx)
				require.NoError(t, ctx.Err(), "the close waits for no download")
			}
			require.Equal(t, data[100:110], <-done)
			require.False(t, waitingForConn(d, diskPath))
			require.Empty(t, d.objectFiles("object"))
			require.Equal(t, int64(1), d.conns.held.Load(), "it took no connection")
		})
	}
}

// A whole-object prefetch waiting for a slot to publish its file when its object is
// deleted ends, downloading nothing.
func TestS3FSPrefetchWaitingForPublicationSlotEndsOnDelete(t *testing.T) {
	fs, storage, _ := newObjectSplitFS(t, objectSplitConfig{}, 300<<10)
	fs.storage = keepOnDeleteStorage{fs.storage}
	fs.SetAsyncUpdate(true)
	d := fs.diskCache
	a := &d.async
	for len(a.slots) < cap(a.slots) {
		a.slots <- struct{}{}
	}
	t.Cleanup(func() {
		a.mu.Lock()
		for len(a.slots) > 0 {
			<-a.slots
		}
		a.wakeFreedLocked()
		a.mu.Unlock()
	})
	done := make(chan error, 1)
	go func() { done <- fs.PrefetchFile(context.Background(), "object") }()
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.mu.freed.ch != nil
	}, 10*time.Second, time.Millisecond, "the prefetch waits for a slot")
	require.NoError(t, fs.Delete(context.Background(), "object"))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the prefetch of the deleted object still waits for a slot")
	}
	require.Zero(t, storage.reads.Load())
	require.Empty(t, d.objectFiles("object"))
	require.Zero(t, d.conns.held.Load())
}

// A whole-object download of an object deleted since the mark of its context, before it
// began, downloads nothing.
func TestWholeObjectDownloadDeletedBeforeItBegins(t *testing.T) {
	fs, storage, _ := newObjectSplitFS(t, objectSplitConfig{}, 300<<10)
	d := fs.diskCache
	diskPath := d.objectKey("object")
	mark := d.deletes.now()
	d.deletes.deleted(diskPath)
	err := fs.PrefetchFile(withDeletesMark(context.Background(), mark), "object")
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, storage.reads.Load())
	require.Empty(t, d.objectFiles("object"))
}

// Deleting an object forgets the size a prefetch told of: after the path is written again
// at another length, a read to the end of it reads the new object whole.
func TestDeleteForgetsPrefetchedSize(t *testing.T) {
	fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Delete(ctx, "object"))
	_, known := fs.diskCache.prefetched.size(fs.diskCache.objectKey("object"))
	require.False(t, known)

	longer := make([]byte, 2*len(data))
	for i := range longer {
		longer[i] = byte(i*11 + i/313)
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(longer)), Data: longer}}, Policy: SkipAllCache}))
	got, err := readFullObjectOf(fs, 0, -1)
	require.NoError(t, err)
	require.Equal(t, longer, got)
}

// A prefetch queued before an object is deleted is dropped, and one already taken from the
// queue does not store the size it was told.
func TestPrefetchBeforeDeleteKeepsNoSize(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
	require.True(t, fs.prefetcher.isQueued("object"))
	taken := fs.prefetcher.entries[prefetchKey{path: "object"}].req
	require.NoError(t, fs.Delete(ctx, "object"))
	require.False(t, fs.prefetcher.isQueued("object"), "the delete drops the queued prefetch")

	fs.prefetchObject(ctx, taken)
	_, known := fs.diskCache.prefetched.size(fs.diskCache.objectKey("object"))
	require.False(t, known, "a prefetch made before the delete does not store its size")
}

// Whole-object downloads hold at most half the connections per host, which are at least
// minMaxConnsPerHost: one configured connection still caches whole objects.
func TestS3FSConfiguredConnsBoundDownloads(t *testing.T) {
	for _, tc := range []struct {
		conns    int
		limit    int64
		download bool
	}{{minMaxConnsPerHost, int64(minMaxConnsPerHost / 2), true}, {256, 128, true}} {
		ctx := context.Background()
		fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir(), MaxConnsPerHost: tc.conns}, CacheConfig{
			DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](64 << 20),
			MemoryCapacity: ptrTo[toml.ByteSize](0),
		}, nil, false, false)
		require.NoError(t, err)
		fs.SetAsyncUpdate(false)
		data := make([]byte, 300<<10)
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(data)), Data: data}}, Policy: SkipDiskCache | SkipMemoryCache}))
		d := fs.diskCache
		d.objectSplit = objectSplitConfig{parts: 1}
		d.prefetched.store(d.objectKey("object"), int64(len(data)))
		require.Equal(t, tc.limit, d.conns.max, "conns %d", tc.conns)
		got, err := readFullObjectOf(fs, 100, 10)
		require.NoError(t, err)
		require.Equal(t, data[100:110], got)
		require.Equal(t, tc.download, d.cache.Contains(d.wholePath("object", int64(len(data)))), "conns %d", tc.conns)
		fs.Close(ctx)
	}
}

// More concurrent split downloads than the connections downloads may hold all cache their
// objects: each part holds a connection only while it reads, so no download holds one
// while it waits for another, and none stays held.
func TestS3FSObjectSplitDownloadsShareFewConnections(t *testing.T) {
	const (
		objects = 6
		size    = 300<<10 + 17
	)
	fs, _, _ := newPrefetchRangeFS(t, 0, 1)
	fs.diskCache.objectSplit = objectSplitConfig{parts: 2, min: 128 << 10}
	fs.diskCache.conns.max = 2
	ctx := context.Background()
	contents := make([][]byte, objects)
	for i := range objects {
		name := fmt.Sprintf("object-%d", i)
		contents[i] = make([]byte, size)
		for j := range contents[i] {
			contents[i][j] = byte(i + j*13)
		}
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: name, Entries: []IOEntry{{Size: size, Data: contents[i]}}, Policy: SkipDiskCache | SkipMemoryCache}))
		fs.diskCache.prefetched.store(fs.diskCache.objectKey(name), size)
	}
	errs := make(chan error, objects)
	for i := range objects {
		go func() {
			vec := &IOVector{FilePath: fmt.Sprintf("object-%d", i), Policy: SkipMemoryCache, Entries: []IOEntry{{Offset: 250 << 10, Size: 10}}}
			err := fs.Read(ctx, vec)
			if err == nil && !slices.Equal(contents[i][250<<10:250<<10+10], vec.Entries[0].Data) {
				err = errors.New("wrong bytes")
			}
			vec.Release()
			errs <- err
		}()
	}
	for range objects {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-time.After(diskCacheLifecycleTestTimeout):
			t.Fatal("a read did not return")
		}
	}
	require.Eventually(t, func() bool { return fs.diskCache.conns.held.Load() == 0 }, 5*time.Second, time.Millisecond)
}

// A read of bytes in a part of a split download that waits for a connection reads its own
// range: no read waits for a part holding no connection.
func TestS3FSReadOfPartWaitingForConnectionReadsItsRange(t *testing.T) {
	fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	d := fs.diskCache
	d.conns.max = 1
	key := fs.pathToKey("object")
	g := newBlackBoxStorage(fs.storage, func(k string, off int64) bool { return k == key && off == 0 })
	fs.storage = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	whole := make(chan error, 1)
	go func() {
		_, err := readFullObjectOf(fs, 100, 10)
		whole <- err
	}()
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond,
		"the first part holds the only connection")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{{Offset: 250 << 10, Size: 10}}}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, data[250<<10:250<<10+10], vec.Entries[0].Data)
	vec.Release()
	release()
	require.NoError(t, <-whole)
}

// A read that joins bytes at copy time takes no download whose part holding them has not
// begun, and takes it once the part begins.
func TestJoinableSkipsPartNotBegun(t *testing.T) {
	fs, _, _ := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	d := fs.diskCache
	diskPath := d.objectKey("object")
	p := newFilePrefetchObject(context.Background(), 0, 8)
	f := &d.prefetchObjects
	f.mu.Lock()
	f.registerLocked(diskPath, p)
	f.mu.Unlock()
	defer d.endFilePrefetch(diskPath, p)(nil)
	o := newObjectDownload("", false, []int64{0, 4, 8})
	o.begin(0)
	p.started(o)
	covering := func(lo, hi int64) prefetchObject {
		f.mu.Lock()
		defer f.mu.Unlock()
		return d.joinableLocked(diskPath, false, lo, hi, time.Now(), true)
	}
	require.NotNil(t, covering(1, 2), "the first part has begun")
	require.Nil(t, covering(5, 6), "the second part has not begun")
	o.begin(1)
	require.NotNil(t, covering(5, 6))
}

// A read whose async entry write waits for a publication slot returns once its object is
// deleted, or once a slot is freed.
func TestS3FSReadEntryWriteWaitingForPublicationSlotEndsOnDelete(t *testing.T) {
	for _, ev := range []string{"delete", "slot freed"} {
		t.Run(ev, func(t *testing.T) {
			fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
			fs.storage = keepOnDeleteStorage{storage}
			fs.SetAsyncUpdate(true)
			ctx := context.Background()
			a := &fs.diskCache.async
			filled := 0
			for len(a.slots) < cap(a.slots) {
				a.slots <- struct{}{}
				filled++
			}
			freeSlots := sync.OnceFunc(func() {
				a.mu.Lock()
				for range filled {
					<-a.slots
				}
				a.wakeFreedLocked()
				a.mu.Unlock()
			})
			t.Cleanup(freeSlots)
			type res struct {
				b   []byte
				err error
			}
			done := make(chan res, 1)
			go func() {
				vec := &IOVector{FilePath: "object", Policy: SkipMemoryCache | SkipFullFilePreloads, Entries: []IOEntry{{Offset: 100, Size: 10}}}
				err := fs.Read(ctx, vec)
				b := append([]byte(nil), vec.Entries[0].Data...)
				vec.Release()
				done <- res{b, err}
			}()
			require.Eventually(t, func() bool {
				a.mu.Lock()
				defer a.mu.Unlock()
				return a.mu.freed.ch != nil
			}, 5*time.Second, time.Millisecond, "the read's entry write waits for a slot")
			if ev == "delete" {
				require.NoError(t, fs.Delete(ctx, "object"))
			} else {
				freeSlots()
			}
			select {
			case r := <-done:
				require.NoError(t, r.err)
				require.Equal(t, data[100:110], r.b)
			case <-time.After(2 * time.Second):
				t.Fatalf("%s: the read still waits for a publication slot", ev)
			}
		})
	}
}
