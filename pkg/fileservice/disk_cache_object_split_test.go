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
	"slices"
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
	fs.diskCache.prefetched.store(fs.diskCache.pathForFile("object"), int64(size))
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
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))

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
		d.downloads.store(fmt.Sprintf("other-%d", i), newObjectDownload("", false, nil))
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
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))

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
	require.GreaterOrEqual(t, wait, rangeReadCosts.firstByte(), "a part not yet read is a latency away")
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

// A download releases its connections once its transfer ends, and a split that cannot
// reserve a connection for each part downloads the object as one range.
func TestS3FSDownloadConnsReservedPerPart(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	d := fs.diskCache
	d.maxDownloadConns = 2
	d.downloadConns.Store(1)
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Empty(t, storage.starts, "one open-ended range with one connection free")
	require.Equal(t, int64(1), d.downloadConns.Load(), "its connection is released")
	require.True(t, d.cache.Contains(d.pathForFile("object")))
}

// A download with no connection free waits for one, and downloads once one is released.
func TestS3FSDownloadWaitsForConn(t *testing.T) {
	fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	d := fs.diskCache
	d.maxDownloadConns = 1
	d.downloadConns.Store(1)
	done := make(chan []byte, 1)
	go func() {
		got, err := readFullObjectOf(fs, 100, 10)
		require.NoError(t, err)
		done <- got
	}()
	require.Eventually(t, func() bool {
		d.connsMu.Lock()
		defer d.connsMu.Unlock()
		return d.connsFreed != nil
	}, 10*time.Second, time.Millisecond, "the download waits for a connection")
	require.False(t, d.cache.Contains(d.pathForFile("object")))
	require.False(t, d.isUpdating(d.pathForFile("object")), "waiting for a connection holds no update of the path")
	d.releaseDownloadConns(1)
	require.Equal(t, data[100:110], <-done)
	require.True(t, d.cache.Contains(d.pathForFile("object")))
	require.Zero(t, d.downloadConns.Load())
}

// Deleting an object forgets the size a prefetch told of: after the path is written again
// at another length, a read to the end of it reads the new object whole.
func TestDeleteForgetsPrefetchedSize(t *testing.T) {
	fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Delete(ctx, "object"))
	_, known := fs.diskCache.prefetched.size(fs.diskCache.pathForFile("object"))
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
	taken := fs.prefetcher.entries["object"].req
	require.NoError(t, fs.Delete(ctx, "object"))
	require.False(t, fs.prefetcher.isQueued("object"), "the delete drops the queued prefetch")

	fs.prefetchObject(ctx, taken)
	_, known := fs.diskCache.prefetched.size(fs.diskCache.pathForFile("object"))
	require.False(t, known, "a prefetch made before the delete does not store its size")
}

// Whole-object downloads hold at most half the configured connections per host: with one
// connection they hold none, so a read reads its range; with two they hold one.
func TestS3FSConfiguredConnsBoundDownloads(t *testing.T) {
	for _, tc := range []struct {
		conns    int
		limit    int64
		download bool
	}{{1, 0, false}, {2, 1, true}} {
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
		d.prefetched.store(d.pathForFile("object"), int64(len(data)))
		require.Equal(t, tc.limit, d.maxDownloadConns, "conns %d", tc.conns)
		got, err := readFullObjectOf(fs, 100, 10)
		require.NoError(t, err)
		require.Equal(t, data[100:110], got)
		require.Equal(t, tc.download, d.cache.Contains(d.pathForFile("object")), "conns %d", tc.conns)
		fs.Close(ctx)
	}
}
