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
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

// downloadCountingStorage counts the reads of a whole object.
type downloadCountingStorage struct {
	ObjectStorage
	size      int64
	reads     atomic.Int64
	downloads atomic.Int64
	gate      chan struct{} // when set, a download waits for it to close
}

func (c *downloadCountingStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	c.reads.Add(1)
	if min != nil && *min == 0 && max != nil && *max == c.size {
		c.downloads.Add(1)
		if c.gate != nil {
			<-c.gate
		}
	}
	return c.ObjectStorage.Read(ctx, key, min, max)
}

func newPinFS(t *testing.T, indexCapacity int64, pin bool, size int) (*S3FS, *downloadCountingStorage, []byte) {
	ctx := context.Background()
	config := indexCacheConfig(t, 64<<20, indexCapacity)
	config.MemoryCapacity = ptrTo[toml.ByteSize](0)
	config.IndexFilePinObjects = pin
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, config, nil, false, false)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	// The disk cache caches exactly the entries read, so a read it serves is told apart.
	fs.diskCache.prefetchRange = prefetchRangeConfig{}
	t.Cleanup(func() { fs.Close(ctx) })
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + i/1021)
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(data)), Data: data}}, Policy: SkipAllCache}))
	storage := &downloadCountingStorage{ObjectStorage: fs.storage, size: int64(size)}
	fs.storage = storage
	return fs, storage, data
}

func readPinTestRange(t *testing.T, fs *S3FS, policy Policy, offset, size int64) []byte {
	t.Helper()
	vec := &IOVector{FilePath: "object", Policy: policy | SkipMemoryCache, Entries: []IOEntry{{Offset: offset, Size: size}}}
	require.NoError(t, fs.Read(context.Background(), vec))
	defer vec.Release()
	return slices.Clone(vec.Entries[0].Data)
}

func pinnedObject(fs *S3FS, file string) bool {
	diskPath, err := fs.indexFileCache.disk.pathForDir(pinnedObjectEntry(file))
	return err == nil && fs.indexFileCache.disk.cache.Contains(diskPath)
}

func waitPinned(t *testing.T, fs *S3FS) {
	t.Helper()
	require.Eventually(t, func() bool { return pinnedObject(fs, "object") }, 10*time.Second, 5*time.Millisecond)
}

// A read of an index-file object pins it in the background, and later reads of it are
// served from the pinned copy without object storage.
func TestPinnedObjectServesReads(t *testing.T) {
	fs, storage, data := newPinFS(t, 64<<20, true, 300<<10)
	require.Equal(t, data[1000:1010], readPinTestRange(t, fs, IndexFileObject, 1000, 10))
	waitPinned(t, fs)
	require.Equal(t, int64(1), storage.downloads.Load())

	reads := storage.reads.Load()
	require.Equal(t, data[200<<10:200<<10+100], readPinTestRange(t, fs, IndexFileObject, 200<<10, 100))
	require.Equal(t, data[5:6], readPinTestRange(t, fs, IndexFileObject|SkipFullFilePreloads, 5, 1))
	require.Equal(t, reads, storage.reads.Load(), "pinned reads do not read object storage")
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")),
		"an object being pinned is not also downloaded whole into the disk cache")

	readPinTestRange(t, fs, 0, 250<<10, 10)
	require.Greater(t, storage.reads.Load(), reads, "a read without IndexFileObject does not use the pinned copy")
}

// A whole-object prefetch of an index-file object pins it instead of downloading it into
// the disk cache.
func TestPrefetchOfIndexFileObjectPinsIt(t *testing.T) {
	fs, storage, _ := newPinFS(t, 64<<20, true, 300<<10)
	ctx := WithFileServicePolicy(context.Background(), IndexFileObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 300 << 10}))
	waitPinned(t, fs)
	require.Equal(t, int64(1), storage.downloads.Load())
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
}

// Without the setting, an index-file object read is not pinned.
func TestPinnedObjectsOff(t *testing.T) {
	fs, storage, data := newPinFS(t, 64<<20, false, 300<<10)
	require.Equal(t, data[1000:1010], readPinTestRange(t, fs, IndexFileObject, 1000, 10))
	require.Nil(t, fs.pinned.queue, "no pin is queued")
	require.False(t, pinnedObject(fs, "object"))
	require.Zero(t, storage.downloads.Load())
}

// An object is pinned only into free room: past it, it is not pinned and not queued again
// within the retry time.
func TestPinnedObjectNeedsFreeRoom(t *testing.T) {
	fs, storage, data := newPinFS(t, 200<<10, true, 300<<10)
	require.Equal(t, data[1000:1010], readPinTestRange(t, fs, IndexFileObject, 1000, 10))
	require.Eventually(t, func() bool {
		o := &fs.pinned
		o.mu.Lock()
		defer o.mu.Unlock()
		return !o.retry["object"].IsZero()
	}, 10*time.Second, 5*time.Millisecond)
	require.False(t, pinnedObject(fs, "object"))
	require.Zero(t, storage.downloads.Load())
	require.False(t, fs.pinRoute("object", 0), "an object not pinned is not queued again within the retry time")
	fs.pinned.mu.Lock()
	_, queued := fs.pinned.pending["object"]
	fs.pinned.mu.Unlock()
	require.False(t, queued)

	// It keeps the disk cache's whole-object read and prefetch.
	require.Equal(t, data[2000:2010], readPinTestRange(t, fs, IndexFileObject, 2000, 10))
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")),
		"an object not pinned is read whole into the disk cache")
	require.False(t, FullFilePreloadSkipped(context.Background(), fs, IndexFileObject, "object"))
}

// An object the cache has no room for, by its known size, is prefetched into the disk
// cache and not queued for a pin.
func TestPrefetchOfUnpinnableObjectUsesDiskCache(t *testing.T) {
	fs, storage, _ := newPinFS(t, 200<<10, true, 300<<10)
	ctx := WithFileServicePolicy(context.Background(), IndexFileObject)
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 300 << 10}))
	require.True(t, fs.prefetcher.isQueued("object"))
	fs.pinned.mu.Lock()
	_, queued := fs.pinned.pending["object"]
	fs.pinned.mu.Unlock()
	require.False(t, queued)
	require.Zero(t, storage.downloads.Load())
}

// The preload gate of both prefetch paths skips the disk cache's whole-object preload of
// an object it queues for a pin.
func TestFullFilePreloadSkippedQueuesPin(t *testing.T) {
	fs, storage, _ := newPinFS(t, 64<<20, true, 300<<10)
	require.False(t, FullFilePreloadSkipped(context.Background(), fs, 0, "object"))
	require.True(t, FullFilePreloadSkipped(context.Background(), fs, IndexFileObject, "object"))
	waitPinned(t, fs)
	require.Equal(t, int64(1), storage.downloads.Load())
}

// A pinned object read stays past the idle time; one not read is collected.
func TestReadKeepsPinnedObject(t *testing.T) {
	for _, read := range []bool{false, true} {
		fs, _, data := newPinFS(t, 64<<20, true, 300<<10)
		readPinTestRange(t, fs, IndexFileObject, 1000, 10)
		waitPinned(t, fs)
		c := fs.indexFileCache
		diskPath, err := c.disk.pathForDir(pinnedObjectEntry("object"))
		require.NoError(t, err)
		old := time.Now().Add(-2 * time.Hour)
		require.NoError(t, os.Chtimes(diskPath, old, old))
		fs.pinned.mu.Lock()
		clear(fs.pinned.touched)
		fs.pinned.mu.Unlock()
		if read {
			require.Equal(t, data[5000:5010], readPinTestRange(t, fs, IndexFileObject, 5000, 10))
		}
		c.idleTTL = time.Hour
		_, err = c.collectIdle(context.Background(), time.Now())
		require.NoError(t, err)
		require.Equal(t, read, pinnedObject(fs, "object"), "read %v", read)
	}
}

// A pin takes connections of the disk cache's download budget: with none left it does not
// download.
func TestPinNeedsDownloadConnections(t *testing.T) {
	fs, storage, _ := newPinFS(t, 64<<20, true, 300<<10)
	d := fs.diskCache
	d.downloadConns.Store(d.downloadConnLimit())
	require.False(t, fs.pinObject(context.Background(), "object"))
	require.Zero(t, storage.downloads.Load())
	d.downloadConns.Store(0)
	require.True(t, fs.pinObject(context.Background(), "object"))
	require.Zero(t, d.downloadConns.Load(), "the connections are released")
}

// A delete drops a pin queued and not started without waiting for it, and close releases
// the pins left queued.
func TestUnpinDropsQueuedPin(t *testing.T) {
	fs, _, _ := newPinFS(t, 64<<20, true, 300<<10)
	o := &fs.pinned
	o.start.Do(func() {
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.pending = make(map[string]*pinJob)
		o.retry = make(map[string]time.Time)
		o.queue = make(chan string, 1)
	})
	queued := &pinJob{done: make(chan struct{})}
	left := &pinJob{done: make(chan struct{})}
	o.pending["object"], o.pending["other"] = queued, left
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, fs.Delete(ctx, "object"))
	require.NotContains(t, o.pending, "object")
	<-queued.done
	o.close()
	<-left.done
}

// Concurrent reads of an object download it once.
func TestPinnedObjectDownloadsOnce(t *testing.T) {
	fs, storage, data := newPinFS(t, 64<<20, true, 300<<10)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			off := int64(i * 1000)
			require.Equal(t, data[off:off+10], readPinTestRange(t, fs, IndexFileObject, off, 10))
		}()
	}
	wg.Wait()
	waitPinned(t, fs)
	require.Equal(t, int64(1), storage.downloads.Load())
}

// Deleting an object deletes its pinned copy, and a pin under way when it is deleted
// does not publish it: after the object is written again, reads return the new bytes.
func TestDeleteUnpinsObject(t *testing.T) {
	for _, underWay := range []bool{false, true} {
		fs, storage, data := newPinFS(t, 64<<20, true, 300<<10)
		ctx := context.Background()
		if underWay {
			storage.gate = make(chan struct{})
		}
		require.Equal(t, data[1000:1010], readPinTestRange(t, fs, IndexFileObject, 1000, 10))
		if underWay {
			require.Eventually(t, func() bool { return storage.downloads.Load() == 1 }, 10*time.Second, time.Millisecond)
			deleted := make(chan error, 1)
			go func() { deleted <- fs.Delete(ctx, "object") }()
			select {
			case err := <-deleted:
				t.Fatalf("the delete returned before the pin under way ended: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			close(storage.gate)
			require.NoError(t, <-deleted)
		} else {
			waitPinned(t, fs)
			require.NoError(t, fs.Delete(ctx, "object"))
		}
		require.False(t, pinnedObject(fs, "object"), "under way %v", underWay)

		recreated := slices.Clone(data)
		for i := range recreated {
			recreated[i] ^= 0xff
		}
		storage.gate = nil
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(recreated)), Data: recreated}}, Policy: SkipAllCache}))
		require.Equal(t, recreated[2000:2010], readPinTestRange(t, fs, IndexFileObject|SkipFullFilePreloads, 2000, 10), "under way %v", underWay)
	}
}

// A large object is pinned by reading its parts at once, each written at its offset.
func TestPinnedObjectDownloadsParts(t *testing.T) {
	size := pinnedObjectPartMin + 3<<10
	fs, storage, data := newPinFS(t, 64<<20, true, size)
	ctx := WithFileServicePolicy(context.Background(), IndexFileObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(size)}))
	waitPinned(t, fs)
	require.Equal(t, int64(pinnedObjectParts), storage.reads.Load(), "one read per part")
	part := (size + pinnedObjectParts - 1) / pinnedObjectParts
	for _, off := range []int{0, part - 5, part, 2*part + 77, size - 10} {
		require.Equal(t, data[off:off+10], readPinTestRange(t, fs, IndexFileObject, int64(off), 10), "offset %d", off)
	}
	require.Equal(t, int64(pinnedObjectParts), storage.reads.Load(), "pinned reads do not read object storage")
}
