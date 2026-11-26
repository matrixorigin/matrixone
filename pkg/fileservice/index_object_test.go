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
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

type readCountingStorage struct {
	ObjectStorage
	reads atomic.Int64
}

func (c *readCountingStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	c.reads.Add(1)
	return c.ObjectStorage.Read(ctx, key, min, max)
}

func newIndexObjectFS(t *testing.T, indexCache bool) (*S3FS, *readCountingStorage, []byte) {
	ctx := context.Background()
	config := indexCacheConfig(t, 64<<20, 64<<20)
	config.MemoryCapacity = ptrTo[toml.ByteSize](0)
	config.IndexFileCacheEnabled = indexCache
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: defines.SharedFileServiceName, Endpoint: "disk", Bucket: t.TempDir()}, config, nil, false, false)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	<-fs.diskCache.load.done
	if fs.indexFileCache != nil {
		<-fs.indexFileCache.disk.load.done
	}
	// The disk cache caches exactly the entries read, so a read it serves is told apart.
	fs.diskCache.prefetchRange = prefetchRangeConfig{}
	t.Cleanup(func() { fs.Close(ctx) })
	data := make([]byte, 300<<10)
	for i := range data {
		data[i] = byte(i*7 + i/1021)
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(data)), Data: data}}, Policy: SkipAllCache}))
	storage := &readCountingStorage{ObjectStorage: fs.server.raw}
	fs.server.raw = storage
	return fs, storage, data
}

// cacheIndexObject puts data as the entry of the object at file.
func cacheIndexObject(t *testing.T, fs *S3FS, file string, data []byte) {
	_, err := fs.indexFileCache.Put(context.Background(), IndexObjectEntry(file), []IndexCacheFile{{
		Name: IndexObjectFile,
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil },
	}})
	require.NoError(t, err)
}

func readIndexObjectRange(t *testing.T, fs *S3FS, policy Policy, offset, size int64) []byte {
	t.Helper()
	vec := &IOVector{FilePath: "object", Policy: policy | SkipMemoryCache, Entries: []IOEntry{{Offset: offset, Size: size}}}
	require.NoError(t, fs.Read(context.Background(), vec))
	defer vec.Release()
	return slices.Clone(vec.Entries[0].Data)
}

// IndexFileObject reads of an object the index-file cache holds are served from there.
func TestIndexObjectServesReads(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	cacheIndexObject(t, fs, "object", data)
	require.Equal(t, data[1000:1010], readIndexObjectRange(t, fs, IndexFileObject, 1000, 10))
	require.Equal(t, data[200<<10:200<<10+100], readIndexObjectRange(t, fs, IndexFileObject|SkipFullFilePreloads, 200<<10, 100))
	require.Equal(t, data[2000:2010], readIndexObjectRange(t, fs, IndexFileObject|SkipDiskCacheReads, 2000, 10), "a skip of disk-cache reads does not apply to the index-file cache")
	require.Zero(t, storage.reads.Load())
	require.Equal(t, data[3000:3010], readIndexObjectRange(t, fs, IndexFileObject|SkipAllCache, 3000, 10))
	require.NotZero(t, storage.reads.Load(), "a read that skips every cache skips the index-file cache")
	storage.reads.Store(0)
	require.Empty(t, fs.diskCache.objectFiles("object"))

	readIndexObjectRange(t, fs, 0, 250<<10, 10)
	require.NotZero(t, storage.reads.Load(), "a read without IndexFileObject does not use the entry")
}

// A decoded IndexFileObject read served from the index-file cache with no memory cache
// gets its cache data from the default allocator.
func TestIndexObjectDecodedReadWithoutMemoryCache(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	require.Nil(t, fs.memCache)
	cacheIndexObject(t, fs, "object", data)
	vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipMemoryCache,
		Entries: []IOEntry{{Offset: 1000, Size: 10, ToCacheData: CacheOriginalData}}}
	require.NoError(t, fs.Read(context.Background(), vec))
	defer vec.Release()
	require.NotNil(t, vec.Entries[0].CachedData)
	require.Equal(t, data[1000:1010], vec.Entries[0].CachedData.Bytes())
	require.Zero(t, storage.reads.Load())
}

// Without an index-file cache, IndexFileObject reads take the normal path.
func TestIndexObjectsWithoutIndexFileCache(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, false)
	require.Nil(t, fs.indexFileCache)
	require.Equal(t, data[1000:1010], readIndexObjectRange(t, fs, IndexFileObject, 1000, 10))
	require.NotZero(t, storage.reads.Load())
	cache, _ := IndexObjectCache(fs)
	require.Nil(t, cache)
}

// A read of an object the cache does not hold takes the normal path.
func TestIndexObjectMissReadsStorage(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	require.Equal(t, data[1000:1010], readIndexObjectRange(t, fs, IndexFileObject, 1000, 10))
	require.NotZero(t, storage.reads.Load())
	cache, shared := IndexObjectCache(fs)
	require.Same(t, fs.indexFileCache, cache)
	require.Same(t, FileService(fs), shared)
}

// The prefetches of an object the index-file cache holds, through a queued fill or the
// IO pipeline, are not downloaded into the disk cache; those of one it does not hold are.
func TestPrefetchOfCachedIndexObjectIsSkipped(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	ctx := WithFileServicePolicy(context.Background(), IndexFileObject)
	busyFills(t, fs)
	require.False(t, FullFilePreloadSkipped(ctx, fs, IndexFileObject, "object"))
	require.False(t, FullFilePreloadSkipped(ctx, fs, IndexFileObject|SkipFullFilePreloads|NarrowRead, "object"),
		"a filtered search still preloads an object the index-file cache keeps")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
	require.True(t, fs.diskCache.isQueued("object"))
	fs.diskCache.dropFills("object")

	cacheIndexObject(t, fs, "object", data)
	require.True(t, FullFilePreloadSkipped(ctx, fs, IndexFileObject, "object"))
	require.False(t, FullFilePreloadSkipped(ctx, fs, 0, "object"))
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
	require.False(t, fs.diskCache.isQueued("object"))
}

// Deleting an object deletes its entry; after it is written again, reads return the new
// bytes.
func TestDeleteRemovesIndexObject(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	ctx := context.Background()
	cacheIndexObject(t, fs, "object", data)
	require.NoError(t, fs.Delete(ctx, "object"))
	_, cached := fs.indexObjectCached("object")
	require.False(t, cached)
	recreated := slices.Clone(data)
	for i := range recreated {
		recreated[i] ^= 0xff
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(recreated)), Data: recreated}}, Policy: SkipAllCache}))
	require.Equal(t, recreated[2000:2010], readIndexObjectRange(t, fs, IndexFileObject, 2000, 10))
}

// A read of an entry marks it used, so idle collection keeps it; one not read is
// collected.
func TestReadKeepsIndexObject(t *testing.T) {
	for _, read := range []bool{false, true} {
		fs, _, data := newIndexObjectFS(t, true)
		cacheIndexObject(t, fs, "object", data)
		diskPath, cached := fs.indexObjectCached("object")
		require.True(t, cached)
		old := time.Now().Add(-2 * time.Hour)
		require.NoError(t, os.Chtimes(diskPath, old, old))
		if read {
			require.Equal(t, data[5000:5010], readIndexObjectRange(t, fs, IndexFileObject, 5000, 10))
		}
		c := fs.indexFileCache
		c.idleTTL = time.Hour
		_, err := c.collectIdle(context.Background(), time.Now())
		require.NoError(t, err)
		_, cached = fs.indexObjectCached("object")
		require.Equal(t, read, cached, "read %v", read)
	}
}

// A whole-object fill of an IndexFileObject read publishes the object into the index-file
// cache, not the disk cache; later reads are served from there without storage reads.
func TestIndexObjectReadFillsIndexFileCache(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	require.NoError(t, fs.PrefetchFile(WithFileServicePolicy(context.Background(), IndexFileObject), "object"))
	diskPath, cached := fs.indexObjectCached("object")
	require.True(t, cached)
	got, err := os.ReadFile(filepath.Join(diskPath, IndexObjectFile))
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Empty(t, fs.diskCache.objectFiles("object"), "not in the disk cache")

	reads := storage.reads.Load()
	require.Equal(t, data[7000:7010], readIndexObjectRange(t, fs, IndexFileObject, 7000, 10))
	require.Equal(t, reads, storage.reads.Load())
}

// A fill without IndexFileObject publishes into the disk cache as before.
func TestNonIndexObjectFillUsesDiskCache(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	require.NoError(t, fs.PrefetchFile(context.Background(), "object"))
	_, cached := fs.indexObjectCached("object")
	require.False(t, cached)
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))))
}

// A queued whole-object prefetch of an IndexFileObject request publishes into the
// index-file cache, though its fill runs without the request's context.
func TestIndexObjectPrefetchFillsIndexFileCache(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	busyFills(t, fs)
	ctx := WithFileServicePolicy(context.Background(), IndexFileObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
	f := &fs.diskCache.prefetchObjects
	f.mu.Lock()
	req := f.fills[fillKey{diskPath: fs.diskCache.objectKey("object"), index: true}][0].req
	f.mu.Unlock()
	require.True(t, req.indexObject)
	fs.diskCache.dropFills("object")
	fs.diskCache.setFillsMax(0)

	require.False(t, fs.haveObject(req))
	fs.prefetchObject(context.Background(), req)
	_, cached := fs.indexObjectCached("object")
	require.True(t, cached)
	require.Empty(t, fs.diskCache.objectFiles("object"))
	require.True(t, fs.haveObject(req))
}

// The fill of an object deleted after its download started publishes nothing, and its
// file is removed.
func TestIndexObjectFillOfDeletedObjectIsNotPublished(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	target := fs.fillTarget(IndexFileObject, "object").(indexFillTarget)
	require.NoError(t, fs.Delete(context.Background(), "object"))
	tempPath := t.TempDir() + "/object.tmp"
	require.NoError(t, os.WriteFile(tempPath, data, 0o644))
	require.NoError(t, target.publish(context.Background(), tempPath, int64(len(data))))
	_, cached := fs.indexObjectCached("object")
	require.False(t, cached)
	_, err := os.Stat(tempPath)
	require.True(t, os.IsNotExist(err))
}

// A miss read under IndexFileObject that reads the object whole streams it into the
// index-file cache and returns its bytes.
func TestIndexObjectMissReadStreamsIntoIndexFileCache(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	require.Equal(t, data[1000:1010], readIndexObjectRange(t, fs, IndexFileObject, 1000, 10))
	_, cached := fs.indexObjectCached("object")
	require.True(t, cached)
	require.Empty(t, fs.diskCache.objectFiles("object"))
	reads := storage.reads.Load()
	require.Equal(t, data[9000:9010], readIndexObjectRange(t, fs, IndexFileObject, 9000, 10))
	require.Equal(t, reads, storage.reads.Load())
}

// Publishing copies the file when it cannot be moved into the index-file cache, and the
// file written is removed either way.
func TestIndexObjectPublishRemovesWrittenFile(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	target := fs.fillTarget(IndexFileObject, "object").(indexFillTarget)
	tempPath := filepath.Join(t.TempDir(), "object.tmp")
	require.NoError(t, os.WriteFile(tempPath, data, 0o644))
	require.NoError(t, target.publish(context.Background(), tempPath, int64(len(data))))
	diskPath, cached := fs.indexObjectCached("object")
	require.True(t, cached)
	got, err := os.ReadFile(filepath.Join(diskPath, IndexObjectFile))
	require.NoError(t, err)
	require.Equal(t, data, got)
	_, err = os.Stat(tempPath)
	require.True(t, os.IsNotExist(err))
}

// An asynchronous finalize of a fill publishes into the index-file cache once the disk
// cache's updates are flushed.
func TestIndexObjectAsyncFillPublishes(t *testing.T) {
	fs, _, _ := newIndexObjectFS(t, true)
	fs.SetAsyncUpdate(true)
	require.NoError(t, fs.PrefetchFile(WithFileServicePolicy(context.Background(), IndexFileObject), "object"))
	fs.FlushCache(context.Background())
	_, cached := fs.indexObjectCached("object")
	require.True(t, cached)
	require.Empty(t, fs.diskCache.objectFiles("object"))
}

// An index-object fill writes its file in the index-file cache's directory, so publishing
// it never copies it across file systems, and a fill leaves no temp file in either cache.
func TestIndexObjectFillWritesInIndexFileCache(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	f, err := fs.fillTarget(IndexFileObject, "object").createTemp()
	require.NoError(t, err)
	require.Equal(t, fs.indexFileCache.Path(), filepath.Dir(f.Name()))
	require.NoError(t, cleanupDiskCacheTempFile(f))

	var published []string
	objectPath := fs.diskCache.objectKey("object")
	fs.diskCache.hooks.beforeFilePublication = func() {
		if o := fs.diskCache.download(objectPath); o != nil {
			o.mu.Lock()
			if o.file != nil {
				published = append(published, o.file.Name())
			}
			o.mu.Unlock()
		}
	}
	require.NoError(t, fs.PrefetchFile(WithFileServicePolicy(context.Background(), IndexFileObject), "object"))
	require.NotEmpty(t, published)
	for _, p := range published {
		require.Equal(t, fs.indexFileCache.Path(), filepath.Dir(p))
	}
	diskPath, cached := fs.indexObjectCached("object")
	require.True(t, cached)
	got, err := os.ReadFile(filepath.Join(diskPath, IndexObjectFile))
	require.NoError(t, err)
	require.Equal(t, data, got)
	for _, dir := range []string{fs.indexFileCache.Path(), fs.diskCache.path} {
		require.NoError(t, filepath.WalkDir(dir, func(path string, d iofs.DirEntry, err error) error {
			require.NoError(t, err)
			require.False(t, strings.HasSuffix(path, cacheFileTempSuffix), "temp file left: %s", path)
			return nil
		}))
	}
}

// A read of an index object keeps the entry's file open for the reads after it; a delete
// of the object closes it.
func TestIndexObjectReadKeepsFileOpen(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	d := fs.indexFileCache.disk
	// The load of the cache owns the entries it indexes, which closes their kept files.
	<-d.load.done
	cacheIndexObject(t, fs, "object", data)
	diskPath, err := d.pathForDir(IndexObjectEntry("object"))
	require.NoError(t, err)
	kept := func() *openCacheFile {
		d.paths.L.Lock()
		defer d.paths.L.Unlock()
		return d.paths.openFiles.m[diskPath]
	}

	require.Equal(t, data[1000:1010], readIndexObjectRange(t, fs, IndexFileObject, 1000, 10))
	first := kept()
	require.NotNil(t, first)
	require.Zero(t, first.refs)
	require.Equal(t, data[5000:5010], readIndexObjectRange(t, fs, IndexFileObject, 5000, 10))
	require.Same(t, first, kept(), "the next read reads the kept file")

	require.NoError(t, fs.Delete(context.Background(), "object"))
	require.Nil(t, kept())
	_, err = first.file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
}

// An IndexFileObject read of an object the index-file cache holds coordinates its shared
// decode as a read of an object the disk cache holds does; a read without the policy, or
// of an object not held, does not.
func TestIndexObjectSharedDecodeFillEligibility(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	vec := func(policy Policy) *IOVector {
		return &IOVector{FilePath: "object", Policy: policy, Entries: []IOEntry{{Offset: 0, Size: 10}}}
	}
	require.False(t, fs.sharedDecodeIndexFillAvailable(vec(IndexFileObject)))
	cacheIndexObject(t, fs, "object", data)
	require.True(t, fs.sharedDecodeIndexFillAvailable(vec(IndexFileObject)))
	require.False(t, fs.sharedDecodeIndexFillAvailable(vec(0)))
}

// HoldCached holds a cached entry until the hold is released, and holds nothing for an
// entry the cache does not hold.
func TestIndexFileCacheHoldCached(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	d := fs.indexFileCache.disk
	name := IndexObjectEntry("object")
	diskPath, err := d.pathForDir(name)
	require.NoError(t, err)
	holds := func() int {
		d.held.mu.Lock()
		defer d.held.mu.Unlock()
		return d.held.m[diskPath]
	}
	require.Nil(t, fs.indexFileCache.HoldCached(name))
	require.Zero(t, holds())

	cacheIndexObject(t, fs, "object", data)
	hold := fs.indexFileCache.HoldCached(name)
	require.NotNil(t, hold)
	require.Equal(t, 1, holds())
	hold.Release()
	require.Zero(t, holds())
}

// A read of an entry whose cached file ends before the read's range drops the entry and
// reads storage, which fills it again; an error of the read's decoding is returned.
func TestIndexObjectShortCacheFileReadsStorage(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	cacheIndexObject(t, fs, "object", data[:1000])
	require.Equal(t, data[2000:2010], readIndexObjectRange(t, fs, IndexFileObject, 2000, 10))
	require.Equal(t, int64(1), storage.reads.Load())
	require.Equal(t, data[len(data)-10:], readIndexObjectRange(t, fs, IndexFileObject, int64(len(data)-10), 10))
	require.Equal(t, int64(1), storage.reads.Load(), "the short entry is replaced by the object the read filled")

	cacheIndexObject(t, fs, "object", data)
	storage.reads.Store(0)
	decodeErr := errors.New("decode")
	vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipMemoryCache, Entries: []IOEntry{{Offset: 1000, Size: 10,
		ToCacheData: func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error) {
			return nil, decodeErr
		}}}}
	require.ErrorIs(t, fs.Read(context.Background(), vec), decodeErr)
	vec.Release()
	require.Zero(t, storage.reads.Load())
	_, cached := fs.indexObjectCached("object")
	require.True(t, cached, "a decoding error keeps the entry")
}

// A reader that found the entry short drops only the file it read: a healthy entry that
// another read filled meanwhile is kept.
func TestIndexObjectShortReaderKeepsRefill(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	cacheIndexObject(t, fs, "object", data[:1000])
	started := make(chan struct{})
	resume := make(chan struct{})
	readErr := make(chan error, 1)
	go func() {
		vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipMemoryCache, Entries: []IOEntry{
			{Offset: 100, Size: 10, ToCacheData: func(ctx context.Context, r io.Reader, b []byte, a CacheDataAllocator) (fscache.Data, error) {
				close(started)
				<-resume
				return CacheOriginalData(ctx, r, b, a)
			}},
			{Offset: 2000, Size: 10},
		}}
		readErr <- fs.Read(context.Background(), vec)
		vec.Release()
	}()
	<-started
	require.Equal(t, data[2000:2010], readIndexObjectRange(t, fs, IndexFileObject, 2000, 10))
	var healthy os.FileInfo
	require.Eventually(t, func() bool {
		diskPath, ok := fs.indexObjectCached("object")
		if !ok {
			return false
		}
		fi, err := os.Stat(filepath.Join(diskPath, IndexObjectFile))
		healthy = fi
		return err == nil && fi.Size() == int64(len(data))
	}, 5*time.Second, time.Millisecond)
	close(resume)
	require.NoError(t, <-readErr)
	diskPath, ok := fs.indexObjectCached("object")
	require.True(t, ok)
	fi, err := os.Stat(filepath.Join(diskPath, IndexObjectFile))
	require.NoError(t, err)
	require.True(t, os.SameFile(healthy, fi), "the healthy entry is kept")
	storage.reads.Store(0)
	require.Equal(t, data[len(data)-10:], readIndexObjectRange(t, fs, IndexFileObject, int64(len(data)-10), 10))
	require.Zero(t, storage.reads.Load())
}

// An open failing for lack of resources keeps the entry; other failures drop it.
func TestIndexObjectOpenResourcesExhausted(t *testing.T) {
	for _, err := range []error{syscall.EMFILE, syscall.ENFILE, syscall.ENOMEM, syscall.EAGAIN,
		&os.PathError{Op: "open", Path: "x", Err: syscall.EMFILE}} {
		require.True(t, openResourcesExhausted(err), err)
	}
	for _, err := range []error{syscall.ENOTDIR, syscall.ELOOP, syscall.EACCES, syscall.EIO,
		&os.PathError{Op: "open", Path: "x", Err: syscall.ENOTDIR}} {
		require.False(t, openResourcesExhausted(err), err)
	}
}

// A deleted entry whose files cannot be removed is gone, its files count against the
// capacity, listed or not, and the collector removes them once it can.
func TestIndexFileCacheTrashRemovedOnceItCanBe(t *testing.T) {
	for _, mode := range []os.FileMode{0o500, 0o000, 0o755} {
		fault := mode != 0o755
		t.Run(mode.String(), func(t *testing.T) {
			fs, _, data := newIndexObjectFS(t, true)
			cacheIndexObject(t, fs, "object", data)
			diskPath, ok := fs.indexObjectCached("object")
			require.True(t, ok)
			d := fs.indexFileCache.disk
			capacity := d.capacityFunc()
			trashDirs := func() []string {
				matches, err := filepath.Glob(diskPath + ".*" + cacheDirTrashSuffix)
				require.NoError(t, err)
				return matches
			}
			if fault {
				require.NoError(t, os.Chmod(diskPath, mode))
				t.Cleanup(func() {
					for _, m := range trashDirs() {
						_ = os.Chmod(m, 0o755)
					}
				})
				if f, err := os.Create(filepath.Join(diskPath, "probe")); err == nil {
					_ = f.Close()
					t.Skip("permissions are not enforced")
				}
			}
			require.NoError(t, fs.indexFileCache.Delete(context.Background(), IndexObjectEntry("object")))
			_, err := os.Stat(diskPath)
			require.True(t, os.IsNotExist(err))
			_, ok = fs.indexObjectCached("object")
			require.False(t, ok)
			if !fault {
				require.Empty(t, trashDirs())
				require.Equal(t, capacity, d.capacityFunc())
				return
			}
			trash := trashDirs()
			require.Len(t, trash, 1)
			require.NoError(t, os.Chmod(trash[0], 0o755))
			size, err := cacheDirSize(trash[0])
			require.NoError(t, err)
			require.Equal(t, int64(len(data)), size, "the files are kept")
			require.NoError(t, os.Chmod(trash[0], mode))
			require.Equal(t, int64(len(data)), d.trash.bytes.Load())
			require.Equal(t, capacity-int64(len(data)), d.capacityFunc())
			d.retryTrash()
			require.Len(t, trashDirs(), 1, "the fault holds")
			require.Equal(t, int64(len(data)), d.trash.bytes.Load())
			require.NoError(t, os.Chmod(trash[0], 0o755))
			d.retryTrash()
			require.Empty(t, trashDirs())
			require.Zero(t, d.trash.bytes.Load())
			require.Equal(t, capacity, d.capacityFunc())
		})
	}
}

// A read of a short entry repairs only the file it read: an entry another cache instance
// on the directory published meanwhile is kept.
func TestIndexObjectStaleShortRepairKeepsOtherInstanceEntry(t *testing.T) {
	for _, replaced := range []bool{true, false} {
		t.Run(map[bool]string{true: "replaced", false: "control"}[replaced], func(t *testing.T) {
			ctx := context.Background()
			fs, storage, data := newIndexObjectFS(t, true)
			name := IndexObjectEntry("object")
			cacheIndexObject(t, fs, "object", data[:1000])
			diskPath, ok := fs.indexObjectCached("object")
			require.True(t, ok)
			other, err := newIndexFileCache(ctx, fs.indexFileCache.disk.path, fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
			require.NoError(t, err)
			t.Cleanup(func() { other.Close(ctx) })
			require.Equal(t, data[10:20], readIndexObjectRange(t, fs, IndexFileObject, 10, 10))
			require.Zero(t, storage.reads.Load())
			var replacement os.FileInfo
			if replaced {
				require.NoError(t, other.Delete(ctx, name))
				written, err := other.Put(ctx, name, []IndexCacheFile{{Name: IndexObjectFile,
					Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }}})
				require.NoError(t, err)
				require.True(t, written)
				replacement, err = os.Stat(filepath.Join(diskPath, IndexObjectFile))
				require.NoError(t, err)
			}
			require.Equal(t, data[2000:2010], readIndexObjectRange(t, fs, IndexFileObject, 2000, 10))
			info, err := os.Stat(filepath.Join(diskPath, IndexObjectFile))
			require.NoError(t, err)
			require.Equal(t, int64(len(data)), info.Size())
			if replaced {
				require.True(t, os.SameFile(replacement, info), "the other instance's entry was dropped")
			}
		})
	}
}

// Trash a removal failed to remove before a restart counts against the capacity of the
// cache loaded after it, and the collector removes it once it can.
func TestIndexFileCacheTrashChargedAfterRestart(t *testing.T) {
	for _, mode := range []os.FileMode{0o500, 0o000, 0o755} {
		fault := mode != 0o755
		t.Run(mode.String(), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			data := bytes.Repeat([]byte{7}, 300<<10)
			name := "entry"
			c1, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
			require.NoError(t, err)
			_, err = c1.Put(ctx, name, []IndexCacheFile{{Name: "data",
				Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }}})
			require.NoError(t, err)
			diskPath, err := c1.disk.pathForDir(name)
			require.NoError(t, err)
			trashDirs := func() []string {
				m, err := filepath.Glob(diskPath + ".*" + cacheDirTrashSuffix)
				require.NoError(t, err)
				return m
			}
			t.Cleanup(func() {
				for _, m := range trashDirs() {
					_ = os.Chmod(m, 0o755)
				}
			})
			if fault {
				require.NoError(t, os.Chmod(diskPath, mode))
				if f, err := os.Create(filepath.Join(diskPath, "probe")); err == nil {
					_ = f.Close()
					t.Skip("permissions are not enforced")
				}
			}
			require.NoError(t, c1.Delete(ctx, name))
			c1.Close(ctx)
			c2, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
			require.NoError(t, err)
			defer c2.Close(ctx)
			<-c2.disk.load.done
			if !fault {
				require.Empty(t, trashDirs())
				require.Zero(t, c2.disk.trash.bytes.Load())
				return
			}
			require.Len(t, trashDirs(), 1)
			require.Equal(t, int64(len(data)), c2.disk.trash.bytes.Load())
			require.NoError(t, os.Chmod(trashDirs()[0], 0o755))
			c2.disk.retryTrash()
			require.Empty(t, trashDirs())
			require.Zero(t, c2.disk.trash.bytes.Load())
		})
	}
}

// Trash of bytes a removal cannot measure counts against the capacity after a restart:
// with the bytes the load measured when the entry is removed while the cache loads, or,
// when its bytes are unknown, by holding the cache at what it used, until it is removed.
func TestIndexFileCacheTrashOfUnknownSize(t *testing.T) {
	for _, by := range []string{"deleted while loading", "old trash", "unlistable entry", "control"} {
		t.Run(by, func(t *testing.T) {
			if by != "control" && !permissionsEnforced(t) {
				t.Skip("permissions are not enforced")
			}
			ctx := context.Background()
			dir := t.TempDir()
			data := bytes.Repeat([]byte{7}, 300<<10)
			capacity := int64(64 << 20)
			put := func(c *IndexFileCache, name string) {
				t.Helper()
				written, err := c.Put(ctx, name, []IndexCacheFile{{Name: "data",
					Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }}})
				require.NoError(t, err)
				require.True(t, written)
			}
			restart := func() *IndexFileCache {
				c, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(capacity), "", "", 0, 0, 0)
				require.NoError(t, err)
				<-c.disk.load.done
				return c
			}
			earlier, err := newIndexFileCache(ctx, t.TempDir(), fscache.ConstCapacity(capacity), "", "", 0, 0, 0)
			require.NoError(t, err)
			<-earlier.disk.load.done
			c2 := restart()
			var diskPath string
			var size int64
			for _, name := range []string{"kept", "entry"} {
				put(earlier, name)
				from, err := earlier.disk.pathForDir(name)
				require.NoError(t, err)
				if name == "entry" {
					size, _ = earlier.disk.cache.Size(from)
					require.Positive(t, size)
				}
				to, err := c2.disk.pathForDir(name)
				require.NoError(t, err)
				require.NoError(t, os.MkdirAll(filepath.Dir(to), 0o755))
				require.NoError(t, os.Rename(from, to))
				diskPath = to
			}
			earlier.Close(ctx)
			trashDirs := func() []string {
				m, err := filepath.Glob(diskPath + ".*" + cacheDirTrashSuffix)
				require.NoError(t, err)
				return m
			}
			t.Cleanup(func() {
				for _, m := range append(trashDirs(), diskPath) {
					_ = os.Chmod(m, 0o755)
				}
			})
			switch by {
			case "deleted while loading":
				c2.disk.hooks.beforeIndexLoadedDirs = func() {
					require.NoError(t, os.Chmod(diskPath, 0))
					require.NoError(t, c2.Delete(ctx, "entry"))
				}
				c2.disk.loadCache(ctx)
				c2.disk.hooks.beforeIndexLoadedDirs = nil
			case "old trash":
				require.NoError(t, os.Rename(diskPath, fmt.Sprintf("%s.%d-%d%s", diskPath, 1, 1, cacheDirTrashSuffix)))
				require.NoError(t, os.Chmod(trashDirs()[0], 0))
			case "unlistable entry":
				require.NoError(t, os.Chmod(diskPath, 0))
			}
			c2.Close(ctx)
			c := restart()
			defer c.Close(ctx)
			used := c.disk.cache.Used()
			if by == "control" {
				require.Equal(t, capacity, c.disk.capacityFunc())
				return
			}
			require.Len(t, trashDirs(), 1)
			_, err = os.Stat(diskPath)
			require.True(t, os.IsNotExist(err))
			if by == "deleted while loading" {
				require.Equal(t, size, c.disk.trash.bytes.Load())
				require.Equal(t, capacity-size, c.disk.capacityFunc())
			} else {
				require.Zero(t, c.disk.trash.bytes.Load())
				require.Equal(t, used, c.disk.capacityFunc(), "the cache is held at what it used")
			}
			require.NoError(t, os.Chmod(trashDirs()[0], 0o755))
			c.disk.retryTrash()
			require.Empty(t, trashDirs())
			require.Zero(t, c.disk.trash.bytes.Load())
			require.Equal(t, capacity, c.disk.capacityFunc())
		})
	}
}

// A read that finds its entry missing a file repairs it although the load indexed the
// entry meanwhile: indexing changes no file of it.
func TestIndexObjectRepairOutlivesLoadIndexing(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		t.Run(map[bool]string{true: "indexed", false: "control"}[indexed], func(t *testing.T) {
			ctx := context.Background()
			fs, storage, data := newIndexObjectFS(t, true)
			<-fs.indexFileCache.disk.load.done
			cacheIndexObject(t, fs, "object", data)
			diskPath, ok := fs.indexObjectCached("object")
			require.True(t, ok)
			require.NoError(t, os.Remove(filepath.Join(diskPath, IndexObjectFile)))
			name := IndexObjectEntry("object")
			_, read, err := fs.indexFileCache.openEntryFile(ctx, name, IndexObjectFile)
			require.Error(t, err)
			if indexed {
				fs.indexFileCache.disk.indexLoadedDirs(ctx, []loadedDir{{path: diskPath}})
			}
			fs.indexFileCache.dropUnopened(ctx, name, IndexObjectFile, read)
			read.done()
			_, cached := fs.indexObjectCached("object")
			require.False(t, cached, "the broken entry is dropped")
			storage.reads.Store(0)
			require.Equal(t, data[2000:2010], readIndexObjectRange(t, fs, IndexFileObject, 2000, 10))
			require.Equal(t, data[len(data)-10:], readIndexObjectRange(t, fs, IndexFileObject, int64(len(data)-10), 10))
			require.Equal(t, int64(1), storage.reads.Load(), "the read filled the entry again")
		})
	}
}

// permissionsEnforced reports whether a directory without permissions cannot be listed.
func permissionsEnforced(t *testing.T) bool {
	dir := filepath.Join(t.TempDir(), "probe")
	require.NoError(t, os.Mkdir(dir, 0))
	defer func() { _ = os.Chmod(dir, 0o755) }()
	_, err := os.ReadDir(dir)
	return err != nil
}

// The load holds the cache to what it used once it finds trash of unknown bytes: a
// publication while it loads is not admitted against that space, and the entries it
// found are indexed. Its removal of an entry it cannot list keeps a replacement published
// since.
func TestIndexFileCacheLoadWithUnknownTrash(t *testing.T) {
	for _, by := range []string{"publication", "publication control", "replacement", "replacement control"} {
		t.Run(by, func(t *testing.T) {
			if !permissionsEnforced(t) {
				t.Skip("permissions are not enforced")
			}
			ctx := context.Background()
			data := bytes.Repeat([]byte{7}, 300<<10)
			files := func() []IndexCacheFile {
				return []IndexCacheFile{{Name: "data",
					Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }}}
			}
			earlier, err := newIndexFileCache(ctx, t.TempDir(), fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
			require.NoError(t, err)
			<-earlier.disk.load.done
			c, err := newIndexFileCache(ctx, t.TempDir(), fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
			require.NoError(t, err)
			defer c.Close(ctx)
			<-c.disk.load.done
			move := func(name string) string {
				written, err := earlier.Put(ctx, name, files())
				require.NoError(t, err)
				require.True(t, written)
				from, err := earlier.disk.pathForDir(name)
				require.NoError(t, err)
				to, err := c.disk.pathForDir(name)
				require.NoError(t, err)
				require.NoError(t, os.MkdirAll(filepath.Dir(to), 0o755))
				require.NoError(t, os.Rename(from, to))
				return to
			}
			kept := move("kept")
			var trash string
			t.Cleanup(func() {
				m, _ := filepath.Glob(filepath.Join(c.disk.path, "*", "*"))
				for _, p := range m {
					_ = os.Chmod(p, 0o755)
				}
			})
			switch by {
			case "publication", "publication control":
				entry := move("entry")
				trash = fmt.Sprintf("%s.%d-%d%s", entry, 1, 1, cacheDirTrashSuffix)
				require.NoError(t, os.Rename(entry, trash))
				if by == "publication" {
					require.NoError(t, os.Chmod(trash, 0))
				}
				fresh, err := c.disk.pathForDir("fresh")
				require.NoError(t, err)
				c.disk.hooks.beforeIndexLoadedDirs = func() {
					_, err := c.Put(ctx, "fresh", files())
					require.NoError(t, err)
				}
				c.disk.loadCache(ctx)
				c.disk.hooks.beforeIndexLoadedDirs = nil
				require.True(t, c.disk.cache.Contains(kept), "the entry the load found is indexed")
				require.Equal(t, by == "publication control", c.disk.cache.Contains(fresh))
				require.LessOrEqual(t, c.disk.cache.Used(), c.disk.capacityFunc())
				if by == "publication" {
					keptSize, _ := c.disk.cache.Size(kept)
					require.Equal(t, keptSize, c.disk.capacityFunc(), "the cache is held at what it used")
				}
			case "replacement", "replacement control":
				entry := move("entry")
				require.NoError(t, os.Chmod(entry, 0))
				if by == "replacement" {
					c.disk.hooks.beforeRemoveUnlistedDir = func(path string) {
						require.Equal(t, entry, path)
						require.NoError(t, os.Chmod(entry, 0o755))
						require.NoError(t, c.Delete(ctx, "entry"))
						written, err := c.Put(ctx, "entry", files())
						require.NoError(t, err)
						require.True(t, written)
					}
				}
				c.disk.loadCache(ctx)
				c.disk.hooks.beforeRemoveUnlistedDir = nil
				size, err := cacheDirSize(entry)
				if by == "replacement" {
					require.NoError(t, err, "the replacement is kept")
					require.Positive(t, size)
					require.True(t, c.disk.cache.Contains(entry))
				} else {
					require.True(t, os.IsNotExist(err), "the unlistable entry is removed")
					require.False(t, c.disk.cache.Contains(entry))
				}
				require.True(t, c.disk.cache.Contains(kept))
			}
		})
	}
}

// A read whose open of a broken entry fails repairs only the entry it read: a
// replacement published before its repair, by a read or by another cache instance on the
// directory, is kept, also when the read could not open the entry.
func TestIndexObjectStaleOpenRepairKeepsReplacement(t *testing.T) {
	for _, by := range []string{"read", "other instance", "unopenable", "control"} {
		replaced := by != "control"
		t.Run(by, func(t *testing.T) {
			ctx := context.Background()
			fs, storage, data := newIndexObjectFS(t, true)
			cacheIndexObject(t, fs, "object", data)
			diskPath, ok := fs.indexObjectCached("object")
			require.True(t, ok)
			if by == "unopenable" {
				require.NoError(t, os.Chmod(diskPath, 0))
				t.Cleanup(func() {
					m, _ := filepath.Glob(diskPath + "*")
					for _, p := range m {
						_ = os.Chmod(p, 0o755)
					}
				})
				if f, err := os.Open(diskPath); err == nil {
					_ = f.Close()
					t.Skip("permissions are not enforced")
				}
			} else {
				file := filepath.Join(diskPath, IndexObjectFile)
				require.NoError(t, os.Remove(file))
				require.NoError(t, os.Symlink(IndexObjectFile, file))
			}
			name := IndexObjectEntry("object")
			_, read, err := fs.indexFileCache.openEntryFile(context.Background(), name, IndexObjectFile)
			require.Error(t, err)
			require.NotNil(t, read)
			if by == "unopenable" {
				require.NoError(t, os.Chmod(diskPath, 0o755))
			}
			switch by {
			case "read":
				storage.reads.Store(0)
				require.Equal(t, data[2000:2010], readIndexObjectRange(t, fs, IndexFileObject, 2000, 10))
				require.Equal(t, int64(1), storage.reads.Load(), "the read filled the entry again")
			case "other instance", "unopenable":
				other, err := newIndexFileCache(ctx, fs.indexFileCache.disk.path, fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
				require.NoError(t, err)
				t.Cleanup(func() { other.Close(ctx) })
				require.NoError(t, other.Delete(ctx, name))
				written, err := other.Put(ctx, name, []IndexCacheFile{{Name: IndexObjectFile,
					Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }}})
				require.NoError(t, err)
				require.True(t, written)
			}
			fs.indexFileCache.dropUnopened(ctx, name, "", read)
			read.done()
			_, cached := fs.indexObjectCached("object")
			require.Equal(t, replaced, cached)
			storage.reads.Store(0)
			require.Equal(t, data[len(data)-10:], readIndexObjectRange(t, fs, IndexFileObject, int64(len(data)-10), 10))
			if replaced {
				require.Zero(t, storage.reads.Load(), "the replacement was dropped")
			}
		})
	}
}

// The repair of a missing entry file is no delete of the object: it waits for no
// publication holding the entry's delete stripe, and keeps the delete mark.
func TestIndexObjectMissingFileRepairWaitsForNoPublication(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	cacheIndexObject(t, fs, "object", data)
	diskPath, ok := fs.indexObjectCached("object")
	require.True(t, ok)
	require.NoError(t, os.Remove(filepath.Join(diskPath, IndexObjectFile)))
	stripe := fs.indexFileCache.disk.deletes.stripe(diskPath)
	stripe.RLock()
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			stripe.RUnlock()
		}
	}
	defer unlock()
	mark := fs.indexFileCache.DeleteMark()
	storage.reads.Store(0)
	done := make(chan []byte, 1)
	go func() {
		// The read fills no cache, so the dropped entry stays dropped.
		done <- readIndexObjectRange(t, fs, IndexFileObject|SkipCacheWrites, 2000, 10)
	}()
	select {
	case got := <-done:
		require.Equal(t, data[2000:2010], got)
	case <-time.After(10 * time.Second):
		unlock()
		t.Fatal("the repair waited for the publication")
	}
	_, cached := fs.indexObjectCached("object")
	require.False(t, cached, "the entry is dropped")
	require.Equal(t, mark, fs.indexFileCache.DeleteMark())
}

// A missing entry file whose path another holder owns is not dropped, and the delete mark
// is kept, so the holder's work is not canceled.
func TestIndexObjectMissingFileOwnedIsKept(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	cacheIndexObject(t, fs, "object", data)
	diskPath, ok := fs.indexObjectCached("object")
	require.True(t, ok)
	require.NoError(t, os.Remove(filepath.Join(diskPath, IndexObjectFile)))
	d := fs.indexFileCache.disk
	owner, ok := d.paths.tryAcquire(diskPath, nil)
	require.True(t, ok)
	mark := fs.indexFileCache.DeleteMark()
	storage.reads.Store(0)
	// The read fills no cache, so it does not wait for the holder to publish.
	vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipMemoryCache | SkipCacheWrites,
		Entries: []IOEntry{{Offset: 2000, Size: 10}}}
	require.NoError(t, fs.Read(context.Background(), vec))
	require.Equal(t, data[2000:2010], vec.Entries[0].Data)
	vec.Release()
	require.True(t, d.cache.Contains(diskPath))
	require.Equal(t, mark, fs.indexFileCache.DeleteMark())
	require.NoError(t, owner.release())
}

// An entry whose file cannot be opened, or is missing, is dropped and read from storage,
// and the read fills it again.
func TestIndexObjectUnopenableEntryReadsStorage(t *testing.T) {
	for _, what := range []string{"file", "dir", "missing"} {
		t.Run(what, func(t *testing.T) {
			fs, storage, data := newIndexObjectFS(t, true)
			cacheIndexObject(t, fs, "object", data)
			diskPath, ok := fs.indexObjectCached("object")
			require.True(t, ok)
			file := filepath.Join(diskPath, IndexObjectFile)
			switch what {
			case "file":
				require.NoError(t, os.Remove(file))
				require.NoError(t, os.Symlink(IndexObjectFile, file))
			case "dir":
				require.NoError(t, os.RemoveAll(diskPath))
				require.NoError(t, os.MkdirAll(filepath.Dir(diskPath), 0o755))
				require.NoError(t, os.WriteFile(diskPath, nil, 0o644))
			case "missing":
				require.NoError(t, os.Remove(file))
			}
			require.Equal(t, data[3000:3010], readIndexObjectRange(t, fs, IndexFileObject|SkipAllCache, 3000, 10))
			storage.reads.Store(0)
			require.Equal(t, data[2000:2010], readIndexObjectRange(t, fs, IndexFileObject, 2000, 10))
			require.Equal(t, int64(1), storage.reads.Load())
			require.Equal(t, data[len(data)-10:], readIndexObjectRange(t, fs, IndexFileObject, int64(len(data)-10), 10))
			require.Equal(t, int64(1), storage.reads.Load(), "the read filled the entry again")
		})
	}
}
