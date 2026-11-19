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
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
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
	// The disk cache caches exactly the entries read, so a read it serves is told apart.
	fs.diskCache.prefetchRange = prefetchRangeConfig{}
	t.Cleanup(func() { fs.Close(ctx) })
	data := make([]byte, 300<<10)
	for i := range data {
		data[i] = byte(i*7 + i/1021)
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(data)), Data: data}}, Policy: SkipAllCache}))
	storage := &readCountingStorage{ObjectStorage: fs.storage}
	fs.storage = storage
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
	require.Equal(t, data[2000:2010], readIndexObjectRange(t, fs, IndexFileObject|SkipAllCache, 2000, 10), "a skip of disk-cache reads does not apply to the index-file cache")
	require.Zero(t, storage.reads.Load())
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))

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

// The prefetches of an object the index-file cache holds, through the prefetcher or the
// IO pipeline, are not downloaded into the disk cache; those of one it does not hold are.
func TestPrefetchOfCachedIndexObjectIsSkipped(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	ctx := WithFileServicePolicy(context.Background(), IndexFileObject)
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	require.False(t, FullFilePreloadSkipped(ctx, fs, IndexFileObject, "object"))
	require.False(t, FullFilePreloadSkipped(ctx, fs, IndexFileObject|SkipFullFilePreloads|NarrowRead, "object"),
		"a filtered search still preloads an object the index-file cache keeps")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
	require.True(t, fs.prefetcher.isQueued("object"))
	fs.prefetcher.remove("object")

	cacheIndexObject(t, fs, "object", data)
	require.True(t, FullFilePreloadSkipped(ctx, fs, IndexFileObject, "object"))
	require.False(t, FullFilePreloadSkipped(ctx, fs, 0, "object"))
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
	require.False(t, fs.prefetcher.isQueued("object"))
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
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")), "not in the disk cache")

	reads := storage.reads.Load()
	require.Equal(t, data[7000:7010], readIndexObjectRange(t, fs, IndexFileObject, 7000, 10))
	require.Equal(t, reads, storage.reads.Load())
}

// A fill without IndexFileObject publishes into the disk cache as before.
func TestNonIndexObjectFillUsesDiskCache(t *testing.T) {
	fs, _, _ := newIndexObjectFS(t, true)
	require.NoError(t, fs.PrefetchFile(context.Background(), "object"))
	_, cached := fs.indexObjectCached("object")
	require.False(t, cached)
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
}

// A queued whole-object prefetch of an IndexFileObject request publishes into the
// index-file cache, though the prefetcher runs it without the request's context.
func TestIndexObjectPrefetchFillsIndexFileCache(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	fs.prefetcher = newPrefetcher(0, func() int64 { return 1 << 40 }, fs.haveObject, fs.prefetchObject)
	ctx := WithFileServicePolicy(context.Background(), IndexFileObject)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
	fs.prefetcher.mu.Lock()
	req := fs.prefetcher.entries["object"].req
	fs.prefetcher.mu.Unlock()
	require.True(t, req.indexObject)
	fs.prefetcher.remove("object")

	require.False(t, fs.haveObject(req))
	fs.prefetchObject(context.Background(), req)
	_, cached := fs.indexObjectCached("object")
	require.True(t, cached)
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
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
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
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
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))
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
	objectPath := fs.diskCache.pathForFile("object")
	fs.diskCache.beforeFilePublication = func() {
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
	cacheIndexObject(t, fs, "object", data)
	d := fs.indexFileCache.disk
	diskPath, err := d.pathForDir(IndexObjectEntry("object"))
	require.NoError(t, err)
	kept := func() *openCacheFile {
		d.updatingPaths.L.Lock()
		defer d.updatingPaths.L.Unlock()
		return d.updatingPaths.openFiles.m[diskPath]
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
