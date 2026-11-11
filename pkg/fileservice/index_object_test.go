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
	"os"
	"slices"
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

func newIndexObjectFS(t *testing.T, on bool) (*S3FS, *readCountingStorage, []byte) {
	ctx := context.Background()
	config := indexCacheConfig(t, 64<<20, 64<<20)
	config.MemoryCapacity = ptrTo[toml.ByteSize](0)
	config.IndexFileCacheObjects = on
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

// cacheIndexObject puts data as the entry of the object at file, as indexfile.OpenObject.
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
	require.Zero(t, storage.reads.Load())
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))

	readIndexObjectRange(t, fs, 0, 250<<10, 10)
	require.NotZero(t, storage.reads.Load(), "a read without IndexFileObject does not use the entry")
}

// Without the setting, the index-file cache's entry of an object is not read.
func TestIndexObjectsOff(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, false)
	cacheIndexObject(t, fs, "object", data)
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
