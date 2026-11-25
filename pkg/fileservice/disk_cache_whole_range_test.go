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
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

// oldWholeFiles returns the files under dir named as whole objects were before.
func oldWholeFiles(t *testing.T, d *DiskCache) []string {
	var found []string
	require.NoError(t, filepath.WalkDir(d.path, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)
		if !entry.IsDir() && d.isOldWholeFile(path) {
			found = append(found, path)
		}
		return nil
	}))
	return found
}

// readObjectEntry reads size bytes at offset of the object at file from cache, and reports
// whether the cache served them and the bytes.
func readObjectEntry(t *testing.T, cache *DiskCache, file string, offset, size int64) (bool, []byte) {
	vec := &IOVector{FilePath: file, Entries: []IOEntry{{Offset: offset, Size: size}}}
	defer vec.Release()
	require.NoError(t, cache.Read(context.Background(), vec))
	return vec.Entries[0].done, bytes.Clone(vec.Entries[0].Data)
}

// A whole object is cached as the range [0, size), which serves reads of its sub-ranges.
func TestWholeObjectCachedAsRangeServesSubRange(t *testing.T) {
	ctx := context.Background()
	cache := newLifecycleTestDiskCache(t)
	data := rangeTestData(100)
	require.NoError(t, cache.SetFile(ctx, "obj", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}))

	whole := cache.wholePath("obj", int64(len(data)))
	require.Equal(t, []string{whole}, cache.objectFiles("obj"))
	require.Empty(t, oldWholeFiles(t, cache))
	require.Equal(t, []cachedRange{{diskPath: whole, offset: 0, size: int64(len(data))}}, cache.ranges.of(cache.objectKey("obj")))

	done, got := readObjectEntry(t, cache, "obj", 10, 20)
	require.True(t, done)
	require.Equal(t, data[10:30], got)
}

// A whole object of unknown size is published as the range of the size its fill finds,
// which is recorded.
func TestWholeObjectOfUnknownSizePublishedAtFoundSize(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
	d := fs.diskCache
	_, known := d.prefetched.size(d.objectKey("object"))
	require.False(t, known)

	require.NoError(t, fs.PrefetchFile(context.Background(), "object"))
	require.Equal(t, int64(1), storage.reads.Load())
	require.Equal(t, []string{d.wholePath("object", int64(len(data)))}, d.objectFiles("object"))
	size, known := d.prefetched.size(d.objectKey("object"))
	require.True(t, known)
	require.Equal(t, int64(len(data)), size)

	got, err := readFullObjectOf(fs, 1000, 10)
	require.NoError(t, err)
	require.Equal(t, data[1000:1010], got)
	require.Equal(t, int64(1), storage.reads.Load(), "served from the cached range")
}

// A prefetch of an object whose size no prefetch told learns it from object storage when
// a cached range starts at the object's start, and downloads only what is not cached.
func TestPrefetchOfWarmWholeObjectLearnsItsSize(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cached   func(*testing.T, *S3FS, []byte)
		stats    int64
		download bool
	}{
		{name: "whole object cached", stats: 1, cached: func(t *testing.T, fs *S3FS, _ []byte) {
			require.NoError(t, fs.PrefetchFile(context.Background(), "object"))
		}},
		{name: "start of the object cached", stats: 1, download: true, cached: func(t *testing.T, fs *S3FS, data []byte) {
			require.NoError(t, fs.diskCache.Update(context.Background(), &IOVector{FilePath: "object",
				Entries: []IOEntry{{Offset: 0, Size: 100, Data: data[:100]}}}, false))
		}},
		{name: "nothing from the start cached", download: true, cached: func(t *testing.T, fs *S3FS, data []byte) {
			require.NoError(t, fs.diskCache.Update(context.Background(), &IOVector{FilePath: "object",
				Entries: []IOEntry{{Offset: 100, Size: 100, Data: data[100:200]}}}, false))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
			d := fs.diskCache
			tc.cached(t, fs, data)
			// The sizes known in the process are lost, as at a restart.
			d.prefetched = prefetchedObjects{}
			stats := &statCountingStorage{ObjectStorage: storage}
			fs.storage = stats
			reads := storage.reads.Load()

			require.NoError(t, fs.PrefetchFile(context.Background(), "object"))
			require.Equal(t, tc.stats, stats.stats.Load())
			if tc.download {
				require.Equal(t, reads+1, storage.reads.Load())
			} else {
				require.Equal(t, reads, storage.reads.Load(), "the cached whole object is not downloaded again")
			}
			require.True(t, d.cache.Contains(d.wholePath("object", int64(len(data)))))
		})
	}
}

// A failed stat of the object leaves the fill to download it as an object of unknown
// size.
func TestPrefetchOfWarmObjectWithFailedStatDownloads(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
	require.NoError(t, fs.diskCache.Update(context.Background(), &IOVector{FilePath: "object",
		Entries: []IOEntry{{Offset: 0, Size: 100, Data: data[:100]}}}, false))
	fs.storage = failingStatStorage{storage}
	require.NoError(t, fs.PrefetchFile(context.Background(), "object"))
	require.Equal(t, int64(1), storage.reads.Load())
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))))
}

type failingStatStorage struct{ ObjectStorage }

func (failingStatStorage) Stat(context.Context, string) (int64, error) {
	return 0, os.ErrPermission
}

// A delete of an object removes its whole object as one of its ranges, and an eviction
// removes it as one range.
func TestWholeObjectDeleteAndEvictionAsOneRange(t *testing.T) {
	ctx := context.Background()
	data := rangeTestData(100)
	fill := func(t *testing.T, cache *DiskCache) string {
		require.NoError(t, cache.Update(ctx, &IOVector{FilePath: "obj",
			Entries: []IOEntry{{Offset: 10, Size: 10, Data: data[10:20]}}}, false))
		require.NoError(t, cache.SetFile(ctx, "obj", func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(data)), nil
		}))
		whole := cache.wholePath("obj", int64(len(data)))
		require.True(t, cache.cache.Contains(whole))
		require.Len(t, cache.ranges.of(cache.objectKey("obj")), 2)
		cache.preloads.mu.Lock()
		_, tracked := cache.preloads.served[whole]
		cache.preloads.mu.Unlock()
		require.True(t, tracked, "the whole object's file counts its reads")
		return whole
	}
	untracked := func(t *testing.T, cache *DiskCache, whole string) {
		cache.preloads.mu.Lock()
		defer cache.preloads.mu.Unlock()
		_, tracked := cache.preloads.served[whole]
		require.False(t, tracked)
	}

	t.Run("delete", func(t *testing.T) {
		cache := newLifecycleTestDiskCache(t)
		whole := fill(t, cache)
		require.NoError(t, cache.DeletePaths(ctx, []string{"obj"}))
		require.Empty(t, cache.objectFiles("obj"))
		require.Empty(t, cache.ranges.of(cache.objectKey("obj")))
		require.False(t, cache.cache.Contains(whole))
		untracked(t, cache, whole)
		_, known := cache.prefetched.size(cache.objectKey("obj"))
		require.False(t, known, "the delete forgets the size")
		done, _ := readObjectEntry(t, cache, "obj", 30, 10)
		require.False(t, done)
	})

	t.Run("eviction", func(t *testing.T) {
		cache := newLifecycleTestDiskCache(t)
		whole := fill(t, cache)
		cache.cache.Delete(ctx, whole)
		require.NoFileExists(t, whole)
		require.Equal(t, []cachedRange{{diskPath: cache.pathForIOEntry("obj", IOEntry{Offset: 10, Size: 10}), offset: 10, size: 10}},
			cache.ranges.of(cache.objectKey("obj")), "the other range stays")
		untracked(t, cache, whole)
		done, _ := readObjectEntry(t, cache, "obj", 30, 10)
		require.False(t, done)
		done, got := readObjectEntry(t, cache, "obj", 12, 5)
		require.True(t, done)
		require.Equal(t, data[12:17], got)
	})
}

// A range of an object cached from its start that no whole-object fill published does not
// count as its whole object.
func TestRangeFromObjectStartIsNotTrackedAsWholeObject(t *testing.T) {
	ctx := context.Background()
	cache := newLifecycleTestDiskCache(t)
	data := rangeTestData(100)
	require.NoError(t, cache.Update(ctx, &IOVector{FilePath: "obj",
		Entries: []IOEntry{{Offset: 0, Size: 100, Data: data}}}, false))
	path := cache.wholePath("obj", 100)
	require.True(t, cache.cache.Contains(path))
	cache.preloads.mu.Lock()
	defer cache.preloads.mu.Unlock()
	require.NotContains(t, cache.preloads.served, path)
}

// Cache files named as whole objects by an earlier version are removed at startup, from
// the disk cache and the index-file cache; range files stay.
func TestStartupRemovesOldWholeFiles(t *testing.T) {
	ctx := context.Background()
	for _, dirEntries := range []bool{false, true} {
		name := "disk cache"
		if dirEntries {
			name = "index-file cache"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			write := func(rel, content string) string {
				path := filepath.Join(dir, rel)
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
				require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
				return path
			}
			old := []string{
				write(filepath.Join(diskCacheShard("obj"), "fullobj"+cacheFileSuffix), "whole"),
				write(filepath.Join(diskCacheShard("dir/obj"), "fulldir", "obj"+cacheFileSuffix), "whole"),
				write("fullfoo"+cacheFileSuffix, "whole"),
			}
			kept := write(filepath.Join(diskCacheShard("obj"), "0-3obj"+cacheFileSuffix), "abc")

			cache, err := newDiskCache(ctx, dir, fscache.ConstCapacity(1<<20), nil, false, nil, "", "",
				diskCacheOptions{dirEntries: dirEntries})
			require.NoError(t, err)
			defer cache.Close(ctx)
			for _, path := range old {
				require.NoFileExists(t, path)
				require.False(t, cache.cache.Contains(path))
			}
			require.FileExists(t, kept)
			require.True(t, cache.cache.Contains(kept))
			if !dirEntries {
				done, got := readObjectEntry(t, cache, "obj", 1, 2)
				require.True(t, done)
				require.Equal(t, []byte("bc"), got)
			}
		})
	}
}

// Only a path named "full" and an object path, in the cache's directory or a shard, is
// an old whole file.
func TestIsOldWholeFile(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	for rel, want := range map[string]bool{
		filepath.Join(diskCacheShard("obj"), "fullobj"+cacheFileSuffix):            true,
		filepath.Join(diskCacheShard("dir/obj"), "fulldir", "obj"+cacheFileSuffix): true,
		"fullfoo" + cacheFileSuffix:                                            true,
		filepath.Join(diskCacheShard("obj"), "0-3obj"+cacheFileSuffix):         false,
		filepath.Join(diskCacheShard("fullobj"), "0-3fullobj"+cacheFileSuffix): false,
		filepath.Join(diskCacheShard("obj"), "fullobj"+cacheFileTempSuffix):    false,
	} {
		require.Equal(t, want, cache.isOldWholeFile(filepath.Join(cache.path, rel)), rel)
	}
	require.False(t, strings.HasPrefix(filepath.Base(cache.wholePath("full", 3)), "full"))
}

// Fix-missing is accepted and ignored: no object is restored from the disk cache.
func TestFixMissingIsIgnored(t *testing.T) {
	ctx := context.Background()
	cacheDir := t.TempDir()
	old := filepath.Join(cacheDir, diskCacheShard("obj"), "fullobj"+cacheFileSuffix)
	require.NoError(t, os.MkdirAll(filepath.Dir(old), 0o755))
	require.NoError(t, os.WriteFile(old, []byte("whole"), 0o644))
	fs, err := NewFileService(ctx, Config{
		Name:       "s3",
		Backend:    s3FileServiceBackend,
		S3:         ObjectStorageArguments{Endpoint: "disk", Bucket: t.TempDir()},
		Cache:      CacheConfig{DiskPath: &cacheDir, DiskCapacity: ptrTo[toml.ByteSize](1 << 20)},
		FixMissing: true,
	}, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	_, err = fs.StatFile(ctx, "obj")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "%v", err)
	<-fs.(*S3FS).diskCache.load.done
	require.NoFileExists(t, old)
}
