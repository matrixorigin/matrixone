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
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/require"
)

func TestDiskCacheShardIsStable(t *testing.T) {
	// Pinned values: a different shard function would make every existing
	// cache file unreachable after an upgrade.
	for path, want := range map[string]string{
		"":  "dc5",
		"a": "92c",
		"query_result/ws_0c604661_01a1396b-fe6e-7c0a-a85b-255064397d69_1.blk": "ea5",
		"01a1396b-b939-7682-af2b-6bc1573dd579_00000":                          "412",
	} {
		require.Equal(t, want, diskCacheShard(path), path)
	}

	for i := 0; i < 1000; i++ {
		path := fmt.Sprintf("object-%d", i)
		h := fnv.New32a()
		_, _ = h.Write([]byte(path))
		require.Equal(t, fmt.Sprintf("%03x", h.Sum32()%diskCacheShardCount), diskCacheShard(path))
		require.True(t, isDiskCacheShard(diskCacheShard(path)))
	}
}

func TestDiskCacheSpreadsFilesAcrossShards(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)

	const numObjects = 100_000
	perDir := make(map[string]int)
	for i := 0; i < numObjects; i++ {
		path := fmt.Sprintf("01a1396b-%04x-7682-af2b-%012x_%05d", i%65536, i*7919, i%10)
		full := cache.pathForFile(path)
		entry := cache.pathForIOEntry(path, IOEntry{Offset: 10, Size: 20})

		shardDir := filepath.Join(cache.path, diskCacheShard(path))
		require.Equal(t, shardDir, filepath.Dir(full))
		require.Equal(t, shardDir, filepath.Dir(entry))
		perDir[filepath.Dir(full)]++
	}

	require.Len(t, perDir, diskCacheShardCount)
	for dir, n := range perDir {
		require.LessOrEqual(t, n, numObjects/1000, dir)
	}

	// Object paths with directories still land below a shard, not directly in
	// a directory shared by every object of that prefix.
	path := "query_result/ws_1_2_3.blk"
	rel, err := filepath.Rel(cache.path, cache.pathForFile(path))
	require.NoError(t, err)
	require.Equal(t, filepath.Join(diskCacheShard(path), "fullquery_result", "ws_1_2_3.blk"+cacheFileSuffix), rel)
}

func TestDiskCachePathsNormalizeDotSegments(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	// "foo/./bar" and "foo/bar" name the same backing object (this mirrors
	// how S3 and the rest of fileservice treat them, see ParsePath/path.Clean
	// elsewhere in this package). filepath.Join cleans the "full..."/"N-N..."
	// filename component for free, so the shard has to be derived from the
	// same cleaned form, or the two spellings would be sharded apart and a
	// delete issued through one spelling would leave the other reachable.
	require.Equal(t, cache.pathForFile("foo/bar"), cache.pathForFile("foo/./bar"))
	require.Equal(t,
		cache.pathForIOEntry("foo/bar", IOEntry{Offset: 1, Size: 2}),
		cache.pathForIOEntry("foo/./bar", IOEntry{Offset: 1, Size: 2}),
	)
}

// TestDiskCacheAliasDelete and TestDiskCacheS3AliasDelete reproduce a review
// finding against the initial version of this change: diskCacheShard hashed
// the raw, uncleaned object path, so an alias like "foo/./bar" sharded apart
// from "foo/bar" even though they name the same object. A delete or
// overwrite issued through one spelling left the other's cache entry
// reachable and stale.
func TestDiskCacheAliasDelete(t *testing.T) {
	ctx := context.Background()
	cache := newLifecycleTestDiskCache(t)
	const original = "foo/./bar"
	const canonical = "foo/bar"
	require.Equal(t, pathpkg.Join("prefix", original), pathpkg.Join("prefix", canonical))
	require.NoError(t, cache.SetFile(ctx, original, func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("old"))), nil
	}))
	require.NoError(t, cache.DeletePaths(ctx, []string{canonical}))
	v := &IOVector{FilePath: original, Entries: []IOEntry{{Offset: 0, Size: 3}}}
	defer v.Release()
	require.NoError(t, cache.Read(ctx, v))
	require.False(t, v.Entries[0].done, "alias deletion must invalidate the full file for the same backing object")
}

func TestDiskCacheS3AliasDelete(t *testing.T) {
	ctx := context.Background()
	s, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, DisabledCacheConfig, nil, true, false)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close(ctx) })
	cache := newLifecycleTestDiskCache(t)
	s.diskCache = cache
	const original = "foo/./bar"
	const canonical = "foo/bar"
	write := func(data string) {
		require.NoError(t, s.Write(ctx, IOVector{FilePath: original, Entries: []IOEntry{{Size: 3, Data: []byte(data)}}, Policy: SkipFullFilePreloads}))
	}
	write("old")
	require.NoError(t, cache.SetFile(ctx, original, func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("old"))), nil
	}))
	require.NoError(t, s.Delete(ctx, canonical))
	write("new")
	v := &IOVector{FilePath: original, Entries: []IOEntry{{Offset: 0, Size: 3}}, Policy: SkipMemoryCache | SkipDiskCacheWrites}
	defer v.Release()
	require.NoError(t, s.Read(ctx, v))
	require.Equal(t, []byte("new"), v.Entries[0].Data, "reading rewritten S3 object must not return its deleted predecessor")
}

func TestDiskCacheDecodeFilePathAcceptsBothLayouts(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)

	for _, path := range []string{"foo", "query_result/ws_1_2_3.blk", "a/b/c"} {
		decoded, err := cache.decodeFilePath(cache.pathForFile(path))
		require.NoError(t, err)
		require.Equal(t, path, decoded)

		legacy := filepath.Join(cache.path, "full"+toOSPath(path)+cacheFileSuffix)
		decoded, err = cache.decodeFilePath(legacy)
		require.NoError(t, err)
		require.Equal(t, path, decoded)

		_, err = cache.decodeFilePath(cache.pathForIOEntry(path, IOEntry{Offset: 1, Size: 2}))
		require.ErrorIs(t, err, ErrNotCacheFile)
		_, err = cache.decodeFilePath(filepath.Join(cache.path, "1-2"+toOSPath(path)+cacheFileSuffix))
		require.ErrorIs(t, err, ErrNotCacheFile)
	}
}

func TestDiskCacheLegacyLayoutFilesAreAgedOut(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	content := []byte("foobar")

	legacyFull := filepath.Join(dir, "fullfoo"+cacheFileSuffix)
	legacyEntry := filepath.Join(dir, "0-3foo"+cacheFileSuffix)
	for _, path := range []string{legacyFull, legacyEntry} {
		require.NoError(t, os.WriteFile(path, content, 0o644))
	}
	stat, err := os.Stat(legacyFull)
	require.NoError(t, err)
	fileBytes := fileSize(stat)

	// Exactly full with the two legacy files.
	cache, err := NewDiskCache(ctx, dir, fscache.ConstCapacity(2*fileBytes), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)

	// Legacy files are accounted for, so they cannot push the cache over its
	// capacity, but they are never served.
	require.True(t, cache.cache.Contains(legacyFull))
	require.True(t, cache.cache.Contains(legacyEntry))
	vector := &IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Offset: 0, Size: 3}},
	}
	require.NoError(t, cache.Read(ctx, vector))
	require.False(t, vector.Entries[0].done)

	setFile := func(path string) {
		require.NoError(t, cache.SetFile(ctx, path, func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		}))
	}
	setFile("foo")
	setFile("bar")

	// New files displace the never-read legacy files first, which removes them
	// from disk.
	for _, path := range []string{legacyFull, legacyEntry} {
		require.False(t, cache.cache.Contains(path), path)
		_, err := os.Stat(path)
		require.ErrorIs(t, err, os.ErrNotExist, path)
	}
	for _, path := range []string{"foo", "bar"} {
		vector := &IOVector{
			FilePath: path,
			Entries:  []IOEntry{{Offset: 0, Size: 3}},
		}
		require.NoError(t, cache.Read(ctx, vector))
		require.Equal(t, content[:3], vector.Entries[0].Data, path)
		vector.Release()
	}
}

func TestClassifyDiskFull(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want diskFullKind
	}{
		{nil, notDiskFull},
		{errors.New("boom"), notDiskFull},
		{&fs.PathError{Op: "rename", Path: "x", Err: syscall.ENOSPC}, diskNoSpace},
		{fmt.Errorf("finalize: %w", &os.LinkError{Op: "rename", Old: "a", New: "b", Err: syscall.ENOSPC}), diskNoSpace},
		{&fs.PathError{Op: "write", Path: "x", Err: syscall.EDQUOT}, diskQuotaExceeded},
		{errorStr("write x: no space left on device"), diskNoSpace},
		{errorStr("write x: disk quota exceeded"), diskQuotaExceeded},
	} {
		require.Equal(t, tc.want, classifyDiskFull(tc.err), "%v", tc.err)
	}
}

const diskFullTestItemBytes = 1024

// newDiskFullTestCache returns a full cache of 100 items. disk reports the
// cache filesystem as exactly as large as the bytes the cache initially used,
// so it is exhausted until the cache frees space.
func newDiskFullTestCache(t *testing.T) (cache *DiskCache, initialUsed int64) {
	t.Helper()
	const numItems = 100
	cache, err := NewDiskCache(
		context.Background(),
		t.TempDir(),
		fscache.ConstCapacity(numItems*diskFullTestItemBytes),
		nil,
		false,
		nil,
		"",
	)
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close(context.Background()) })
	for i := 0; i < numItems; i++ {
		cache.cache.Set(context.Background(), filepath.Join(cache.path, "item", strconv.Itoa(i)), struct{}{}, diskFullTestItemBytes)
	}
	initialUsed = cache.cache.Used()
	require.Equal(t, int64(numItems*diskFullTestItemBytes), initialUsed)
	return cache, initialUsed
}

func (d *DiskCache) withFilesystemAvailable(available func() uint64, ok bool) {
	d.statFilesystem = func(string) (diskCacheFilesystemStat, bool) {
		return diskCacheFilesystemStat{
			availableBytes: available(),
			totalInodes:    1000,
			freeInodes:     1000,
		}, ok
	}
}

func TestDiskCacheENOSPCWithFreeSpaceKeepsCache(t *testing.T) {
	cache, used := newDiskFullTestCache(t)
	cache.withFilesystemAvailable(func() uint64 { return 1 << 40 }, true)

	// The directory-index-full failure: plenty of blocks and inodes.
	for i := 0; i < 20; i++ {
		cache.evictForDiskFull(context.Background(), &fs.PathError{Op: "rename", Path: "x", Err: syscall.ENOSPC})
	}
	require.Equal(t, used, cache.cache.Used())
}

func TestDiskCacheENOSPCEvictsWhenFilesystemIsExhausted(t *testing.T) {
	enospc := &fs.PathError{Op: "write", Path: "x", Err: syscall.ENOSPC}
	n := int64(100 * diskFullTestItemBytes / 10)

	t.Run("bytes", func(t *testing.T) {
		cache, used := newDiskFullTestCache(t)
		cache.withFilesystemAvailable(func() uint64 { return 0 }, true)
		cache.evictForDiskFull(context.Background(), enospc)
		require.Equal(t, used-n, cache.cache.Used())
	})

	t.Run("inodes", func(t *testing.T) {
		cache, used := newDiskFullTestCache(t)
		cache.statFilesystem = func(string) (diskCacheFilesystemStat, bool) {
			return diskCacheFilesystemStat{availableBytes: 1 << 40, totalInodes: 1000, freeInodes: 0}, true
		}
		cache.evictForDiskFull(context.Background(), enospc)
		require.Equal(t, used-n, cache.cache.Used())
	})

	t.Run("unknown filesystem state", func(t *testing.T) {
		cache, used := newDiskFullTestCache(t)
		cache.withFilesystemAvailable(func() uint64 { return 1 << 40 }, false)
		cache.evictForDiskFull(context.Background(), enospc)
		require.Equal(t, used-n, cache.cache.Used())
	})
}

// TestDiskCacheSmallInodeFilesystemTriggersEviction reproduces a review
// finding: totalInodes/100 truncates to zero on a filesystem with under 100
// inodes total, so freeInodes < 0 could never be true and zero free inodes
// was never recognized as exhaustion.
func TestDiskCacheSmallInodeFilesystemTriggersEviction(t *testing.T) {
	cache, used := newDiskFullTestCache(t)
	cache.statFilesystem = func(string) (diskCacheFilesystemStat, bool) {
		return diskCacheFilesystemStat{availableBytes: 1 << 40, totalInodes: 64, freeInodes: 0}, true
	}
	cache.evictForDiskFull(context.Background(), &fs.PathError{Op: "write", Path: "x", Err: syscall.ENOSPC})
	require.Less(t, cache.cache.Used(), used, "zero free inodes must trigger eviction even when total inodes is below 100")
}

func TestDiskCacheQuotaExceededEvictsRegardlessOfFilesystem(t *testing.T) {
	cache, used := newDiskFullTestCache(t)
	cache.withFilesystemAvailable(func() uint64 { return 1 << 40 }, true)
	cache.evictForDiskFull(context.Background(), &fs.PathError{Op: "write", Path: "x", Err: syscall.EDQUOT})
	require.Equal(t, used-int64(100*diskFullTestItemBytes/10), cache.cache.Used())
}

func TestDiskCacheSourceErrorDoesNotEvict(t *testing.T) {
	cache, used := newDiskFullTestCache(t)
	cache.withFilesystemAvailable(func() uint64 { return 0 }, true)
	cache.evictForDiskFull(context.Background(), &diskCacheSourceError{err: syscall.ENOSPC})
	require.Equal(t, used, cache.cache.Used())
}

func TestDiskCacheConcurrentDiskFullFailuresEvictOnce(t *testing.T) {
	cache, used := newDiskFullTestCache(t)
	n := int64(100 * diskFullTestItemBytes / 10)
	// The filesystem is exhausted until the cache has released n bytes.
	cache.withFilesystemAvailable(func() uint64 {
		return uint64(used - cache.cache.Used())
	}, true)

	const numFailures = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < numFailures; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			cache.evictForDiskFull(context.Background(), &fs.PathError{Op: "write", Path: "x", Err: syscall.ENOSPC})
		}()
	}
	close(start)
	wg.Wait()

	// Evictions deferred while the eviction lock was busy are applied by the
	// next eviction; include them.
	done := make(chan int64, 1)
	cache.cache.Evict(context.Background(), done, 0)
	<-done
	require.Equal(t, used-n, cache.cache.Used())
}

func TestDiskCacheDeleteRemovesLegacyLayoutFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// A pre-upgrade binary left a legacy, unsharded cache file on disk; the
	// fix never migrates it, only ages it out (option B), so it is still
	// present when the current binary starts up and indexes it.
	legacyFull := filepath.Join(dir, "fullfoo"+cacheFileSuffix)
	require.NoError(t, os.WriteFile(legacyFull, []byte("stale"), 0o644))

	cache, err := NewDiskCache(ctx, dir, fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	require.True(t, cache.cache.Contains(legacyFull))

	require.NoError(t, cache.DeletePaths(ctx, []string{"foo"}))

	_, err = os.Stat(legacyFull)
	require.True(t, os.IsNotExist(err), "delete must remove the leftover legacy-layout file, not just the new sharded path")
	require.False(t, cache.cache.Contains(legacyFull))
}

func TestDiskCacheDeleteInvalidatesLegacyLayoutBeforeRestore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Same setup as TestDiskCacheDeleteRemovesLegacyLayoutFile: a legacy
	// full-file cache entry left over from before an upgrade.
	legacyFull := filepath.Join(dir, "fullfoo"+cacheFileSuffix)
	require.NoError(t, os.WriteFile(legacyFull, []byte("stale"), 0o644))

	cache, err := NewDiskCache(ctx, dir, fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	require.True(t, cache.cache.Contains(legacyFull))

	s, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, DisabledCacheConfig, nil, true, false)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close(ctx) })
	s.diskCache = cache

	require.NoError(t, s.Delete(ctx, "foo"))

	// Without the fix, the orphaned legacy file is still on disk here, and
	// the directory walk below treats it as live cache content.
	counterSet := new(perfcounter.CounterSet)
	ctx = perfcounter.WithCounterSet(ctx, counterSet)
	s.restoreFromDiskCache(ctx)
	require.Equal(t, int64(0), counterSet.FileService.S3.Put.Load(), "restoring from disk cache must not resurrect an explicitly deleted object from a stale legacy cache file")

	err = s.Read(ctx, &IOVector{FilePath: "foo", Entries: []IOEntry{{Size: 3}}})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}
