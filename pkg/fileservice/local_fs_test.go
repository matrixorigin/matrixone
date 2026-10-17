// Copyright 2022 Matrix Origin
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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type localBlockingDataCache struct {
	fscache.DataCache
	updateStarted chan struct{}
	releaseUpdate chan struct{}
	once          sync.Once
}

type localObservedDataCache struct {
	fscache.DataCache
	beforeGet func()
}

func (c *localObservedDataCache) Get(ctx context.Context, key fscache.CacheKey) (fscache.Data, bool) {
	c.beforeGet()
	return c.DataCache.Get(ctx, key)
}

func requireDirFilesClosed(
	t *testing.T,
	dirFiles map[string]*os.File,
	closeFn func(),
) {
	t.Helper()
	handles := make([]*os.File, 0, len(dirFiles))
	for _, file := range dirFiles {
		handles = append(handles, file)
	}
	require.NotEmpty(t, handles)

	closeFn()
	require.Empty(t, dirFiles)
	for _, file := range handles {
		require.Error(t, file.Sync())
	}

	// Close must remain safe when cleanup paths are duplicated.
	closeFn()
}

func TestLocalFSCanonicalizesEmptyRoot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	t.Chdir(root)

	local, err := NewLocalFS(ctx, "local", "", DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(ctx) })
	require.Equal(t, root, local.RootPath())
	require.NoError(t, local.Write(ctx, IOVector{
		FilePath: "nested/file",
		Entries:  []IOEntry{{Size: 1, Data: []byte{1}}},
		Policy:   SkipAllCache,
	}))
	_, err = os.Stat(filepath.Join(root, "nested", "file"))
	require.NoError(t, err)
	requireDirFilesClosed(t, local.dirFiles, func() { local.Close(ctx) })
}

func TestLocalFSEmptyRootAcrossFilesystems(t *testing.T) {
	if _, err := os.Stat("/dev/shm"); err != nil {
		t.Skipf("/dev/shm is unavailable: %v", err)
	}

	destDir, err := os.MkdirTemp("/dev/shm", "matrixone-local-fs-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(destDir))
	})

	systemTempDir, err := os.MkdirTemp("/tmp", "matrixone-local-fs-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(systemTempDir))
	})
	destInfo, err := os.Stat(destDir)
	require.NoError(t, err)
	tempInfo, err := os.Stat(systemTempDir)
	require.NoError(t, err)
	if destInfo.Sys().(*syscall.Stat_t).Dev == tempInfo.Sys().(*syscall.Stat_t).Dev {
		t.Skip("test requires the destination and system temp directory on different filesystems")
	}

	t.Setenv("TMPDIR", systemTempDir)
	t.Chdir(destDir)

	ctx := context.Background()
	local, err := NewLocalFS(ctx, "local", "", DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(ctx) })
	require.Equal(t, destDir, local.RootPath())

	require.NoError(t, local.Write(ctx, IOVector{
		FilePath: "transfer/page",
		Entries:  []IOEntry{{Size: 4, Data: []byte("page")}},
		Policy:   SkipAllCache,
	}))
	require.FileExists(t, filepath.Join(destDir, "transfer", "page"))

	writer, err := local.NewWriter(ctx, "stream/page")
	require.NoError(t, err)
	_, err = writer.Write([]byte("page"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.FileExists(t, filepath.Join(destDir, "stream", "page"))
}

func (c *localBlockingDataCache) Set(ctx context.Context, key fscache.CacheKey, data fscache.Data) (bool, error) {
	c.once.Do(func() { close(c.updateStarted) })
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.releaseUpdate:
	}
	return c.DataCache.Set(ctx, key, data)
}

func TestLocalFSStatFileReturnsNonNotExistError(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "file"), []byte("data"), 0644))
	fs, err := NewLocalFS(ctx, "local", root, DisabledCacheConfig, nil)
	require.NoError(t, err)

	_, err = fs.StatFile(ctx, "file/child")
	require.ErrorIs(t, err, syscall.ENOTDIR)
}

func TestLocalFSMergeWaitHasBoundedFallback(t *testing.T) {
	ctx := context.Background()
	originalMaxWait := maxIOWaitDuration
	maxIOWaitDuration = 5 * time.Millisecond
	t.Cleanup(func() { maxIOWaitDuration = originalMaxWait })

	local, err := NewLocalFS(ctx, "local", t.TempDir(), DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, local.Write(ctx, IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Size: 3, Data: []byte("foo")}},
		Policy:   SkipAllCache,
	}))

	vector := &IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Offset: 0, Size: 3}},
	}
	t.Cleanup(vector.Release)
	finishMerge, waitMerge := local.ioMerger.Merge(vector.ioMergeKey(), maxIOWaitDuration)
	require.NotNil(t, finishMerge)
	require.Nil(t, waitMerge)
	releaseMerge := sync.OnceFunc(finishMerge)
	t.Cleanup(releaseMerge)

	readDone := make(chan error, 1)
	go func() { readDone <- local.Read(context.Background(), vector) }()
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		releaseMerge()
		select {
		case <-readDone:
		case <-time.After(time.Second):
			t.Fatal("local follower did not terminate after merge release")
		}
		t.Fatal("local follower rejoined a timed-out merge generation")
	}
	require.Equal(t, []byte("foo"), vector.Entries[0].Data)
}

func TestLocalFSMergeCompletesAfterCacheUpdate(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalFS(ctx, "local", t.TempDir(), CacheConfig{
		MemoryCapacity: ptrTo(toml.ByteSize(1 << 20)),
	}, nil)
	require.NoError(t, err)
	require.NoError(t, local.Write(ctx, IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Size: 3, Data: []byte("foo")}},
		Policy:   SkipAllCache,
	}))

	cache := &localBlockingDataCache{
		DataCache:     local.memCache.cache,
		updateStarted: make(chan struct{}),
		releaseUpdate: make(chan struct{}),
	}
	local.memCache.cache = cache
	releaseUpdate := sync.OnceFunc(func() { close(cache.releaseUpdate) })
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	var wg sync.WaitGroup
	vector := &IOVector{
		FilePath: "foo",
		Policy:   SkipFullFilePreloads,
		Entries: []IOEntry{{
			Offset:      0,
			Size:        3,
			ToCacheData: CacheOriginalData,
		}},
	}
	t.Cleanup(func() {
		cancel()
		releaseUpdate()
		wg.Wait()
		vector.Release()
	})

	readDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		readDone <- local.Read(readCtx, vector)
	}()
	select {
	case <-cache.updateStarted:
	case <-readCtx.Done():
		t.Fatalf("cache update did not start: %v", readCtx.Err())
	}
	require.True(t, local.ioMerger.IsMerging(vector.ioMergeKey()))
	releaseUpdate()
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-readCtx.Done():
		t.Fatalf("local read did not finish: %v", readCtx.Err())
	}
}

func TestLocalFSMergeCompletionWithoutCacheProgress(t *testing.T) {
	// Observe the merge wait, not Done calls while readCache derives a timeout
	// context. Other package tests may have enabled that optional cache timeout.
	previousCacheReadThreshold := slowCacheReadThreshold
	slowCacheReadThreshold = 0
	t.Cleanup(func() { slowCacheReadThreshold = previousCacheReadThreshold })
	for _, scenario := range []struct {
		name     string
		cached   bool
		pressure bool
	}{
		{name: "cache_published=false"},
		{name: "cache_published=true", cached: true},
		{name: "real_pressure_rejects_cache_publication", pressure: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			payload := []byte("foo")
			capacity := 1 << 20
			if scenario.pressure {
				payload = bytes.Repeat([]byte{7}, 64<<10)
				capacity = 4 * len(payload)
			}
			local, err := NewLocalFS(ctx, "local", t.TempDir(), CacheConfig{
				MemoryCapacity: ptrTo(toml.ByteSize(capacity)),
			}, nil)
			require.NoError(t, err)
			t.Cleanup(func() { local.Close(context.Background()) })
			if scenario.pressure {
				// Use the real cache allocator, shrink and admission contract.
				// A completed physical read must not imply a published cache entry.
				for i := range 4 {
					func() {
						name := fmt.Sprintf("warm-%d", i)
						require.NoError(t, local.Write(ctx, IOVector{FilePath: name, Policy: SkipAllCache,
							Entries: []IOEntry{{Size: int64(len(payload)), Data: payload}}}))
						warm := &IOVector{FilePath: name, Policy: SkipFullFilePreloads,
							Entries: []IOEntry{{Size: int64(len(payload)), ToCacheData: CacheOriginalData}}}
						defer warm.Release()
						require.NoError(t, local.Read(ctx, warm))
					}()
				}
				require.Equal(t, int64(capacity), local.memCache.cache.Used())
				owner := t.Name()
				t.Cleanup(func() { ClearMemoryCachePressureTargetByOwner(owner) })
				SetMemoryCachePressureTargetPercentByOwner(owner, 80, time.Now().Add(time.Minute))
				require.Equal(t, int64(3*len(payload)), local.memCache.EvictToCapacityPercent(ctx, 80))
			}
			require.NoError(t, local.Write(ctx, IOVector{
				FilePath: "foo", Policy: SkipAllCache,
				Entries: []IOEntry{{Size: int64(len(payload)), Data: payload}},
			}))
			vector := &IOVector{
				FilePath: "foo", Policy: SkipFullFilePreloads,
				Entries: []IOEntry{{Size: int64(len(payload)), ToCacheData: CacheOriginalData}},
			}
			t.Cleanup(vector.Release)
			key := vector.ioMergeKey()
			finishFirst, generation := local.ioMerger.merge(key)
			require.NotNil(t, finishFirst)
			require.Nil(t, generation)
			releaseFirst := sync.OnceFunc(finishFirst)
			var finishSecond func()
			var wg sync.WaitGroup
			t.Cleanup(func() {
				cancel()
				releaseFirst()
				wg.Wait()
				if finishSecond != nil {
					finishSecond()
				}
			})
			gets := 0
			dataCache := local.memCache.cache
			local.memCache.cache = &localObservedDataCache{
				DataCache: dataCache,
				beforeGet: func() {
					gets++
					if gets == 2 {
						// Another caller starts a new read before this follower
						// rechecks the cache. Completion of the first generation
						// does not guarantee cache admission (pressure, eviction,
						// or a leader requesting different columns).
						finishSecond, _ = local.ioMerger.merge(key)
					}
				},
			}
			waitCtx := &waitStartedContext{Context: ctx, started: make(chan struct{})}
			result := make(chan error, 1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				result <- local.Read(waitCtx, vector)
			}()
			select {
			case <-waitCtx.started:
			case <-ctx.Done():
				t.Fatal("follower did not enter the first generation")
			}
			if scenario.cached {
				data := local.memCache.CopyToCacheData(ctx, payload)
				defer data.Release()
				inserted, err := local.memCache.cache.Set(ctx, fscache.CacheKey{Path: "foo", Sz: int64(len(payload))}, data)
				require.NoError(t, err)
				require.True(t, inserted)
			} else if scenario.pressure {
				func() {
					// Bypass only read merging; the ordinary physical read, allocation
					// and cache update must run and really reject cache publication.
					leader := &IOVector{FilePath: "foo",
						Policy:  SkipMemoryCacheReads | SkipDiskCacheReads | SkipFullFilePreloads,
						Entries: []IOEntry{{Size: int64(len(payload)), ToCacheData: CacheOriginalData}}}
					defer leader.Release()
					require.NoError(t, local.Read(ctx, leader))
					require.Equal(t, payload, leader.Entries[0].CachedData.Bytes())
					_, present := dataCache.Get(ctx, fscache.CacheKey{Path: "foo", Sz: int64(len(payload))})
					require.False(t, present, "pressure must actually reject the leader's cache write")
				}()
			}
			releaseFirst()
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("completed generation without a cache hit must fall back, not wait on another generation")
			}
			wg.Wait()
			require.NotNil(t, finishSecond)
			require.Equal(t, 2, gets)
			require.Equal(t, payload, vector.Entries[0].CachedData.Bytes())
			require.Equal(t, scenario.cached, vector.Entries[0].fromCache == local.memCache)
			if scenario.pressure {
				require.Equal(t, int64(3*len(payload)), local.memCache.cache.Used(), "fallback must not bypass cache pressure")
			}
			// Finishing this follower must not release another reader's generation.
			require.True(t, local.ioMerger.IsMerging(key))
		})
	}
}

func TestLocalFS(t *testing.T) {

	t.Run("file service", func(t *testing.T) {
		testFileService(t, 0, func(name string) FileService {
			ctx := context.Background()
			dir := t.TempDir()
			fs, err := NewLocalFS(ctx, name, dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("with memory cache", func(t *testing.T) {
		testFileService(t, 0, func(name string) FileService {
			ctx := context.Background()
			dir := t.TempDir()
			fs, err := NewLocalFS(ctx, name, dir, CacheConfig{
				MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024),
			}, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("mutable file service", func(t *testing.T) {
		testMutableFileService(t, func() MutableFileService {
			ctx := context.Background()
			dir := t.TempDir()
			fs, err := NewLocalFS(ctx, "local", dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("replaceable file service", func(t *testing.T) {
		testReplaceableFileService(t, func() ReplaceableFileService {
			ctx := context.Background()
			dir := t.TempDir()
			fs, err := NewLocalFS(ctx, "local", dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			ctx := context.Background()
			dir := t.TempDir()
			fs, err := NewLocalFS(ctx, "local", dir, CacheConfig{
				MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024),
			}, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("error in write", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fs, err := NewLocalFS(
			ctx,
			"test",
			t.TempDir(),
			DisabledCacheConfig,
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Close(ctx)

		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					ReaderForWrite: errReader{},
					Size:           -1,
				},
				{
					ReaderForWrite: errReader{},
					Size:           -1,
				},
			},
		})

		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("got %v", err)
		}
	})

}

func TestLocalDirEntryInfoSkipsMissingEntries(t *testing.T) {
	info, ok, err := localDirEntryInfo(fakeDirEntryInfo{
		err: &os.PathError{Op: "lstat", Path: "/tmp/ccTEST.cdtor.c", Err: os.ErrNotExist},
	})
	assert.Nil(t, err)
	assert.False(t, ok)
	assert.Nil(t, info)

	_, _, err = localDirEntryInfo(fakeDirEntryInfo{err: fs.ErrPermission})
	assert.ErrorIs(t, err, fs.ErrPermission)
}

type fakeDirEntryInfo struct {
	err error
}

func (f fakeDirEntryInfo) Name() string {
	return "transient"
}

func (f fakeDirEntryInfo) IsDir() bool {
	return false
}

func (f fakeDirEntryInfo) Type() fs.FileMode {
	return 0
}

func (f fakeDirEntryInfo) Info() (fs.FileInfo, error) {
	return nil, f.err
}

func BenchmarkLocalFS(b *testing.B) {
	ctx := context.Background()
	benchmarkFileService(ctx, b, func() FileService {
		dir := b.TempDir()
		fs, err := NewLocalFS(ctx, "local", dir, DisabledCacheConfig, nil)
		assert.Nil(b, err)
		return fs
	})
}

func BenchmarkLocalFSAllocateCacheData(b *testing.B) {
	ctx := context.Background()
	fs, err := NewLocalFS(
		ctx,
		"local",
		b.TempDir(),
		CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024)},
		nil,
	)
	assert.NoError(b, err)
	b.Cleanup(func() { fs.Close(ctx) })

	benchmarkFileServiceAllocateCacheData(b, fs.AllocateCacheData, 42, 1)
}

func BenchmarkLocalFSAllocateCacheDataHighCardinality(b *testing.B) {
	ctx := context.Background()
	fs, err := NewLocalFS(
		ctx,
		"local",
		b.TempDir(),
		CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024)},
		nil,
	)
	assert.NoError(b, err)
	b.Cleanup(func() { fs.Close(ctx) })

	benchmarkFileServiceAllocateCacheData(b, fs.AllocateCacheData, 1, 1024)
}

func TestLocalFSWithDiskCache(t *testing.T) {
	ctx := context.Background()
	var counter perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counter)
	const (
		n       = 32
		dataLen = 32
	)

	// new fs
	fs, err := NewLocalFS(
		ctx,
		"foo",
		t.TempDir(),
		CacheConfig{
			DiskPath:                  ptrTo(t.TempDir()),
			DiskCapacity:              ptrTo[toml.ByteSize](dataLen * n / 32),
			enableDiskCacheForLocalFS: true,
		},
		nil,
	)
	assert.Nil(t, err)
	defer fs.Close(ctx)

	// prepare data
	datas := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		data := make([]byte, dataLen)
		_, err := rand.Read(data)
		assert.Nil(t, err)
		datas = append(datas, data)
	}

	// write
	for i := 0; i < n; i++ {
		data := datas[i]
		vec := IOVector{
			FilePath: fmt.Sprintf("%d", i),
			Entries: []IOEntry{
				{
					Data: data,
					Size: int64(len(data)),
				},
			},
		}
		err := fs.Write(ctx, vec)
		assert.Nil(t, err)
	}

	// read
	for i := 0; i < n*10; i++ {
		idx := i % n
		expected := datas[idx]
		length := 8
		for j := 0; j < dataLen/length; j++ {
			offset := j * length
			vec := IOVector{
				FilePath: fmt.Sprintf("%d", idx),
				Entries: []IOEntry{
					{
						Offset: int64(offset),
						Size:   int64(length),
					},
				},
			}
			err := fs.Read(ctx, &vec)
			assert.Nil(t, err)
			assert.Equal(t, expected[offset:offset+length], vec.Entries[0].Data)
			vec.Release()
		}
	}

}

// default allocator must use with no memcache
// only memory obtained through memcache.Alloc can be set to memcache
func TestLocalFSWithIOVectorCache(t *testing.T) {
	ctx := context.Background()
	memCache1 := NewMemCache(
		fscache.ConstCapacity(1<<20), nil,
		nil,
		"",
	)
	defer memCache1.Close(ctx)
	memCache2 := NewMemCache(
		fscache.ConstCapacity(1<<20), nil,
		nil,
		"",
	)
	defer memCache2.Close(ctx)
	caches := []IOVectorCache{memCache1, memCache2}

	dir := t.TempDir()
	fs, err := NewLocalFS(ctx, "test", dir, CacheConfig{
		MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024),
	}, nil)
	assert.Nil(t, err)

	err = fs.Write(ctx, IOVector{
		Caches:   caches,
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 8,
				Data: []byte("12345678"),
			},
		},
	})
	assert.Nil(t, err)

	vec := IOVector{
		Caches:   caches,
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 8,
				ToCacheData: func(ctx context.Context, r io.Reader, _ []byte, allocator CacheDataAllocator) (fscache.Data, error) {
					cacheData := allocator.AllocateCacheData(ctx, 8)
					_, err := io.ReadFull(r, cacheData.Bytes())
					if err != nil {
						return nil, err
					}
					return cacheData, nil
				},
			},
		},
	}
	err = fs.Read(ctx, &vec)
	assert.Nil(t, err)
	vec.Release()

	backingSize := int64(DefaultCacheDataAllocator().BackingSize(8))
	assert.Equal(t, backingSize, memCache1.cache.Used())
	assert.Equal(t, backingSize, memCache2.cache.Used())
	memCache1.cache.Flush(ctx)
	memCache2.cache.Flush(ctx)
	fs.FlushCache(ctx)
}

func TestLocalFSEmptyRootPath(t *testing.T) {
	fs, err := NewLocalFS(
		context.Background(),
		"test",
		"",
		CacheConfig{},
		nil,
	)
	assert.Nil(t, err)
	assert.NotNil(t, fs)
}

func TestLocalFSOpenFile(t *testing.T) {
	ctx := context.Background()
	var counter perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counter)
	const (
		n       = 32
		dataLen = 32
	)

	// new fs
	fs, err := NewLocalFS(
		ctx,
		"foo",
		t.TempDir(),
		CacheConfig{
			DiskPath:                  ptrTo(t.TempDir()),
			DiskCapacity:              ptrTo[toml.ByteSize](dataLen * n / 32),
			enableDiskCacheForLocalFS: true,
		},
		nil,
	)
	assert.Nil(t, err)

	f, err := fs.CreateFile(ctx, "aaa.txt")
	assert.Nil(t, err)

	f.Write([]byte("0123456789"))
	f.Close()

	f, err = fs.OpenFile(ctx, "aaa.txt")
	assert.Nil(t, err)

	buf := make([]byte, 10)
	f.Read(buf)
	assert.Equal(t, []byte("0123456789"), buf)
	f.Close()

	err = fs.RemoveFile(ctx, "aaa.txt")
	assert.Nil(t, err)

	// removed, open should fail.
	f, err = fs.OpenFile(ctx, "aaa.txt")
	assert.NotNil(t, err)

	f2, err := fs.CreateAndRemoveFile(ctx, "aaa2.txt")
	assert.Nil(t, err)
	f2.Write([]byte("0123456789"))
	nn, err := f2.ReadAt(buf, 0)
	assert.Nil(t, err)
	assert.Equal(t, 10, nn)
	assert.Equal(t, []byte("0123456789"), buf)
	f2.Close()

	f2, err = fs.OpenFile(ctx, "aaa2.txt")
	assert.NotNil(t, err)
}
