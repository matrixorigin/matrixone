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
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type limitedReadAllocator struct {
	successes int
	calls     int
	releases  int
	err       error
}

func (a *limitedReadAllocator) Allocate(size uint64, _ malloc.Hints) ([]byte, malloc.Deallocator, error) {
	a.calls++
	if a.calls > a.successes {
		if a.err != nil {
			return nil, nil, a.err
		}
		return nil, nil, syscall.ENOMEM
	}
	return make([]byte, size), malloc.FuncDeallocator(func() { a.releases++ }), nil
}

func TestIOEntryReadBufferAllocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))

	t.Run("existing_buffer", func(t *testing.T) {
		file, err := os.Open(path)
		require.NoError(t, err)
		defer file.Close()

		allocator := new(limitedReadAllocator)
		ctx := context.WithValue(context.Background(), ioEntryAllocatorKey{}, allocator)
		entry := IOEntry{Size: 1, Data: make([]byte, 0, 1)}
		require.NoError(t, entry.ReadFromOSFile(ctx, file, nil))
		require.Equal(t, []byte("x"), entry.Data)
		require.True(t, entry.done)
		require.Zero(t, allocator.calls)
	})

	t.Run("needs_buffer", func(t *testing.T) {
		file, err := os.Open(path)
		require.NoError(t, err)
		defer file.Close()

		allocator := new(limitedReadAllocator)
		ctx := context.WithValue(context.Background(), ioEntryAllocatorKey{}, allocator)
		entry := IOEntry{Size: 1}
		require.ErrorIs(t, entry.ReadFromOSFile(ctx, file, nil), syscall.ENOMEM)
		require.Equal(t, 1, allocator.calls)
		require.False(t, entry.done)
		require.Nil(t, entry.Data)
		require.Nil(t, entry.releaseData)
		offset, err := file.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		require.Zero(t, offset)
	})
}

func TestLocalFSReadBufferAllocationRollback(t *testing.T) {
	ctx := context.Background()
	fs, err := NewLocalFS(ctx, "local", t.TempDir(), DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	const path = "input"
	require.NoError(t, fs.Write(ctx, IOVector{
		FilePath: path,
		Entries:  []IOEntry{{Size: 2, Data: []byte("xy")}},
	}))

	allocator := &limitedReadAllocator{successes: 1}
	vector := &IOVector{
		FilePath: path,
		Entries:  []IOEntry{{Size: 1}, {Offset: 1, Size: 1}},
	}
	t.Cleanup(func() {
		vector.Release()
		assert.Equal(t, 1, allocator.releases)
	})

	readCtx := context.WithValue(ctx, ioEntryAllocatorKey{}, allocator)
	require.ErrorIs(t, fs.Read(readCtx, vector), syscall.ENOMEM)
	require.Equal(t, 2, allocator.calls)
	require.Equal(t, 1, allocator.releases)
	require.Nil(t, vector.Entries[0].Data)
	require.Nil(t, vector.Entries[0].releaseData)
	require.False(t, vector.Entries[1].done)
}

func TestLocalFSReadPublishesReusedDataOnSuccess(t *testing.T) {
	ctx := context.Background()
	fs, err := NewLocalFS(ctx, "local", t.TempDir(), DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	const path = "input"
	require.NoError(t, fs.Write(ctx, IOVector{
		FilePath: path,
		Entries:  []IOEntry{{Size: 2, Data: []byte("xy")}},
	}))

	conversionErr := errors.New("cache conversion failed")
	for _, tc := range []struct {
		name    string
		size    int64
		convert func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error)
		wantErr error
	}{
		{name: "success", size: 2},
		{name: "short_read", size: 3, wantErr: io.ErrUnexpectedEOF},
		{
			name: "conversion_error",
			size: 2,
			convert: func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error) {
				return nil, conversionErr
			},
			wantErr: conversionErr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reserved := []byte("abc")[:1]
			vector := &IOVector{
				FilePath: path,
				Entries:  []IOEntry{{Size: tc.size, Data: reserved, ToCacheData: tc.convert}},
			}
			t.Cleanup(vector.Release)

			allocator := new(limitedReadAllocator)
			readCtx := context.WithValue(ctx, ioEntryAllocatorKey{}, allocator)
			err := fs.Read(readCtx, vector)
			entry := &vector.Entries[0]
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Len(t, entry.Data, len(reserved))
				require.False(t, entry.done)
				require.Nil(t, entry.CachedData)
			} else {
				require.NoError(t, err)
				require.Equal(t, []byte("xy"), entry.Data)
			}
			require.Same(t, &reserved[0], &entry.Data[0])
			require.Zero(t, allocator.calls)
		})
	}
}

func newAllocationTestS3FS(t *testing.T) *S3FS {
	t.Helper()
	ctx := context.Background()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{
		Name: "allocation-test", Endpoint: "disk", Bucket: t.TempDir(),
	}, CacheConfig{
		MemoryCapacity: ptrTo[toml.ByteSize](DisableCacheCapacity),
		DiskPath:       ptrTo(t.TempDir()),
		DiskCapacity:   ptrTo[toml.ByteSize](1 << 20),
	}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	return fs
}

func TestS3FSReadDiskCacheAllocationFailure(t *testing.T) {
	for _, cacheType := range []string{"range", "full"} {
		t.Run(cacheType, func(t *testing.T) {
			ctx := context.Background()
			fs := newAllocationTestS3FS(t)
			const name = "object"
			require.NoError(t, fs.Write(ctx, IOVector{
				FilePath: name, Entries: []IOEntry{{Size: 2, Data: []byte("xy")}}, Policy: SkipAllCache,
			}))
			if cacheType == "range" {
				require.NoError(t, fs.diskCache.Update(ctx, &IOVector{
					FilePath: name, Entries: []IOEntry{{Size: 2, Data: []byte("xy")}}, Policy: SkipFullFilePreloads,
				}, false))
			} else {
				require.NoError(t, fs.diskCache.SetFile(ctx, name, func(context.Context) (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("xy")), nil
				}))
			}
			storage := &readRangeRecordingObjectStorage{ObjectStorage: fs.storage}
			fs.storage = storage
			converted := false
			vector := &IOVector{FilePath: name, Policy: SkipFullFilePreloads, Entries: []IOEntry{{
				Size: 2,
				ToCacheData: func(ctx context.Context, r io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
					converted = true
					return CacheOriginalData(ctx, r, data, allocator)
				},
			}}}
			t.Cleanup(vector.Release)
			allocator := &limitedReadAllocator{err: fmt.Errorf("allocate: %w", syscall.ENOMEM)}
			err := fs.Read(context.WithValue(ctx, ioEntryAllocatorKey{}, allocator), vector)
			require.ErrorIs(t, err, syscall.ENOMEM)
			require.Equal(t, 1, allocator.calls)
			require.False(t, converted)
			_, read := storage.lastRead()
			require.False(t, read, "allocation failure must not fall back to object storage")
			require.False(t, vector.Entries[0].done)
			require.Nil(t, vector.Entries[0].Data)
			require.Nil(t, vector.Entries[0].releaseData)

			// A later attempt can use the same vector and cached bytes once the
			// allocation failure has cleared.
			require.NoError(t, fs.Read(ctx, vector))
			require.True(t, converted)
			require.Equal(t, []byte("xy"), vector.Entries[0].CachedData.Bytes())
			_, read = storage.lastRead()
			require.False(t, read)
		})
	}
}

func TestS3FSReadDiskCacheShortReadFallsBack(t *testing.T) {
	ctx := context.Background()
	fs := newAllocationTestS3FS(t)
	const name = "object"
	entry := IOEntry{Size: 2, Data: []byte("xy")}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: name, Entries: []IOEntry{entry}, Policy: SkipAllCache}))
	require.NoError(t, fs.diskCache.Update(ctx, &IOVector{
		FilePath: name, Entries: []IOEntry{entry}, Policy: SkipFullFilePreloads,
	}, false))
	require.NoError(t, os.Truncate(fs.diskCache.pathForIOEntry(name, entry), 1))
	storage := &readRangeRecordingObjectStorage{ObjectStorage: fs.storage}
	fs.storage = storage
	vector := &IOVector{FilePath: name, Policy: SkipFullFilePreloads, Entries: []IOEntry{{Size: 2}}}
	t.Cleanup(vector.Release)
	require.NoError(t, fs.Read(ctx, vector))
	require.Equal(t, []byte("xy"), vector.Entries[0].Data)
	_, read := storage.lastRead()
	require.True(t, read, "short cache read should fall back to object storage")
}

func TestDiskCacheAllocationFailureKeepsEarlierHitOwned(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close(ctx) })
	const name = "object"
	require.NoError(t, cache.Update(ctx, &IOVector{
		FilePath: name, Policy: SkipFullFilePreloads,
		Entries: []IOEntry{{Size: 1, Data: []byte("x")}, {Offset: 1, Size: 1, Data: []byte("y")}},
	}, false))
	allocator := &limitedReadAllocator{successes: 1}
	readCtx := context.WithValue(ctx, ioEntryAllocatorKey{}, allocator)
	var reader io.ReadCloser
	vector := &IOVector{FilePath: name, Entries: []IOEntry{
		{Size: 1, ReadCloserForRead: &reader}, {Offset: 1, Size: 1},
	}}
	t.Cleanup(vector.ReleaseReadResultOnError)
	err = cache.Read(readCtx, vector)
	require.ErrorIs(t, err, syscall.ENOMEM)
	require.True(t, vector.Entries[0].done)
	require.Equal(t, []byte("x"), vector.Entries[0].Data)
	require.NotNil(t, reader)
	require.False(t, vector.Entries[1].done)
	require.Zero(t, allocator.releases)
	vector.ReleaseReadResultOnError()
	require.Equal(t, 1, allocator.releases)
	require.Nil(t, vector.Entries[0].Data)
	require.Nil(t, reader)

	// A retry must allocate a fresh buffer instead of reusing released memory.
	allocator.successes = allocator.calls + 2
	require.NoError(t, cache.Read(readCtx, vector))
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("x"), got)
	require.Equal(t, []byte("y"), vector.Entries[1].Data)
	vector.ReleaseReadResultOnError()
	require.Equal(t, 3, allocator.releases)
}

func TestDiskCacheDownloadAllocationFailure(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close(ctx) })
	diskPath := cache.pathForFile("obj")
	download := newPreloadGeneration(t, cache.preloads, diskPath, []byte("xy"))
	registerWholeFill(cache, diskPath, download)
	download.advance(0, 2)
	allocator := new(limitedReadAllocator)
	ctx = context.WithValue(ctx, ioEntryAllocatorKey{}, allocator)
	vector := &IOVector{FilePath: "obj", Entries: []IOEntry{{Size: 2}}}
	t.Cleanup(vector.ReleaseReadResultOnError)
	require.ErrorIs(t, cache.Read(ctx, vector), syscall.ENOMEM)
	require.False(t, vector.Entries[0].done)
	download.mu.Lock()
	readers := download.readers
	download.mu.Unlock()
	require.Zero(t, readers)
}
