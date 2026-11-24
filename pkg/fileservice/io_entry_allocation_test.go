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
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type limitedReadAllocator struct {
	successes int
	calls     int
	releases  int
}

func (a *limitedReadAllocator) Allocate(size uint64, _ malloc.Hints) ([]byte, malloc.Deallocator, error) {
	a.calls++
	if a.calls > a.successes {
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

func TestS3FSReadDiskCacheAllocationFailure(t *testing.T) {
	ctx := t.Context()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{
		Name:     "s3",
		Endpoint: "disk",
		Bucket:   t.TempDir(),
	}, CacheConfig{
		DiskPath:     ptrTo(t.TempDir()),
		DiskCapacity: ptrTo[toml.ByteSize](1 << 20),
	}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	for _, tc := range []struct {
		name      string
		cached    bool
		truncated bool
		exhausted bool
	}{
		{name: "persistent_enomem", cached: true, exhausted: true},
		{name: "cache_miss"},
		{name: "short_cache_read", cached: true, truncated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.name
			require.NoError(t, fs.Write(ctx, IOVector{
				FilePath: path,
				Entries:  []IOEntry{{Size: 1, Data: []byte("x")}},
				Policy:   SkipAllCache,
			}))
			if tc.cached {
				require.NoError(t, fs.PrefetchFile(ctx, path))
			}
			if tc.truncated {
				require.NoError(t, os.Truncate(fs.diskCache.pathForFile(path), 0))
			}

			readAllocator := &limitedReadAllocator{successes: 1}
			copyAllocator := &limitedReadAllocator{successes: 1}
			if tc.exhausted {
				readAllocator.successes = 0
				copyAllocator.successes = 0
			}
			cacheAllocator := &bytesAllocator{allocator: copyAllocator}
			vector := &IOVector{
				FilePath: path,
				Policy:   SkipMemoryCache | SkipDiskCacheWrites | SkipFullFilePreloads,
				Entries: []IOEntry{{
					Size: 1,
					ToCacheData: func(ctx context.Context, r io.Reader, data []byte, _ CacheDataAllocator) (fscache.Data, error) {
						return CacheOriginalData(ctx, r, data, cacheAllocator)
					},
				}},
			}
			t.Cleanup(vector.Release)
			counters := new(perfcounter.CounterSet)
			readCtx := perfcounter.WithCounterSet(context.WithValue(ctx, ioEntryAllocatorKey{}, readAllocator), counters)
			err := fs.Read(readCtx, vector)
			entry := &vector.Entries[0]

			if tc.exhausted {
				require.ErrorIs(t, err, syscall.ENOMEM)
				require.False(t, entry.done)
				require.Nil(t, entry.Data)
				require.Nil(t, entry.CachedData)
				require.Nil(t, entry.releaseData)
				require.Equal(t, 1, readAllocator.calls)
				require.Zero(t, copyAllocator.calls)
				require.Zero(t, counters.FileService.S3.Get.Load())
				return
			}

			require.NoError(t, err)
			require.NotNil(t, entry.CachedData)
			require.Equal(t, []byte("x"), entry.CachedData.Bytes())
			require.Equal(t, 1, copyAllocator.calls)
			require.EqualValues(t, 1, counters.FileService.S3.Get.Load())
			if tc.truncated {
				require.Equal(t, 1, readAllocator.calls)
				require.Equal(t, 1, readAllocator.releases)
			} else {
				require.Zero(t, readAllocator.calls)
			}
		})
	}
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
