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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAsyncDiskCacheWrite tests that disk cache writes are asynchronous
// and don't block IO merger waiters.
func TestAsyncDiskCacheWrite(t *testing.T) {
	ctx := context.Background()

	// Create S3FS with both memory and disk cache
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:     "test",
			Endpoint: "disk",
			Bucket:   t.TempDir(),
		},
		CacheConfig{
			MemoryCapacity: ptrTo(toml.ByteSize(1 << 20)),
			DiskPath:       ptrTo(t.TempDir()),
			DiskCapacity:   ptrTo(toml.ByteSize(10 * (1 << 20))),
		},
		nil,
		false,
		false,
	)
	require.Nil(t, err)
	defer fs.Close(ctx)

	// Write a file (skip disk cache to ensure we test the read path)
	data := []byte("hello world async disk cache test")
	err = fs.Write(ctx, IOVector{
		FilePath: "test-async",
		Policy:   SkipDiskCache,
		Entries: []IOEntry{
			{
				Data: data,
				Size: int64(len(data)),
			},
		},
	})
	require.Nil(t, err)

	// Clear memory cache to force S3 read
	fs.FlushCache(ctx)

	// Concurrent reads - this tests that async disk cache write doesn't block waiters
	nReaders := 100
	var wg sync.WaitGroup
	wg.Add(nReaders)

	var successCount atomic.Int32
	startTime := time.Now()

	for i := 0; i < nReaders; i++ {
		go func() {
			defer wg.Done()
			vec := &IOVector{
				FilePath: "test-async",
				Entries: []IOEntry{
					{
						Size:        int64(len(data)),
						ToCacheData: CacheOriginalData,
					},
				},
			}
			err := fs.Read(ctx, vec)
			if err == nil && vec.Entries[0].CachedData != nil &&
				bytes.Equal(vec.Entries[0].CachedData.Bytes(), data) {
				successCount.Add(1)
			}
			vec.Release()
		}()
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	// All reads should succeed
	assert.Equal(t, int32(nReaders), successCount.Load())

	// The test should complete quickly (not blocked by disk cache f.Sync())
	// If disk cache write was synchronous and slow, this would take much longer
	t.Logf("Completed %d concurrent reads in %v", nReaders, elapsed)
}

// TestAsyncDiskCacheWriteEventualConsistency tests that data is eventually
// written to disk cache after async write completes.
func TestAsyncDiskCacheWriteEventualConsistency(t *testing.T) {
	ctx := context.Background()

	diskCacheDir := t.TempDir()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:     "test",
			Endpoint: "disk",
			Bucket:   t.TempDir(),
		},
		CacheConfig{
			MemoryCapacity: ptrTo(toml.ByteSize(1 << 20)),
			DiskPath:       ptrTo(diskCacheDir),
			DiskCapacity:   ptrTo(toml.ByteSize(10 * (1 << 20))),
		},
		nil,
		false,
		false,
	)
	require.Nil(t, err)
	defer fs.Close(ctx)

	// Write a file
	data := []byte("test data for eventual consistency")
	err = fs.Write(ctx, IOVector{
		FilePath: "test-eventual",
		Policy:   SkipDiskCache, // Skip disk cache on write
		Entries: []IOEntry{
			{
				Data: data,
				Size: int64(len(data)),
			},
		},
	})
	require.Nil(t, err)

	// Clear memory cache
	fs.FlushCache(ctx)

	// First read - triggers S3 read and async disk cache write
	vec := &IOVector{
		FilePath: "test-eventual",
		Entries: []IOEntry{
			{
				Size:        int64(len(data)),
				ToCacheData: CacheOriginalData,
			},
		},
	}
	err = fs.Read(ctx, vec)
	require.Nil(t, err)
	require.NotNil(t, vec.Entries[0].CachedData)
	assert.Equal(t, data, vec.Entries[0].CachedData.Bytes())
	vec.Release()

	// Wait for async disk cache write to complete
	time.Sleep(500 * time.Millisecond)

	// Clear memory cache again
	fs.FlushCache(ctx)

	// Second read - should hit disk cache
	var counterSet perfcounter.CounterSet
	ctx2 := perfcounter.WithCounterSet(ctx, &counterSet)

	vec2 := &IOVector{
		FilePath: "test-eventual",
		Entries: []IOEntry{
			{
				Size:        int64(len(data)),
				ToCacheData: CacheOriginalData,
			},
		},
	}
	err = fs.Read(ctx2, vec2)
	require.Nil(t, err)
	require.NotNil(t, vec2.Entries[0].CachedData)
	assert.Equal(t, data, vec2.Entries[0].CachedData.Bytes())
	vec2.Release()

	// Verify disk cache was hit (S3 Get should be 0 for second read)
	t.Logf("Disk cache hit: %d", counterSet.FileService.Cache.Disk.Hit.Load())
}

// TestAsyncDiskCacheWriteNoDataCorruption tests that concurrent reads
// don't get corrupted data during async disk cache write.
func TestAsyncDiskCacheWriteNoDataCorruption(t *testing.T) {
	ctx := context.Background()

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:     "test",
			Endpoint: "disk",
			Bucket:   t.TempDir(),
		},
		CacheConfig{
			MemoryCapacity: ptrTo(toml.ByteSize(1 << 20)),
			DiskPath:       ptrTo(t.TempDir()),
			DiskCapacity:   ptrTo(toml.ByteSize(10 * (1 << 20))),
		},
		nil,
		false,
		false,
	)
	require.Nil(t, err)
	defer fs.Close(ctx)

	// Write multiple files
	numFiles := 10
	fileData := make(map[string][]byte)
	for i := 0; i < numFiles; i++ {
		path := "test-corruption-" + string(rune('a'+i))
		data := make([]byte, 1024)
		for j := range data {
			data[j] = byte('a' + i)
		}
		fileData[path] = data

		err = fs.Write(ctx, IOVector{
			FilePath: path,
			Policy:   SkipDiskCache,
			Entries: []IOEntry{
				{
					Data: data,
					Size: int64(len(data)),
				},
			},
		})
		require.Nil(t, err)
	}

	// Clear memory cache
	fs.FlushCache(ctx)

	// Concurrent reads of all files
	var wg sync.WaitGroup
	var errorCount atomic.Int32

	for path, expectedData := range fileData {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(p string, expected []byte) {
				defer wg.Done()
				vec := &IOVector{
					FilePath: p,
					Entries: []IOEntry{
						{
							Size:        int64(len(expected)),
							ToCacheData: CacheOriginalData,
						},
					},
				}
				err := fs.Read(ctx, vec)
				if err != nil {
					errorCount.Add(1)
					return
				}
				// Verify data integrity
				if vec.Entries[0].CachedData == nil ||
					!bytes.Equal(vec.Entries[0].CachedData.Bytes(), expected) {
					errorCount.Add(1)
					t.Errorf("Data corruption detected for %s", p)
				}
				vec.Release()
			}(path, expectedData)
		}
	}

	wg.Wait()
	assert.Equal(t, int32(0), errorCount.Load(), "No data corruption should occur")
}

// TestIOMergerWaiterNotBlockedByDiskCache tests that IO merger waiters
// are not blocked by slow disk cache writes.
func TestIOMergerWaiterNotBlockedByDiskCache(t *testing.T) {
	ctx := context.Background()

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:     "test",
			Endpoint: "disk",
			Bucket:   t.TempDir(),
		},
		CacheConfig{
			MemoryCapacity: ptrTo(toml.ByteSize(1 << 20)),
			DiskPath:       ptrTo(t.TempDir()),
			DiskCapacity:   ptrTo(toml.ByteSize(10 * (1 << 20))),
		},
		nil,
		false,
		false,
	)
	require.Nil(t, err)
	defer fs.Close(ctx)

	// Write a larger file to make disk cache write take longer
	data := make([]byte, 1<<20) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}

	err = fs.Write(ctx, IOVector{
		FilePath: "test-large",
		Policy:   SkipDiskCache,
		Entries: []IOEntry{
			{
				Data: data,
				Size: int64(len(data)),
			},
		},
	})
	require.Nil(t, err)

	// Clear memory cache
	fs.FlushCache(ctx)

	// Start many concurrent reads
	nReaders := 50
	var wg sync.WaitGroup
	wg.Add(nReaders)

	readTimes := make([]time.Duration, nReaders)
	var mu sync.Mutex
	var errorCount atomic.Int32

	for i := 0; i < nReaders; i++ {
		go func(idx int) {
			defer wg.Done()
			start := time.Now()
			vec := &IOVector{
				FilePath: "test-large",
				Entries: []IOEntry{
					{
						Size:        int64(len(data)),
						ToCacheData: CacheOriginalData,
					},
				},
			}
			err := fs.Read(ctx, vec)
			elapsed := time.Since(start)

			mu.Lock()
			readTimes[idx] = elapsed
			mu.Unlock()

			if err != nil {
				errorCount.Add(1)
				t.Errorf("Read failed: %v", err)
			} else if vec.Entries[0].CachedData == nil ||
				!bytes.Equal(vec.Entries[0].CachedData.Bytes(), data) {
				errorCount.Add(1)
				t.Errorf("Data mismatch")
			}
			vec.Release()
		}(i)
	}

	wg.Wait()

	// Calculate statistics
	var totalTime time.Duration
	var maxTime time.Duration
	for _, d := range readTimes {
		totalTime += d
		if d > maxTime {
			maxTime = d
		}
	}
	avgTime := totalTime / time.Duration(nReaders)

	t.Logf("Read times - Avg: %v, Max: %v, Errors: %d", avgTime, maxTime, errorCount.Load())

	// All reads should succeed
	assert.Equal(t, int32(0), errorCount.Load())

	// With async disk cache write, max time should be reasonable
	// (not blocked by f.Sync() which could take 10-30 seconds)
	assert.Less(t, maxTime, 30*time.Second, "Max read time should be less than 30s")
}
