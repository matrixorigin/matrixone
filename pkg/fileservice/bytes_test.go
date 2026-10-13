// Copyright 2024 Matrix Origin
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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytes(t *testing.T) {
	t.Run("Bytes without refs", func(t *testing.T) {
		bytes, deallocator, err := ioAllocator().Allocate(42, malloc.NoHints)
		assert.Nil(t, err)
		bs := &Bytes{
			bytes:       bytes,
			deallocator: deallocator,
		}
		bs.refs.Store(1)
		bs.Release()
	})
}

func TestBytesError(t *testing.T) {
	t.Run("Bytes get invalid memory", func(t *testing.T) {
		bytes, deallocator, err := ioAllocator().Allocate(42, malloc.NoHints)
		assert.Nil(t, err)
		bs := &Bytes{
			bytes:       bytes,
			deallocator: deallocator,
		}
		bs.refs.Store(1)

		// deallocate memory
		bs.Release()

		// nil pointer
		assert.Panics(t, func() { bs.Bytes() }, "get invalid memory")
	})

	t.Run("Bytes double free", func(t *testing.T) {
		bytes, deallocator, err := ioAllocator().Allocate(42, malloc.NoHints)
		assert.Nil(t, err)
		bs := &Bytes{
			bytes:       bytes,
			deallocator: deallocator,
		}
		bs.refs.Store(1)

		// deallocate memory
		bs.Release()

		// double free
		assert.Panics(t, func() { bs.Release() }, "double free")
	})

	t.Run("Bytes nil deallocator", func(t *testing.T) {
		data := []byte("123")
		bs := NewBytes(data)

		// deallocate memory
		bs.Release()

		assert.Panics(t, func() { bs.Release() }, "double free")
	})
}

// TestBytesResurrection: a released object must not be resurrectable.
// Regression for the sequence NewBytes -> Release -> Retain: before the CAS
// guard, Retain silently brought refs back to 1 and Bytes() returned the
// cleared slice without panicking, defeating both use-after-free and
// double-free detection.
func TestBytesResurrection(t *testing.T) {
	deallocated := 0
	bs := &Bytes{
		bytes:       []byte("123"),
		deallocator: malloc.FuncDeallocator(func() { deallocated++ }),
	}
	bs.refs.Store(1)

	bs.Release()
	assert.Equal(t, 1, deallocated, "exactly one deallocation")

	// Retain after the final Release must panic, not resurrect.
	assert.Panics(t, func() { bs.Retain() }, "retain after free")
	// And the object stays terminally dead.
	assert.Panics(t, func() { bs.Bytes() }, "use after free")
	assert.Panics(t, func() { bs.Slice(1) }, "slice after free")
	assert.Panics(t, func() { bs.Size() }, "size after free")
	assert.Panics(t, func() { bs.Capacity() }, "capacity after free")
	assert.Equal(t, 1, deallocated, "still exactly one deallocation")
}

// TestBytesOrderRetainThenRelease: deterministic order where Retain wins
// before the final Release. The object must stay alive for the retainer and
// be deallocated exactly once, by the last releaser.
func TestBytesOrderRetainThenRelease(t *testing.T) {
	deallocated := 0
	bs := &Bytes{
		bytes:       []byte("123"),
		deallocator: malloc.FuncDeallocator(func() { deallocated++ }),
	}
	bs.refs.Store(1)

	retained := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		bs.Retain() // wins while refs is still positive
		close(retained)
		assert.Equal(t, []byte("123"), bs.Bytes())
		bs.Release() // this becomes the final release
	}()

	<-retained
	bs.Release() // owner's release: not final, must not deallocate yet
	<-done

	assert.Equal(t, 1, deallocated, "exactly one deallocation")
	assert.Equal(t, int32(0), bs.refs.Load(), "terminal refcount")
	assert.Panics(t, func() { bs.Retain() }, "terminally dead")
}

// TestBytesOrderReleaseThenRetain: deterministic order where the final
// Release wins before the Retain. The late Retain must panic instead of
// resurrecting the freed object.
func TestBytesOrderReleaseThenRetain(t *testing.T) {
	deallocated := 0
	bs := &Bytes{
		bytes:       []byte("123"),
		deallocator: malloc.FuncDeallocator(func() { deallocated++ }),
	}
	bs.refs.Store(1)

	released := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		bs.Release() // final release wins
		close(released)
	}()

	<-released
	assert.Panics(t, func() { bs.Retain() }, "late retain must not resurrect")
	<-done

	assert.Equal(t, 1, deallocated, "exactly one deallocation")
}

// TestBytesConcurrent is a -race stress over mixed Retain/Release, with the
// deterministic orders covered by the two tests above. The base reference is
// held until all goroutines finish, then the terminal state is asserted.
func TestBytesConcurrent(t *testing.T) {
	deallocated := 0
	bs := &Bytes{
		bytes:       []byte("123"),
		deallocator: malloc.FuncDeallocator(func() { deallocated++ }),
	}
	bs.refs.Store(1)

	nthread := 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // barrier: maximize interleaving
			for j := 0; j < 100; j++ {
				bs.Retain()
				_ = bs.Bytes()
				bs.Release()
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, 0, deallocated, "alive while base reference held")
	bs.Release()
	assert.Equal(t, 1, deallocated, "exactly one deallocation")
	assert.Panics(t, func() { bs.Release() }, "double free")
}

func TestBytesSliceKeepsBackingCapacity(t *testing.T) {
	data := NewBytes(make([]byte, 700, 1024))
	defer data.Release()

	data.Slice(3)
	require.Equal(t, int64(3), data.Size())
	require.Equal(t, int64(1024), data.Capacity())
}

func TestDefaultCacheDataAllocatorReportsJemallocClassBackingSize(t *testing.T) {
	const request = 700 * 1024
	backingSize := DefaultCacheDataAllocator().BackingSize(request)
	require.GreaterOrEqual(t, backingSize, request)
	data := DefaultCacheDataAllocator().AllocateCacheData(context.Background(), request)
	defer data.Release()
	require.Equal(t, int64(backingSize), data.Capacity())
}

func TestFileServiceCacheDataAllocatorsReserveBackingCapacity(t *testing.T) {
	ctx := context.Background()
	const request = 10
	want := DefaultCacheDataAllocator().BackingSize(request)

	allocators := []struct {
		name string
		new  func(*MemCache) CacheDataAllocator
	}{
		{
			name: "local",
			new: func(cache *MemCache) CacheDataAllocator {
				return &LocalFS{memCache: cache}
			},
		},
		{
			name: "s3",
			new: func(cache *MemCache) CacheDataAllocator {
				return &S3FS{memCache: cache}
			},
		},
	}
	operations := []struct {
		name     string
		allocate func(CacheDataAllocator) fscache.Data
	}{
		{
			name: "allocate",
			allocate: func(allocator CacheDataAllocator) fscache.Data {
				return allocator.AllocateCacheData(ctx, request)
			},
		},
		{
			name: "allocate-with-hint",
			allocate: func(allocator CacheDataAllocator) fscache.Data {
				return allocator.AllocateCacheDataWithHint(ctx, request, malloc.NoClear)
			},
		},
		{
			name: "copy",
			allocate: func(allocator CacheDataAllocator) fscache.Data {
				return allocator.CopyToCacheData(ctx, make([]byte, request))
			},
		},
	}

	for _, allocatorTest := range allocators {
		for _, operation := range operations {
			t.Run(allocatorTest.name+"/"+operation.name, func(t *testing.T) {
				cache := NewMemCache(fscache.ConstCapacity(int64(want)), nil, nil, "")
				defer cache.Close(ctx)

				seed := NewBytes(make([]byte, 1))
				_, err := cache.cache.Set(ctx, fscache.CacheKey{Path: "seed", Sz: 1}, seed)
				require.NoError(t, err)
				seed.Release()

				allocator := allocatorTest.new(cache)
				require.Equal(t, want, allocator.BackingSize(request))

				data := operation.allocate(allocator)
				defer data.Release()
				require.Equal(t, int64(0), cache.cache.Used())
				require.Equal(t, int64(want), data.Capacity())
			})
		}
	}
}

func TestMemCachesUseIndependentJemallocArenas(t *testing.T) {
	first := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "first")
	defer first.Close(context.Background())
	second := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "second")
	defer second.Close(context.Background())

	require.NotNil(t, first.arenaAllocator)
	require.NotNil(t, second.arenaAllocator)
	require.NotSame(t, first.arenaAllocator, second.arenaAllocator)

	data := first.AllocateCacheData(context.Background(), 1024)
	defer data.Release()
	stats, err := first.arenaAllocator.Stats()
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats.Allocated, uint64(data.Capacity()))
}
