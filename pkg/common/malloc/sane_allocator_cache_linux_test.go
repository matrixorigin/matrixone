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

//go:build linux

package malloc

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newCachedTestSimpleCAllocator(
	capacity func() uint64,
	idle time.Duration,
) *SimpleCAllocator {
	allocator := newTestSimpleCAllocator()
	allocator.mmapCache = newSimpleCAllocatorMmapCache(capacity, idle, nil)
	return allocator
}

func (c *simpleCAllocatorMmapCache) cachedBytes() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes
}

func (c *simpleCAllocatorMmapCache) drain() {
	c.mu.Lock()
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.timerGeneration++
	entries := c.bySize
	c.bySize = make(map[uint64][][]byte)
	c.bytes = 0
	c.updateGaugeLocked()
	c.mu.Unlock()

	unmapSimpleCAllocatorCacheEntries(entries)
}

func TestSimpleCAllocatorMmapCacheExactSizeReuseAndZero(t *testing.T) {
	const size = simpleCAllocatorMmapThreshold
	allocator := newCachedTestSimpleCAllocator(
		func() uint64 { return size * 2 },
		time.Hour,
	)
	t.Cleanup(allocator.mmapCache.drain)

	slice, err := allocator.Allocate(size)
	require.NoError(t, err)
	for i := range slice {
		slice[i] = 0xff
	}
	base := &slice[0]

	allocator.Deallocate(slice, size)
	require.Equal(t, uint64(size), allocator.mmapCache.cachedBytes())

	reused, err := allocator.Allocate(size)
	require.NoError(t, err)
	require.True(t, base == &reused[0])
	require.Equal(t, make([]byte, size), reused)
	require.Zero(t, allocator.mmapCache.cachedBytes())

	allocator.Deallocate(reused, size)
}

func TestSimpleCAllocatorMmapCacheUsesExactSizes(t *testing.T) {
	const size = simpleCAllocatorMmapThreshold
	allocator := newCachedTestSimpleCAllocator(
		func() uint64 { return size * 4 },
		time.Hour,
	)
	t.Cleanup(allocator.mmapCache.drain)

	slice, err := allocator.Allocate(size)
	require.NoError(t, err)
	base := &slice[0]
	allocator.Deallocate(slice, size)

	different, err := allocator.Allocate(size + 1)
	require.NoError(t, err)
	require.False(t, base == &different[0])
	require.Equal(t, uint64(size), allocator.mmapCache.cachedBytes())
	allocator.Deallocate(different, size+1)
}

func TestSimpleCAllocatorMmapCacheDoesNotReuseForMalloc(t *testing.T) {
	const size = simpleCAllocatorMmapThreshold
	allocator := newCachedTestSimpleCAllocator(
		func() uint64 { return size * 2 },
		time.Hour,
	)
	t.Cleanup(allocator.mmapCache.drain)

	slice, err := allocator.Allocate(size)
	require.NoError(t, err)
	base := &slice[0]
	allocator.Deallocate(slice, size)

	uncleared, err := allocator.Malloc(size)
	require.NoError(t, err)
	require.False(t, base == &uncleared[0])
	require.Equal(t, uint64(size), allocator.mmapCache.cachedBytes())
	allocator.Deallocate(uncleared, size)
}

func TestSimpleCAllocatorMmapCacheReallocZero(t *testing.T) {
	const (
		oldSize = simpleCAllocatorMmapThreshold
		newSize = oldSize * 2
	)
	allocator := newCachedTestSimpleCAllocator(
		func() uint64 { return oldSize + newSize },
		time.Hour,
	)
	t.Cleanup(allocator.mmapCache.drain)

	destination, err := allocator.Allocate(newSize)
	require.NoError(t, err)
	destinationBase := &destination[0]
	allocator.Deallocate(destination, newSize)

	old, err := allocator.Allocate(oldSize)
	require.NoError(t, err)
	for i := range old {
		old[i] = 0x7f
	}

	resized, err := allocator.ReallocZero(old, oldSize, newSize)
	require.NoError(t, err)
	require.True(t, destinationBase == &resized[0])
	require.Equal(t, makeRepeatedByteSlice(oldSize, 0x7f), resized[:oldSize])
	require.Equal(t, make([]byte, oldSize), resized[oldSize:])
	require.Equal(t, uint64(oldSize), allocator.mmapCache.cachedBytes())

	allocator.Deallocate(resized, newSize)
	require.Zero(t, allocator.currentInuse.Load())
}

func TestSimpleCAllocatorMmapCacheCapacity(t *testing.T) {
	const size = simpleCAllocatorMmapThreshold
	allocator := newCachedTestSimpleCAllocator(
		func() uint64 { return size },
		time.Hour,
	)
	t.Cleanup(allocator.mmapCache.drain)

	first, err := allocator.Allocate(size)
	require.NoError(t, err)
	second, err := allocator.Allocate(size)
	require.NoError(t, err)

	allocator.Deallocate(first, size)
	allocator.Deallocate(second, size)
	require.Equal(t, uint64(size), allocator.mmapCache.cachedBytes())
}

func TestSimpleCAllocatorMmapCacheIdleExpiry(t *testing.T) {
	const size = simpleCAllocatorMmapThreshold
	allocator := newCachedTestSimpleCAllocator(
		func() uint64 { return size },
		10*time.Millisecond,
	)
	t.Cleanup(allocator.mmapCache.drain)

	slice, err := allocator.Allocate(size)
	require.NoError(t, err)
	allocator.Deallocate(slice, size)

	require.Eventually(
		t,
		func() bool {
			return allocator.mmapCache.cachedBytes() == 0
		},
		time.Second,
		10*time.Millisecond,
	)
}

func TestSimpleCAllocatorMmapCacheIgnoresExpiredTimerGeneration(t *testing.T) {
	const size = simpleCAllocatorMmapThreshold
	allocator := newCachedTestSimpleCAllocator(
		func() uint64 { return size },
		time.Hour,
	)
	t.Cleanup(allocator.mmapCache.drain)

	first, err := allocator.Allocate(size)
	require.NoError(t, err)
	allocator.Deallocate(first, size)

	allocator.mmapCache.mu.Lock()
	expiredGeneration := allocator.mmapCache.timerGeneration
	allocator.mmapCache.mu.Unlock()
	allocator.mmapCache.drain()

	second, err := allocator.Allocate(size)
	require.NoError(t, err)
	allocator.Deallocate(second, size)
	require.Equal(t, uint64(size), allocator.mmapCache.cachedBytes())

	// Model a callback that became runnable before drain stopped the old timer.
	// It must not release mappings owned by the new timer generation.
	allocator.mmapCache.expire(expiredGeneration)
	require.Equal(t, uint64(size), allocator.mmapCache.cachedBytes())
}

func TestSimpleCAllocatorMmapCacheConcurrentReuse(t *testing.T) {
	const (
		size       = simpleCAllocatorMmapThreshold
		goroutines = 8
		iterations = 32
	)
	allocator := newCachedTestSimpleCAllocator(
		func() uint64 { return size * goroutines },
		time.Hour,
	)
	t.Cleanup(allocator.mmapCache.drain)

	var waitGroup sync.WaitGroup
	errs := make(chan error, goroutines)
	waitGroup.Add(goroutines)
	for range goroutines {
		go func() {
			defer waitGroup.Done()
			for range iterations {
				slice, err := allocator.Allocate(size)
				if err != nil {
					errs <- err
					return
				}
				slice[0] = 1
				slice[len(slice)-1] = 2
				allocator.Deallocate(slice, size)
			}
		}()
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Zero(t, allocator.currentInuse.Load())
	require.LessOrEqual(
		t,
		allocator.mmapCache.cachedBytes(),
		uint64(size*goroutines),
	)
}
