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

package malloc

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

const simpleCAllocatorMmapCacheIdle = time.Second

// simpleCAllocatorMmapCache bridges the gap between libc arena reuse and
// immediate munmap. Free pages remain reclaimable by the kernel, the cache has
// a hard byte bound, and an idle timer eventually releases every mapping.
type simpleCAllocatorMmapCache struct {
	mu sync.Mutex

	bySize  map[uint64][][]byte
	bytes   uint64
	lastPut time.Time
	timer   *time.Timer
	// timerGeneration prevents a callback that was already runnable when drain
	// stopped its timer from acting on a later cache generation.
	timerGeneration uint64

	capacity         func() uint64
	idle             time.Duration
	cachedBytesGauge prometheus.Gauge
}

func newSimpleCAllocatorMmapCache(
	capacity func() uint64,
	idle time.Duration,
	cachedBytesGauge prometheus.Gauge,
) *simpleCAllocatorMmapCache {
	return &simpleCAllocatorMmapCache{
		bySize:           make(map[uint64][][]byte),
		capacity:         capacity,
		idle:             idle,
		cachedBytesGauge: cachedBytesGauge,
	}
}

func (c *simpleCAllocatorMmapCache) take(size uint64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entries := c.bySize[size]
	if len(entries) == 0 {
		return nil, false
	}

	last := len(entries) - 1
	slice := entries[last]
	entries[last] = nil
	if last == 0 {
		delete(c.bySize, size)
	} else {
		c.bySize[size] = entries[:last]
	}
	c.bytes -= size
	c.updateGaugeLocked()
	return slice, true
}

func (c *simpleCAllocatorMmapCache) put(slice []byte) bool {
	size := uint64(len(slice))
	if c.capacity == nil || size == 0 {
		return false
	}

	capacity := c.capacity()
	if size > capacity {
		return false
	}

	if !prepareSimpleCAllocatorMmapForCache(slice) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Re-evaluate under the lock because a runtime limit may have changed while
	// the pages were being prepared.
	capacity = c.capacity()
	if size > capacity || c.bytes > capacity-size {
		return false
	}

	c.bySize[size] = append(c.bySize[size], slice)
	c.bytes += size
	c.lastPut = time.Now()
	c.updateGaugeLocked()
	if c.timer == nil {
		c.timerGeneration++
		generation := c.timerGeneration
		c.timer = time.AfterFunc(c.idle, func() {
			c.expire(generation)
		})
	} else {
		c.timer.Reset(c.idle)
	}
	return true
}

func (c *simpleCAllocatorMmapCache) expire(generation uint64) {
	c.mu.Lock()
	// drain may have stopped the timer after its callback became runnable and
	// a subsequent put may already have created another timer.
	if c.timer == nil || generation != c.timerGeneration {
		c.mu.Unlock()
		return
	}
	if remaining := c.idle - time.Since(c.lastPut); remaining > 0 {
		c.timer.Reset(remaining)
		c.mu.Unlock()
		return
	}

	entries := c.bySize
	c.bySize = make(map[uint64][][]byte)
	c.bytes = 0
	c.timer = nil
	c.timerGeneration++
	c.updateGaugeLocked()
	c.mu.Unlock()

	unmapSimpleCAllocatorCacheEntries(entries)
}

func (c *simpleCAllocatorMmapCache) updateGaugeLocked() {
	if c.cachedBytesGauge != nil {
		c.cachedBytesGauge.Set(float64(c.bytes))
	}
}

func unmapSimpleCAllocatorCacheEntries(entries map[uint64][][]byte) {
	for _, slices := range entries {
		for _, slice := range slices {
			if err := unix.Munmap(slice); err != nil {
				panic(moerr.NewInternalErrorNoCtxf(
					"failed to unmap cached %d-byte allocation: %v",
					len(slice),
					err,
				))
			}
		}
	}
}
