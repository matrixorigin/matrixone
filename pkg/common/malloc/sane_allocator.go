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

package malloc

import (
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/prometheus/client_golang/prometheus"
)

/*
#include <stdlib.h>
*/
import "C"

const (
	// simpleCAllocatorMmapThreshold keeps small, frequently reused allocations on
	// libc's fast path while ensuring large buffers have deterministic release
	// semantics. In particular, it avoids depending on libc's adaptive mmap
	// threshold, which can otherwise retain freed buffers in per-thread arenas.
	simpleCAllocatorMmapThreshold = 128 << 10
)

type SimpleCAllocator struct {
	allocateBytesCounter   prometheus.Counter
	inuseBytesGauge        prometheus.Gauge
	allocateObjectsCounter prometheus.Counter
	inuseObjectsGauge      prometheus.Gauge
	// absoluteInuseGauge publishes instantaneous in-use bytes (not deltas). Nil disables reporting.
	absoluteInuseGauge prometheus.Gauge

	// it is not clear if these shared counters are overengineering.
	allocateBytes   *ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	inuseBytes      *ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	allocateObjects *ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	inuseObjects    *ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	updating        atomic.Bool
	// currentInuse mirrors allocator in-use bytes and feeds absoluteInuseGauge.
	currentInuse atomic.Int64
	// libcFreedBytes accumulates small allocations returned to libc. libc may
	// retain those pages in per-thread arenas.
	libcFreedBytes atomic.Uint64
	libcTrimQueued atomic.Bool
	// The trim configuration is immutable after publication of the allocator.
	libcTrimThreshold uint64
	libcTrimCooldown  time.Duration
	libcTrim          func() bool
	libcTrimReleased  prometheus.Counter
	libcTrimNoop      prometheus.Counter

	// mmapCache is configured before the allocator is published and remains
	// immutable afterwards. It retains only free mmap-backed allocations.
	mmapCache *simpleCAllocatorMmapCache
}

func NewSimpleCAllocator(
	allocateBytesCounter prometheus.Counter,
	inuseBytesGauge prometheus.Gauge,
	allocateObjectsCounter prometheus.Counter,
	inuseObjectsGauge prometheus.Gauge,
	absoluteInuseGauge prometheus.Gauge,
) *SimpleCAllocator {
	sca := &SimpleCAllocator{
		allocateBytesCounter:   allocateBytesCounter,
		inuseBytesGauge:        inuseBytesGauge,
		allocateObjectsCounter: allocateObjectsCounter,
		inuseObjectsGauge:      inuseObjectsGauge,
		absoluteInuseGauge:     absoluteInuseGauge,
		allocateBytes:          NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0)),
		inuseBytes:             NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0)),
		allocateObjects:        NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0)),
		inuseObjects:           NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0)),
	}
	return sca
}

// EnableMmapCache retains recently freed mmap-backed allocations for exact-size
// reuse. capacity is evaluated on every insertion so callers can tie the hard
// bound to a runtime memory limit.
//
// EnableMmapCache must be called before the allocator is used concurrently.
func (sca *SimpleCAllocator) EnableMmapCache(
	capacity func() uint64,
	cachedBytesGauge prometheus.Gauge,
) {
	sca.mmapCache = newSimpleCAllocatorMmapCache(
		capacity,
		simpleCAllocatorMmapCacheIdle,
		cachedBytesGauge,
	)
}

// EnableLibcTrim periodically returns completely free libc arena pages to the
// OS after enough small-allocation churn. It must be called before the
// allocator is used concurrently. threshold amortizes the process-wide trim
// cost by released bytes; cooldown bounds it by time as well.
func (sca *SimpleCAllocator) EnableLibcTrim(
	threshold uint64,
	cooldown time.Duration,
	releasedCounter prometheus.Counter,
	noopCounter prometheus.Counter,
) {
	if threshold == 0 || cooldown <= 0 {
		panic("libc trim requires a positive threshold and cooldown")
	}
	sca.libcTrimThreshold = threshold
	sca.libcTrimCooldown = cooldown
	sca.libcTrim = trimCAllocator
	sca.libcTrimReleased = releasedCounter
	sca.libcTrimNoop = noopCounter
}

// Malloc does not clear the memory.
func (sca *SimpleCAllocator) Malloc(size uint64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	slice, err := sca.allocateMemory(size, false)
	if err != nil {
		return nil, err
	}
	sca.allocateBytes.Add(size)
	sca.inuseBytes.Add(int64(size))
	sca.currentInuse.Add(int64(size))
	sca.allocateObjects.Add(1)
	sca.inuseObjects.Add(1)
	sca.triggerUpdate()
	return slice, nil
}

// Allocate returns zeroed memory.
func (sca *SimpleCAllocator) Allocate(size uint64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	slice, err := sca.allocateMemory(size, true)
	if err != nil {
		return nil, err
	}
	sca.allocateBytes.Add(size)
	sca.inuseBytes.Add(int64(size))
	sca.currentInuse.Add(int64(size))
	sca.allocateObjects.Add(1)
	sca.inuseObjects.Add(1)
	sca.triggerUpdate()
	return slice, nil
}

// ReallocZero resizes an allocation whose stable backing size is oldSize and
// zeros bytes beyond old's logical length. old may be a reduced-capacity view;
// allocator provenance must never be inferred from that mutable view.
func (sca *SimpleCAllocator) ReallocZero(old []byte, oldSize, size uint64) ([]byte, error) {
	oldLength := uint64(len(old))

	if oldSize == 0 {
		if oldLength != 0 || cap(old) != 0 {
			return old, moerr.NewInternalErrorNoCtx(
				"non-empty allocation has zero recorded size",
			)
		}
		if size == 0 {
			return nil, nil
		}
		return sca.Allocate(size)
	}
	if unsafe.SliceData(old) == nil {
		return old, moerr.NewInternalErrorNoCtx(
			"recorded allocation has nil base pointer",
		)
	}
	if oldLength > oldSize || uint64(cap(old)) > oldSize {
		return old, moerr.NewInternalErrorNoCtxf(
			"allocation view exceeds recorded size, len %d, cap %d, recorded %d",
			oldLength,
			cap(old),
			oldSize,
		)
	}

	if size == 0 {
		sca.deallocateMemory(old, oldSize)
		sca.recordReallocation(oldSize, 0)
		sca.inuseObjects.Add(-1)
		sca.triggerUpdate()
		return nil, nil
	}

	if !simpleCAllocatorUsesMmap(oldSize) && !simpleCAllocatorUsesMmap(size) {
		oldptr := unsafe.Pointer(unsafe.SliceData(old))
		ptr := C.realloc(oldptr, C.ulong(size))
		if ptr == nil {
			return old, moerr.NewOOMNoCtx()
		}

		slice := unsafe.Slice((*byte)(ptr), size)
		if size > oldLength {
			clear(slice[oldLength:])
		}
		sca.recordReallocation(oldSize, size)
		sca.triggerUpdate()
		return slice, nil
	}

	// C.realloc cannot resize memory obtained from mmap, and allowing libc to
	// choose the destination allocator would reintroduce arena retention.
	// Allocate first so an allocation failure leaves old valid and owned by the
	// caller, then copy and release the old backing store.
	slice, err := sca.allocateMemory(size, true)
	if err != nil {
		return old, err
	}
	copy(slice, old)
	sca.deallocateMemory(old, oldSize)
	sca.recordReallocation(oldSize, size)
	sca.triggerUpdate()
	return slice, nil
}

func (sca *SimpleCAllocator) Deallocate(slice []byte, size uint64) {
	if cap(slice) == 0 {
		// free(nil) is a no-op.
		if size != 0 {
			panic(moerr.NewInternalErrorNoCtxf("deallocate size mismatch, expected %d, got 0", size))
		}
		return
	}

	if cap(slice) != int(size) {
		panic(moerr.NewInternalErrorNoCtxf("deallocate size mismatch, expected %d, got %d", size, cap(slice)))
	}

	sca.deallocateMemory(slice, size)

	sca.inuseBytes.Add(-int64(size))
	sca.currentInuse.Add(-int64(size))
	sca.inuseObjects.Add(-1)
	sca.triggerUpdate()
}

func (sca *SimpleCAllocator) recordReallocation(oldSize, newSize uint64) {
	if newSize > oldSize {
		delta := newSize - oldSize
		sca.allocateBytes.Add(delta)
		sca.inuseBytes.Add(int64(delta))
		sca.currentInuse.Add(int64(delta))
	} else if newSize < oldSize {
		delta := oldSize - newSize
		sca.inuseBytes.Add(-int64(delta))
		sca.currentInuse.Add(-int64(delta))
	}
}

func simpleCAllocatorUsesMmap(size uint64) bool {
	return size >= simpleCAllocatorMmapThreshold
}

func (sca *SimpleCAllocator) allocateMemory(size uint64, clearMemory bool) ([]byte, error) {
	if simpleCAllocatorUsesMmap(size) {
		if size > uint64(maxIntValue()) {
			return nil, moerr.NewOOMNoCtx()
		}
		// MADV_FREE pages remain reclaimable until written. Allocate clears the
		// whole mapping below and therefore safely reclaims ownership of every
		// page. Malloc cannot reuse them because its no-clear contract would
		// leave untouched pages reclaimable after they were handed to a caller.
		if clearMemory && sca.mmapCache != nil {
			if slice, ok := sca.mmapCache.take(size); ok {
				clear(slice)
				return slice, nil
			}
		}
		slice, err := mmapMemory(int(size))
		if err != nil {
			return nil, moerr.NewOOMNoCtx()
		}
		// Anonymous mappings are zero-filled by the kernel. This also satisfies
		// Malloc's weaker contract, which does not promise non-zero contents.
		return slice, nil
	}

	var ptr unsafe.Pointer
	if clearMemory {
		ptr = C.calloc(C.ulong(size), C.ulong(1))
	} else {
		ptr = C.malloc(C.ulong(size))
	}
	if ptr == nil {
		return nil, moerr.NewOOMNoCtx()
	}
	return unsafe.Slice((*byte)(ptr), size), nil
}

func (sca *SimpleCAllocator) deallocateMemory(slice []byte, size uint64) {
	ptr := unsafe.Pointer(unsafe.SliceData(slice))
	if simpleCAllocatorUsesMmap(size) {
		if size > uint64(maxIntValue()) {
			panic(moerr.NewInternalErrorNoCtxf("cannot unmap allocation larger than max int: %d", size))
		}
		fullAllocation := unsafe.Slice((*byte)(ptr), int(size))
		if sca.mmapCache != nil && sca.mmapCache.put(fullAllocation) {
			return
		}
		unmapMemory(fullAllocation)
		return
	}
	C.free(ptr)
	sca.recordLibcFree(size)
}

func maxIntValue() int {
	return int(^uint(0) >> 1)
}

func (sca *SimpleCAllocator) triggerUpdate() {
	const simpleCAllocatorUpdateWindow = time.Second
	if sca.updating.CompareAndSwap(false, true) {
		time.AfterFunc(simpleCAllocatorUpdateWindow, func() {
			if sca.allocateBytesCounter != nil {
				var n uint64
				sca.allocateBytes.Each(func(v *atomic.Uint64) {
					n += v.Swap(0)
				})
				sca.allocateBytesCounter.Add(float64(n))
			}

			if sca.inuseBytesGauge != nil {
				var n int64
				sca.inuseBytes.Each(func(v *atomic.Int64) {
					n += v.Swap(0)
				})
				sca.inuseBytesGauge.Add(float64(n))
			}

			if sca.allocateObjectsCounter != nil {
				var n uint64
				sca.allocateObjects.Each(func(v *atomic.Uint64) {
					n += v.Swap(0)
				})
				sca.allocateObjectsCounter.Add(float64(n))
			}

			if sca.inuseObjectsGauge != nil {
				var n int64
				sca.inuseObjects.Each(func(v *atomic.Int64) {
					n += v.Swap(0)
				})
				sca.inuseObjectsGauge.Add(float64(n))
			}

			// absolute off-heap in-use across all mpools sharing this allocator
			if sca.absoluteInuseGauge != nil {
				sca.absoluteInuseGauge.Set(float64(sca.currentInuse.Load()))
			}

			sca.updating.Store(false)
		})
	}
}

func (sca *SimpleCAllocator) recordLibcFree(size uint64) {
	if sca.libcTrimThreshold == 0 || sca.libcTrim == nil {
		return
	}
	if sca.libcFreedBytes.Add(size) < sca.libcTrimThreshold {
		return
	}
	sca.scheduleLibcTrim()
}

func (sca *SimpleCAllocator) scheduleLibcTrim() {
	if !sca.libcTrimQueued.CompareAndSwap(false, true) {
		return
	}
	time.AfterFunc(sca.libcTrimCooldown, func() {
		sca.tryLibcTrim()
		sca.libcTrimQueued.Store(false)
		// A release racing the callback may have crossed the threshold while
		// the callback still owned the queue token.
		if sca.libcFreedBytes.Load() >= sca.libcTrimThreshold {
			sca.scheduleLibcTrim()
		}
	})
}

func (sca *SimpleCAllocator) tryLibcTrim() bool {
	freed := sca.libcFreedBytes.Swap(0)
	if freed < sca.libcTrimThreshold {
		if freed != 0 {
			sca.libcFreedBytes.Add(freed)
		}
		return false
	}
	released := sca.libcTrim()
	if released {
		if sca.libcTrimReleased != nil {
			sca.libcTrimReleased.Inc()
		}
	} else if sca.libcTrimNoop != nil {
		sca.libcTrimNoop.Inc()
	}
	return true
}
