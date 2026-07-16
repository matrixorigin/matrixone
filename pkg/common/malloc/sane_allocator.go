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
	"golang.org/x/sys/unix"
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

// Malloc does not clear the memory.
func (sca *SimpleCAllocator) Malloc(size uint64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	slice, err := allocateSimpleCAllocatorMemory(size, false)
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
	slice, err := allocateSimpleCAllocatorMemory(size, true)
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

// ReallocZero resizes old and zeros bytes beyond old's logical length.
func (sca *SimpleCAllocator) ReallocZero(old []byte, size uint64) ([]byte, error) {
	oldCapacity := uint64(cap(old))
	oldLength := uint64(len(old))

	if oldCapacity == 0 {
		if size == 0 {
			return nil, nil
		}
		return sca.Allocate(size)
	}

	if size == 0 {
		deallocateSimpleCAllocatorMemory(old, oldCapacity)
		sca.recordReallocation(oldCapacity, 0)
		sca.inuseObjects.Add(-1)
		sca.triggerUpdate()
		return nil, nil
	}

	if !simpleCAllocatorUsesMmap(oldCapacity) && !simpleCAllocatorUsesMmap(size) {
		oldptr := unsafe.Pointer(unsafe.SliceData(old))
		ptr := C.realloc(oldptr, C.ulong(size))
		if ptr == nil {
			return old, moerr.NewOOMNoCtx()
		}

		slice := unsafe.Slice((*byte)(ptr), size)
		if size > oldLength {
			clear(slice[oldLength:])
		}
		sca.recordReallocation(oldCapacity, size)
		sca.triggerUpdate()
		return slice, nil
	}

	// C.realloc cannot resize memory obtained from mmap, and allowing libc to
	// choose the destination allocator would reintroduce arena retention.
	// Allocate first so an allocation failure leaves old valid and owned by the
	// caller, then copy and release the old backing store.
	slice, err := allocateSimpleCAllocatorMemory(size, true)
	if err != nil {
		return old, err
	}
	copy(slice, old)
	deallocateSimpleCAllocatorMemory(old, oldCapacity)
	sca.recordReallocation(oldCapacity, size)
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

	deallocateSimpleCAllocatorMemory(slice, size)

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

func allocateSimpleCAllocatorMemory(size uint64, clearMemory bool) ([]byte, error) {
	if simpleCAllocatorUsesMmap(size) {
		if size > uint64(maxIntValue()) {
			return nil, moerr.NewOOMNoCtx()
		}
		slice, err := unix.Mmap(
			-1,
			0,
			int(size),
			unix.PROT_READ|unix.PROT_WRITE,
			unix.MAP_PRIVATE|unix.MAP_ANONYMOUS,
		)
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

func deallocateSimpleCAllocatorMemory(slice []byte, size uint64) {
	if simpleCAllocatorUsesMmap(size) {
		if size > uint64(maxIntValue()) {
			panic(moerr.NewInternalErrorNoCtxf("cannot unmap allocation larger than max int: %d", size))
		}
		if err := unix.Munmap(slice[:int(size)]); err != nil {
			panic(moerr.NewInternalErrorNoCtxf("failed to unmap %d-byte allocation: %v", size, err))
		}
		return
	}
	C.free(unsafe.Pointer(unsafe.SliceData(slice)))
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
