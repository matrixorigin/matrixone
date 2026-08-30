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
	"os"
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"golang.org/x/sys/unix"
)

const (
	// Small mappings can churn under metadata-heavy workloads. Reuse their
	// address space after returning resident pages to the OS. Larger cache
	// buffers are long-lived and are unmapped directly when evicted.
	hybridMmapPooledMaxSize    = 128 << 10
	hybridMmapReleasedPoolSize = 64
)

var hybridMmapPageSize = uint64(os.Getpagesize())

// HybridMmapAllocator is intended for long-lived cache buffers. Every request
// owns an independent, page-aligned mapping, so its backing size is known
// before allocation and remains attributable to that buffer for its lifetime.
// Small mappings reuse only MADV-released address space; large mappings are
// unmapped on release.
type HybridMmapAllocator struct {
	pools sync.Map // map[uint64]*fixedSizeMmapAllocator, keyed by page-rounded size

	deallocatorPool *ClosureDeallocatorPool[hybridMmapDeallocatorArgs, *hybridMmapDeallocatorArgs]
}

type hybridMmapDeallocatorArgs struct {
	length uint64
	ptr    unsafe.Pointer
}

func (h hybridMmapDeallocatorArgs) As(trait Trait) bool {
	if info, ok := trait.(*MmapInfo); ok {
		info.Addr = h.ptr
		info.Length = h.length
		return true
	}
	return false
}

func NewHybridMmapAllocator() *HybridMmapAllocator {
	ret := new(HybridMmapAllocator)
	ret.deallocatorPool = NewClosureDeallocatorPool(
		func(_ Hints, args *hybridMmapDeallocatorArgs) {
			slice := unsafe.Slice((*byte)(args.ptr), args.length)
			if err := unix.Munmap(slice); err != nil {
				panic(moerr.NewInternalErrorNoCtxf(
					"failed to unmap %d-byte cache allocation: %v",
					args.length,
					err,
				))
			}
		},
	)
	return ret
}

var _ Allocator = new(HybridMmapAllocator)

func HybridMmapAllocationSize(size uint64) (uint64, bool) {
	if size == 0 || size > maxClassSize {
		return 0, false
	}
	if size > ^uint64(0)-(hybridMmapPageSize-1) {
		return 0, false
	}
	return (size + hybridMmapPageSize - 1) &^ (hybridMmapPageSize - 1), true
}

func (h *HybridMmapAllocator) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	backingSize, ok := HybridMmapAllocationSize(size)
	if !ok {
		if size == 0 {
			return nil, nil, moerr.NewInternalErrorNoCtx("invalid allocate size: 0")
		}
		return nil, nil, moerr.NewInternalErrorNoCtxf("cannot allocate %v bytes: too large", size)
	}

	if backingSize <= hybridMmapPooledMaxSize && hints&DoNotReuse == 0 {
		pool, ok := h.pools.Load(backingSize)
		if !ok {
			candidate := newFixedSizeMmapAllocator(
				backingSize,
				0,
				hybridMmapReleasedPoolSize,
			)
			pool, _ = h.pools.LoadOrStore(backingSize, candidate)
		}
		slice, dec, err := pool.(*fixedSizeMmapAllocator).Allocate(hints, size)
		if err != nil {
			return nil, nil, err
		}
		return slice[:size], dec, nil
	}

	if backingSize > uint64(int(^uint(0)>>1)) {
		return nil, nil, moerr.NewInternalErrorNoCtxf("cannot allocate %v bytes: platform int overflow", size)
	}
	slice, err := unix.Mmap(
		-1,
		0,
		int(backingSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_PRIVATE|unix.MAP_ANONYMOUS,
	)
	if err != nil {
		return nil, nil, err
	}

	return slice[:size], h.deallocatorPool.Get(hybridMmapDeallocatorArgs{
		ptr:    unsafe.Pointer(unsafe.SliceData(slice)),
		length: backingSize,
	}), nil
}
