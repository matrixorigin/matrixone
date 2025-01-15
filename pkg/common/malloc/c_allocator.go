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

//go:build cgo

package malloc

import (
	"sync/atomic"
	"unsafe"
)

/*
#include <stdlib.h>
#include <malloc.h>
*/
import "C"

func init() {
	// malloc tunings
	C.mallopt(C.M_TOP_PAD, 0)        // no sbrk padding
	C.mallopt(C.M_MMAP_THRESHOLD, 0) // always use mmap
}

type CAllocator struct {
	deallocatorPool *ClosureDeallocatorPool[cDeallocatorArgs, *cDeallocatorArgs]
	bytesFree       atomic.Uint64
}

type cDeallocatorArgs struct {
	ptr  unsafe.Pointer
	size uint64
}

func (cDeallocatorArgs) As(Trait) bool {
	return false
}

const (
	cMallocReturnToOSThreshold = 64 * MB
)

func NewCAllocator() (ret *CAllocator) {
	ret = &CAllocator{
		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *cDeallocatorArgs) {
				C.free(args.ptr)

				n := ret.bytesFree.Add(args.size)
				if n > cMallocReturnToOSThreshold {
					ret.bytesFree.Store(0)
					// return memory to OS
					C.malloc_trim(0)
				}

			},
		),
	}
	return
}

var _ Allocator = new(CAllocator)

func (c *CAllocator) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	ptr := C.malloc(C.ulong(size))
	if hints&NoClear == 0 {
		clear(unsafe.Slice((*byte)(ptr), size))
	}
	slice := unsafe.Slice((*byte)(ptr), size)
	return slice, c.deallocatorPool.Get(cDeallocatorArgs{
		ptr: ptr,
	}), nil
}
