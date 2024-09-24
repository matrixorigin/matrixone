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

import "unsafe"

/*
#include <stdlib.h>
*/
import "C"

type CAllocator struct {
	deallocatorPool *ClosureDeallocatorPool[cDeallocatorArgs, *cDeallocatorArgs]
}

type cDeallocatorArgs struct {
	ptr unsafe.Pointer
}

func (cDeallocatorArgs) As(Trait) bool {
	return false
}

func NewCAllocator() *CAllocator {
	return &CAllocator{
		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *cDeallocatorArgs) {
				C.free(args.ptr)
			},
		),
	}
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
