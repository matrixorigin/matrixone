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
	"fmt"
	"runtime"
	"sync/atomic"
	"unsafe"
)

type CheckedAllocator struct {
	upstream        Allocator
	fraction        uint32
	deallocatorPool *ClosureDeallocatorPool[checkedAllocatorArgs]
}

type checkedAllocatorArgs struct {
	deallocated  *atomic.Bool
	deallocator  Deallocator
	stacktraceID StacktraceID
	size         uint64
	ptr          unsafe.Pointer
}

func NewCheckedAllocator(
	upstream Allocator,
	fraction uint32,
) *CheckedAllocator {
	return &CheckedAllocator{
		upstream: upstream,
		fraction: fraction,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *checkedAllocatorArgs) {

				if args.deallocated == nil || !args.deallocated.CompareAndSwap(false, true) {
					panic(fmt.Sprintf(
						"double free: address %p, size %v, allocated at %s",
						args.ptr,
						args.size,
						args.stacktraceID,
					))
				}

				hints |= DoNotReuse
				args.deallocator.Deallocate(hints)

				// unref to allow finalizer
				args.deallocated = nil
			},
		),
	}

}

var _ Allocator = new(CheckedAllocator)

func (c *CheckedAllocator) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	ptr, dec, err := c.upstream.Allocate(size, hints)
	if err != nil {
		return nil, nil, err
	}

	if fastrand()%c.fraction > 0 {
		return ptr, dec, nil
	}

	stacktraceID := GetStacktraceID(0)
	deallocated := new(atomic.Bool) // this will not be GC until deallocator is called
	runtime.SetFinalizer(deallocated, func(deallocated *atomic.Bool) {
		if !deallocated.Load() {
			panic(fmt.Sprintf(
				"missing free: address %p, size %v, allocated at %s",
				ptr,
				size,
				stacktraceID,
			))
		}
	})

	dec = c.deallocatorPool.Get(checkedAllocatorArgs{
		deallocated:  deallocated,
		deallocator:  dec,
		stacktraceID: stacktraceID,
		size:         size,
		ptr:          unsafe.Pointer(unsafe.SliceData(ptr)),
	})

	return ptr, dec, nil
}
