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
	deallocatorPool *ClosureDeallocatorPool[checkedAllocatorArgs, *checkedAllocatorArgs]
}

type checkedAllocatorArgs struct {
	deallocated *atomic.Bool
	deallocator Deallocator
	allocatePCs []uintptr
	size        uint64
	ptr         unsafe.Pointer
}

func (checkedAllocatorArgs) As(Trait) bool {
	return false
}

func NewCheckedAllocator(
	upstream Allocator,
) *CheckedAllocator {
	return &CheckedAllocator{
		upstream: upstream,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *checkedAllocatorArgs) {

				if args.deallocated == nil || !args.deallocated.CompareAndSwap(false, true) {
					panic(fmt.Sprintf(
						"double free: address %p, size %v, allocated at %s",
						args.ptr,
						args.size,
						pcsToString(args.allocatePCs),
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

	deallocated := new(atomic.Bool) // this will not be GC until deallocator is called
	var pcs []uintptr
	dec = c.deallocatorPool.Get2(func(args *checkedAllocatorArgs) checkedAllocatorArgs {
		pcs = args.allocatePCs
		if cap(pcs) < 32 {
			pcs = make([]uintptr, 32)
		} else {
			pcs = pcs[:cap(pcs)]
		}
		n := runtime.Callers(1, pcs)
		pcs = pcs[:n]
		return checkedAllocatorArgs{
			deallocated: deallocated,
			deallocator: dec,
			allocatePCs: pcs,
			size:        size,
			ptr:         unsafe.Pointer(unsafe.SliceData(ptr)),
		}
	})

	runtime.SetFinalizer(deallocated, func(deallocated *atomic.Bool) {
		if !deallocated.Load() {
			panic(fmt.Sprintf(
				"missing free: address %p, size %v, allocated at %s",
				ptr,
				size,
				pcsToString(pcs),
			))
		}
	})

	return ptr, dec, nil
}
