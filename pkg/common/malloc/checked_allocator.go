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
	"sync"
	"sync/atomic"
	"unsafe"
)

type CheckedAllocator struct {
	upstream Allocator
	fraction uint32
	funcPool sync.Pool
}

type checkedAllocatorArgs struct {
	deallocated *atomic.Bool
	deallocator Deallocator
	stackID     uint64
	size        uint64
}

func NewCheckedAllocator(upstream Allocator, fraction uint32) *CheckedAllocator {
	ret := &CheckedAllocator{
		upstream: upstream,
		fraction: fraction,
	}

	ret.funcPool = sync.Pool{
		New: func() any {
			argumented := new(argumentedFuncDeallocator[checkedAllocatorArgs])
			argumented.fn = func(ptr unsafe.Pointer, hints Hints, args checkedAllocatorArgs) {

				if !args.deallocated.CompareAndSwap(false, true) {
					panic(fmt.Sprintf(
						"double free: address %p, size %v, allocated at %s",
						ptr,
						args.size,
						stackInfo(args.stackID),
					))
				}

				hints |= DoNotReuse
				args.deallocator.Deallocate(ptr, hints)

				ret.funcPool.Put(argumented)
			}
			return argumented
		},
	}

	return ret
}

var _ Allocator = new(CheckedAllocator)

func (c *CheckedAllocator) Allocate(size uint64, hints Hints) (unsafe.Pointer, Deallocator, error) {
	ptr, dec, err := c.upstream.Allocate(size, hints)
	if err != nil {
		return nil, nil, err
	}

	if fastrand()%c.fraction > 0 {
		return ptr, dec, nil
	}

	stackID := getStacktraceID(0)
	deallocated := new(atomic.Bool) // this will not be GC until deallocator is called
	runtime.SetFinalizer(deallocated, func(deallocated *atomic.Bool) {
		if !deallocated.Load() {
			panic(fmt.Sprintf(
				"missing free: address %p, size %v, allocated at %s",
				ptr,
				size,
				stackInfo(stackID),
			))
		}
	})

	fn := c.funcPool.Get().(*argumentedFuncDeallocator[checkedAllocatorArgs])
	fn.SetArgument(checkedAllocatorArgs{
		deallocated: deallocated,
		deallocator: dec,
		stackID:     stackID,
		size:        size,
	})
	return ptr, fn, nil
}
