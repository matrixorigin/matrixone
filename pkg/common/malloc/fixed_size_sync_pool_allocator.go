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
	"sync"
)

type fixedSizeSyncPoolAllocator struct {
	size            uint64
	pool            sync.Pool
	deallocatorPool *ClosureDeallocatorPool[fixedSizeSyncPoolDeallocatorArgs, *fixedSizeSyncPoolDeallocatorArgs]
}

type fixedSizeSyncPoolDeallocatorArgs struct {
	slice *[]byte
}

func (fixedSizeSyncPoolDeallocatorArgs) As(Trait) bool {
	return false
}

func NewFixedSizeSyncPoolAllocator(size uint64) (ret *fixedSizeSyncPoolAllocator) {
	ret = &fixedSizeSyncPoolAllocator{
		size: size,

		pool: sync.Pool{
			New: func() any {
				slice := make([]byte, size)
				return &slice
			},
		},

		deallocatorPool: NewClosureDeallocatorPool(
			func(hint Hints, args *fixedSizeSyncPoolDeallocatorArgs) {
				if hint&DoNotReuse > 0 {
					return
				}
				ret.pool.Put(args.slice)
			},
		),
	}

	return
}

var _ FixedSizeAllocator = new(fixedSizeSyncPoolAllocator)

func (f *fixedSizeSyncPoolAllocator) Allocate(hint Hints) ([]byte, Deallocator, error) {
	slice := f.pool.Get().(*[]byte)
	if hint&NoClear == 0 {
		clear(*slice)
	}
	return *slice, f.deallocatorPool.Get(fixedSizeSyncPoolDeallocatorArgs{
		slice: slice,
	}), nil
}
