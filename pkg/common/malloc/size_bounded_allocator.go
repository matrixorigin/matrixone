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
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type SizeBoundedAllocator struct {
	upstream Allocator
	max      uint64
	counter  *atomic.Uint64
	funcPool sync.Pool
}

func NewSizeBoundedAllocator(upstream Allocator, maxSize uint64, counter *atomic.Uint64) *SizeBoundedAllocator {
	if counter == nil {
		counter = new(atomic.Uint64)
	}

	ret := &SizeBoundedAllocator{
		max:      maxSize,
		upstream: upstream,
		counter:  counter,
	}

	ret.funcPool = sync.Pool{
		New: func() any {
			argumented := new(argumentedFuncDeallocator[uint64])
			argumented.fn = func(_ unsafe.Pointer, hints Hints, size uint64) {
				ret.counter.Add(-size)
				ret.funcPool.Put(argumented)
			}
			return argumented
		},
	}

	return ret
}

var _ Allocator = new(SizeBoundedAllocator)

func (s *SizeBoundedAllocator) Allocate(size uint64, hints Hints) (unsafe.Pointer, Deallocator, error) {
	for {

		cur := s.counter.Load()
		newInuse := cur + size
		if newInuse > s.max {
			return nil, nil, moerr.NewInternalErrorNoCtx("out of space")
		}

		swapped := s.counter.CompareAndSwap(cur, newInuse)
		if !swapped {
			continue
		}

		ptr, dec, err := s.upstream.Allocate(size, hints)
		if err != nil {
			// give back
			s.counter.Add(-size)
			return nil, nil, err
		}

		fn := s.funcPool.Get().(*argumentedFuncDeallocator[uint64])
		fn.SetArgument(size)
		return ptr, ChainDeallocator(dec, fn), nil
	}

}
