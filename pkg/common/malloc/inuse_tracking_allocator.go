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
	"sync/atomic"
)

type InuseTrackingAllocator struct {
	upstream        Allocator
	inUse           atomic.Uint64
	onChange        func(uint64)
	deallocatorPool *ClosureDeallocatorPool[inuseTrackingDeallocatorArgs, *inuseTrackingDeallocatorArgs]
}

type inuseTrackingDeallocatorArgs struct {
	size uint64
}

func (inuseTrackingDeallocatorArgs) As(Trait) bool {
	return false
}

func NewInuseTrackingAllocator(
	upstream Allocator,
	onChange func(uint64),
) (ret *InuseTrackingAllocator) {
	ret = &InuseTrackingAllocator{
		upstream: upstream,
		onChange: onChange,
		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *inuseTrackingDeallocatorArgs) {
				n := ret.inUse.Add(-args.size)
				if onChange != nil {
					onChange(n)
				}
			},
		),
	}
	return ret
}

var _ Allocator = new(InuseTrackingAllocator)

func (s *InuseTrackingAllocator) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	ptr, dec, err := s.upstream.Allocate(size, hints)
	if err != nil {
		return nil, nil, err
	}

	n := s.inUse.Add(size)
	if s.onChange != nil {
		s.onChange(n)
	}

	return ptr, ChainDeallocator(
		dec,
		s.deallocatorPool.Get(inuseTrackingDeallocatorArgs{
			size: size,
		}),
	), nil
}
