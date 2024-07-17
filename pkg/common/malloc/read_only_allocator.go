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

type ReadOnlyAllocator struct {
	upstream        Allocator
	deallocatorPool *ClosureDeallocatorPool[readOnlyDeallocatorArgs]
}

type readOnlyDeallocatorArgs struct {
	slice  []byte
	frozen bool
}

func NewReadOnlyAllocator(
	upstream Allocator,
) *ReadOnlyAllocator {
	return &ReadOnlyAllocator{
		upstream: upstream,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *readOnlyDeallocatorArgs) {
				if args.frozen {
					//TODO unfreeze
				}
			},
		),
	}
}

var _ Allocator = new(ReadOnlyAllocator)

func (r *ReadOnlyAllocator) Allocate(size uint64, hint Hints) ([]byte, Deallocator, error) {
	bytes, dec, err := r.upstream.Allocate(size, hint)
	if err != nil {
		return nil, nil, err
	}

	return bytes, ChainDeallocator(
		dec,
		r.deallocatorPool.Get(readOnlyDeallocatorArgs{
			slice: bytes,
		}),
	), nil
}
