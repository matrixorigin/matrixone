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
	"unsafe"

	"golang.org/x/sys/unix"
)

type ReadOnlyAllocator[U Allocator] struct {
	upstream        U
	deallocatorPool *ClosureDeallocatorPool[readOnlyDeallocatorArgs, *readOnlyDeallocatorArgs]
}

type readOnlyDeallocatorArgs struct {
	info   MmapInfo
	frozen bool
}

func (r *readOnlyDeallocatorArgs) As(trait Trait) bool {
	if ptr, ok := trait.(*Freezer); ok {
		ptr.freezer = r
		return true
	}
	return false
}

func (r *readOnlyDeallocatorArgs) Freeze() {
	r.frozen = true
	slice := unsafe.Slice(
		(*byte)(r.info.Addr),
		r.info.Length,
	)
	unix.Mprotect(slice, unix.PROT_READ)
}

type Freezer struct {
	freezer
}

type freezer interface {
	Freeze()
}

func (*Freezer) IsTrait() {}

func NewReadOnlyAllocator[U Allocator](
	upstream U,
) *ReadOnlyAllocator[U] {
	return &ReadOnlyAllocator[U]{
		upstream: upstream,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *readOnlyDeallocatorArgs) {
				if args.frozen {
					// unfreeze
					slice := unsafe.Slice(
						(*byte)(args.info.Addr),
						args.info.Length,
					)
					unix.Mprotect(slice, unix.PROT_READ|unix.PROT_WRITE)
				}
			},
		),
	}
}

var _ Allocator = new(ReadOnlyAllocator[Allocator])

func (r *ReadOnlyAllocator[U]) Allocate(size uint64, hint Hints) ([]byte, Deallocator, error) {
	bytes, dec, err := r.upstream.Allocate(size, hint)
	if err != nil {
		return nil, nil, err
	}

	info := mmapInfoPool.Get().(*MmapInfo)
	defer mmapInfoPool.Put(info)
	if !dec.As(info) {
		// not mmap allocated
		return bytes, dec, nil
	}

	return bytes, ChainDeallocator(
		dec,
		r.deallocatorPool.Get(readOnlyDeallocatorArgs{
			info: *info,
		}),
	), nil
}

var mmapInfoPool = sync.Pool{
	New: func() any {
		return new(MmapInfo)
	},
}
