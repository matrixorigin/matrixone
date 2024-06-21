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
)

type fixedSizeSyncPoolAllocator struct {
	size uint64
	pool sync.Pool
}

func NewFixedSizeSyncPoolAllocator(size uint64) *fixedSizeSyncPoolAllocator {
	return &fixedSizeSyncPoolAllocator{
		size: size,
		pool: sync.Pool{
			New: func() any {
				ptr := unsafe.Pointer(unsafe.SliceData(make([]byte, size)))
				return ptr
			},
		},
	}
}

var _ FixedSizeAllocator = new(fixedSizeSyncPoolAllocator)

func (f *fixedSizeSyncPoolAllocator) Allocate(hint Hints) (unsafe.Pointer, Deallocator, error) {
	ptr := f.pool.Get().(unsafe.Pointer)
	if hint&NoClear == 0 {
		clear(unsafe.Slice((*byte)(ptr), f.size))
	}
	return ptr, f, nil
}

var _ Deallocator = new(fixedSizeSyncPoolAllocator)

func (f *fixedSizeSyncPoolAllocator) Deallocate(ptr unsafe.Pointer, hint Hints) {
	if hint&DoNotReuse > 0 {
		return
	}
	f.pool.Put(ptr)
}
