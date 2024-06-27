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

type ManagedAllocator struct {
	upstream Allocator
	inUse    [256]sync.Map // ptr -> Deallocator
}

func NewManagedAllocator(
	upstream Allocator,
) *ManagedAllocator {
	return &ManagedAllocator{
		upstream: upstream,
	}
}

func (m *ManagedAllocator) Allocate(size uint64, hints Hints) ([]byte, error) {
	slice, dec, err := m.upstream.Allocate(size, hints)
	if err != nil {
		return nil, err
	}
	ptr := unsafe.Pointer(unsafe.SliceData(slice))
	shard := &m.inUse[hashPointer(uintptr(ptr))]
	shard.Store(ptr, dec)
	return slice, nil
}

func (m *ManagedAllocator) Deallocate(slice []byte, hints Hints) {
	ptr := unsafe.Pointer(unsafe.SliceData(slice))
	v, ok := m.inUse[hashPointer(uintptr(ptr))].LoadAndDelete(ptr)
	if !ok {
		panic("bad pointer")
	}
	v.(Deallocator).Deallocate(hints)
}

func hashPointer(ptr uintptr) uint8 {
	ret := uint8(ptr)
	ret ^= uint8(ptr >> 8)
	ret ^= uint8(ptr >> 16)
	ret ^= uint8(ptr >> 24)
	ret ^= uint8(ptr >> 32)
	ret ^= uint8(ptr >> 40)
	ret ^= uint8(ptr >> 48)
	ret ^= uint8(ptr >> 56)
	return ret
}
