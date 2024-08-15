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

	"golang.org/x/sys/cpu"
)

type ManagedAllocator struct {
	upstream Allocator
	inUse    [256]managedAllocatorShard
}

type managedAllocatorShard struct {
	sync.Mutex
	items []managedAllocatorItem
	_     cpu.CacheLinePad
}

type managedAllocatorItem struct {
	ptr         unsafe.Pointer
	deallocator Deallocator
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
	shard.allocate(ptr, dec)
	return slice, nil
}

func (m *ManagedAllocator) Deallocate(slice []byte, hints Hints) {
	ptr := unsafe.Pointer(unsafe.SliceData(slice))
	shard := &m.inUse[hashPointer(uintptr(ptr))]
	shard.deallocate(ptr, hints)
}

func (m *managedAllocatorShard) allocate(ptr unsafe.Pointer, deallocator Deallocator) {
	m.Lock()
	defer m.Unlock()
	m.items = append(m.items, managedAllocatorItem{
		ptr:         ptr,
		deallocator: deallocator,
	})
}

func (m *managedAllocatorShard) deallocate(ptr unsafe.Pointer, hints Hints) {
	m.Lock()
	defer m.Unlock()
	for i := 0; i < len(m.items); i++ {
		if m.items[i].ptr == ptr {
			// found
			m.items[i].deallocator.Deallocate(hints)
			m.items[i] = m.items[len(m.items)-1]
			m.items = m.items[:len(m.items)-1]
			return
		}
	}
	panic("bad pointer")
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
