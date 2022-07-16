// Copyright 2021 Matrix Origin
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

package objectio

import "sync"

type ObjectAllocator struct {
	mutex     sync.RWMutex
	available uint64
	pageSize  uint32
}

func p2roundup(x uint64, align uint64) uint64 {
	return -(-x & -align)
}

func NewObjectAllocator(capacity uint64, pageSize uint32) *ObjectAllocator {
	allocator := &ObjectAllocator{
		pageSize: pageSize,
	}
	allocator.Init(capacity, pageSize)
	return allocator
}

func (o *ObjectAllocator) Init(capacity uint64, pageSize uint32) {
	o.pageSize = pageSize
	o.available = 0
}

func (o *ObjectAllocator) Allocate(needLen uint64) (uint64, uint64) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	length := p2roundup(needLen, uint64(o.pageSize))
	offset := o.available
	o.available += length
	return offset, length
}

func (o *ObjectAllocator) GetAvailable() uint64 {
	o.mutex.RLock()
	defer o.mutex.RUnlock()
	return o.available
}
