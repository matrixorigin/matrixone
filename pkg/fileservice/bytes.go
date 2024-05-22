// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"
)

type Bytes struct {
	bytes       []byte
	ptr         unsafe.Pointer
	deallocator malloc.Deallocator
}

func (b Bytes) Size() int64 {
	return int64(len(b.bytes))
}

func (b Bytes) Bytes() []byte {
	return b.bytes
}

func (b Bytes) Slice(length int) memorycache.CacheData {
	b.bytes = b.bytes[:length]
	return b
}

func (b Bytes) Release() {
	if b.deallocator != nil {
		b.deallocator.Deallocate(b.ptr)
	}
}

type bytesAllocator struct {
	allocator malloc.Allocator
}

var _ CacheDataAllocator = new(bytesAllocator)

func (b *bytesAllocator) Alloc(size int) memorycache.CacheData {
	ptr, dec := b.allocator.Allocate(uint64(size))
	return Bytes{
		bytes:       unsafe.Slice((*byte)(ptr), size),
		ptr:         ptr,
		deallocator: dec,
	}
}
