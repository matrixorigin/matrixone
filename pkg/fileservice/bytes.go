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
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

type Bytes struct {
	mu          sync.Mutex
	bytes       []byte
	deallocator malloc.Deallocator
	_refs       int32
	refs        *int32
}

func (b *Bytes) Size() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bytes == nil {
		panic("fileservice.Bytes.Size() buffer already deallocated")
	}
	return int64(len(b.bytes))
}

func (b *Bytes) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bytes == nil {
		panic("fileservice.Bytes.Bytes() buffer already deallocated")
	}
	return b.bytes
}

func (b *Bytes) Slice(length int) fscache.Data {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bytes == nil {
		panic("fileservice.Bytes.Slice() buffer already deallocated")
	}
	b.bytes = b.bytes[:length]
	return b
}

func (b *Bytes) Retain() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.bytes == nil {
		panic("fileservice.Bytes.Retain() buffer already deallocated")
	}

	if b.refs != nil {
		(*b.refs) += 1
	}
}

func (b *Bytes) Release() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.bytes == nil {
		panic("fileservice.Bytes.Release() double free")
	}

	if b.refs != nil {
		(*b.refs) -= 1
		if *b.refs == 0 {
			if b.deallocator != nil {
				b.deallocator.Deallocate(malloc.NoHints)
				b.bytes = nil
				return true
			}
		}
	} else {
		if b.deallocator != nil {
			b.deallocator.Deallocate(malloc.NoHints)
			b.bytes = nil
			return true
		}
	}
	return false
}

type bytesAllocator struct {
	allocator malloc.Allocator
}

var _ CacheDataAllocator = new(bytesAllocator)

func (b *bytesAllocator) allocateCacheData(size int, hints malloc.Hints) fscache.Data {
	slice, dec, err := b.allocator.Allocate(uint64(size), hints)
	if err != nil {
		panic(err)
	}
	bytes := &Bytes{
		bytes:       slice,
		deallocator: dec,
	}
	bytes._refs = 1
	bytes.refs = &bytes._refs
	return bytes
}

func (b *bytesAllocator) AllocateCacheData(ctx context.Context, size int) fscache.Data {
	return b.allocateCacheData(size, malloc.NoHints)
}

func (b *bytesAllocator) AllocateCacheDataWithHint(ctx context.Context, size int, hints malloc.Hints) fscache.Data {
	return b.allocateCacheData(size, hints)
}

func (b *bytesAllocator) CopyToCacheData(ctx context.Context, data []byte) fscache.Data {
	ret := b.allocateCacheData(len(data), malloc.NoClear)
	copy(ret.Bytes(), data)
	return ret
}
