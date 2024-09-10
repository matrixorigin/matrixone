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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

type Bytes struct {
	bytes      []byte
	deallocate func()
	refs       *atomic.Int32
}

func (b Bytes) Size() int64 {
	return int64(len(b.bytes))
}

func (b Bytes) Bytes() []byte {
	return b.bytes
}

func (b Bytes) Slice(length int) fscache.Data {
	b.bytes = b.bytes[:length]
	return b
}

func (b Bytes) Retain() {
	if b.refs != nil {
		b.refs.Add(1)
	}
}

func (b Bytes) Release() {
	if b.refs != nil {
		if n := b.refs.Add(-1); n == 0 {
			if b.deallocate != nil {
				b.deallocate()
			}
		}
	} else {
		if b.deallocate != nil {
			b.deallocate()
		}
	}
}

type bytesAllocator struct {
	allocator malloc.Allocator
}

var _ CacheDataAllocator = new(bytesAllocator)

func (b *bytesAllocator) AllocateCacheData(size int) fscache.Data {
	slice, dec, err := b.allocator.Allocate(uint64(size), malloc.NoHints)
	if err != nil {
		panic(err)
	}
	var refs atomic.Int32
	refs.Store(1)
	return Bytes{
		bytes: slice,
		deallocate: sync.OnceFunc(func() {
			dec.Deallocate(malloc.NoHints)
		}),
		refs: &refs,
	}
}
