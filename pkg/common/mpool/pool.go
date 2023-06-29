// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mpool

import (
	"sync/atomic"
	"unsafe"
)

const (
	// 4k may be a good size for page
	PageSize = 4 << 10
	// 1MB may be a good size for chunk
	ChunkSize = 1 << 20
	WordSize  = 8
)

type page struct {
	slotSize int64
	head     int64
	data     []byte
	// start pointer
	ptr uintptr
}

type chunk struct {
	pages []page
	data  []byte
	// start pointer
	ptr uintptr
}

type pool struct {
	data   []byte
	chunks []chunk
	// start pointer
	ptr uintptr
}

func newPool(size int) *pool {
	if size < ChunkSize {
		size = ChunkSize
	}
	chunks := make([]chunk, size/ChunkSize)
	data := make([]byte, ChunkSize*len(chunks))
	for i := range chunks {
		chunks[i] = newChunk(data[i*ChunkSize:])
	}
	return &pool{
		data:   data,
		chunks: chunks,
		ptr:    uintptr(unsafe.Pointer(&data[0])),
	}
}

func alloc[T any](p *pool) *T {
	var v T

	sz := round(int(unsafe.Sizeof(v) + WordSize))
	if data := p.alloc(sz); data != nil {
		return (*T)(unsafe.Pointer(&data[0]))
	}
	return new(T)
}

func free[T any](p *pool, v *T) {
	p.free(uintptr(unsafe.Pointer(v)))
}

func (p *pool) alloc(sz int) []byte {
	for i := range p.chunks {
		chunk := &p.chunks[i]
		if data := chunk.alloc(sz); data != nil {
			return data
		}
	}
	return nil
}

func (p *pool) free(ptr uintptr) {
	if ptr >= p.ptr && ptr < p.ptr+uintptr(len(p.data)) {
		chunk := &p.chunks[(ptr-p.ptr)/ChunkSize]
		chunk.free(ptr)
		return
	}
}

func (ck *chunk) alloc(sz int) []byte {
	for i := range ck.pages {
		pg := &ck.pages[i]
		if slotSize := atomic.LoadInt64(&pg.slotSize); slotSize == -1 {
			for { // ensure that only one caller has exclusive access to this page
				if atomic.CompareAndSwapInt64(&pg.slotSize, -1, int64(sz)) {
					pg.init(sz)
					return pg.alloc()
				}
				// failure to seize, abandonment
				if atomic.LoadInt64(&pg.slotSize) != -1 {
					return nil
				}
			}
		} else if slotSize != int64(sz) {
			continue
		}
		if data := pg.alloc(); data != nil {
			return data
		}
	}
	return nil
}

func (ck *chunk) free(ptr uintptr) {
	pg := &ck.pages[(ptr-ck.ptr)/PageSize]
	pg.free(ptr - WordSize)
}

func newChunk(data []byte) chunk {
	pages := make([]page, ChunkSize/PageSize)
	for i := range pages {
		pages[i] = newPage(data[i*PageSize : (i+1)*PageSize])
	}
	return chunk{
		data:  data,
		pages: pages,
		ptr:   uintptr(unsafe.Pointer(&data[0])),
	}
}

func newPage(data []byte) page {
	return page{
		slotSize: -1,
		data:     data,
		ptr:      uintptr(unsafe.Pointer(&data[0])),
	}
}

func (pg *page) init(sz int) {
	atomic.StoreInt64(&pg.head, -1)
	atomic.StoreInt64(&pg.slotSize, int64(sz))
	for i := 0; i+sz < PageSize; i += sz {
		atomic.StoreInt64((*int64)(unsafe.Pointer(&pg.data[i])),
			atomic.LoadInt64(&pg.head))
		atomic.StoreInt64(&pg.head, int64(i))
	}
}

func (pg *page) alloc() []byte {
	for {
		head := atomic.LoadInt64(&pg.head)
		if head == -1 {
			return nil
		}
		if ok := atomic.CompareAndSwapInt64(&pg.head, head,
			atomic.LoadInt64((*int64)(unsafe.Pointer(&pg.data[head])))); ok {
			return pg.data[head+WordSize:]
		}
	}
}

func (pg *page) free(ptr uintptr) {
	ptr -= pg.ptr
	for {
		head := atomic.LoadInt64(&pg.head)
		if ok := atomic.CompareAndSwapInt64(&pg.head, head, int64(ptr)); ok {
			atomic.StoreInt64((*int64)(unsafe.Pointer(&pg.data[ptr])), head)
			return
		}
	}
}

func round(x int) int {
	return ((x + 7) & (-8))
}
