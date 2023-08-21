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

// #include <stdlib.h>
import "C"
import (
	"sync/atomic"
)

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1<<50 - 1
)

// RCBytes represents a reference counting []byte from a pool
// newly created RCBytes' ref count is 1
// owner should call Release to give it back to the pool
// new sharing owner should call Retain to increase ref count
type RCBytes struct {
	data  []byte
	count atomic.Int32
	pool  *rcBytesPool
}

func (r *RCBytes) Bytes() []byte {
	return r.data
}

func (r *RCBytes) Slice(length int) CacheData {
	r.data = r.data[:length]
	return r
}

func (r *RCBytes) Retain() {
	r.count.Add(1)
}

func (r *RCBytes) Release() {
	if c := r.count.Add(-1); c == 0 {
		free(r.data)
		r.pool.size.Add(int64(cap(r.data)) * -1)
	} else if c < 0 {
		panic("bad release")
	}
}

type rcBytesPool struct {
	limit     int64
	size      atomic.Int64
	forceGCCh chan struct{}
}

func newRCBytesPool(limit int64) *rcBytesPool {
	return &rcBytesPool{limit: limit, forceGCCh: make(chan struct{})}
}

var _ CacheDataAllocator = new(rcBytesPool)

func (r *rcBytesPool) Size() int64 {
	return r.size.Load()
}

func (r *rcBytesPool) ForceGCChan() chan struct{} {
	return r.forceGCCh
}

func (r *rcBytesPool) Alloc(size int) CacheData {
	/*
		if r.size.Load() > r.limit {
			r.forceGCCh <- struct{}{}
			// forced cleanup of excess memory
			// runtime.GC()
			go debug.FreeOSMemory()
		}
	*/

	item := &RCBytes{
		pool: r,
		data: alloc(size),
	}
	r.size.Add(int64(size))
	item.Retain()
	return item
}

// the impact of gc and cgo is currently ambiguous and it is not clear how it will be handled for the time being
func alloc(size int) []byte {
	return make([]byte, size, size)
	/*
		ptr := C.calloc(C.size_t(size), 1)
		if ptr == nil {
			// NB: throw is like panic, except it guarantees the process will be
			// terminated. The call below is exactly what the Go runtime invokes when
			// it cannot allocate memory.
			panic("out of memory")
		}
		// Interpret the C pointer as a pointer to a Go array, then slice.
		return (*[MaxArrayLen]byte)(unsafe.Pointer(ptr))[:size:size]
	*/
}

// free frees the specified slice.
func free(b []byte) {
	/*
		if cap(b) != 0 {
			b = b[:cap(b)]
			ptr := unsafe.Pointer(&b[0])
			C.free(ptr)
		}
	*/
}
