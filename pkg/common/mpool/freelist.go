// Copyright 2021 - 2022 Matrix Origin
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

package mpool

import (
	"sync/atomic"
	"unsafe"
)

// Poor man's sync.Pool.  golang's sync.Pool is not good for our usage
// because it has no guanrantees, it may silently drop entries.
//
// The freelist is implemented as a ring buffer.  Both put and get spin.
type freelist struct {
	cap        int32
	head, tail atomic.Int32
	ptrs       []unsafe.Pointer
}

func make_freelist(cap int32) *freelist {
	var fl freelist
	fl.cap = cap
	// +1 so that we can tell empty from full.
	fl.ptrs = make([]unsafe.Pointer, cap+1)
	return &fl
}

// ring buffer next
func (fl *freelist) next(i int32) int32 {
	if i == fl.cap {
		return 0
	}
	return i + 1
}

// put always succeeds.  It is caller's job to make sure
// we never overflow the freelist
func (fl *freelist) put(ptr unsafe.Pointer) {
	for {
		curr := fl.tail.Load()
		next := fl.next(curr)
		ok := fl.tail.CompareAndSwap(curr, next)
		if ok {
			atomic.StorePointer(&fl.ptrs[curr], ptr)
			return
		}
	}
}

// get may return nil if there is no free item
func (fl *freelist) get() unsafe.Pointer {
	for {
		pos := fl.head.Load()
		if pos == fl.tail.Load() {
			// empty
			return nil
		}
		next := fl.next(pos)
		ptr := atomic.LoadPointer(&fl.ptrs[pos])
		ok := fl.head.CompareAndSwap(pos, next)
		if ok {
			return ptr
		}
	}
}
