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
// The freelist is implemented as a stack.  Both put and get spin.
// i replaced the original ring with the simplest non-locking stack
type freelist struct {
	cap int32
	len atomic.Int32
	top atomic.Pointer[node]
}

type node struct {
	next *node
	val  unsafe.Pointer
}

func make_freelist(cap int32) *freelist {
	var fl freelist
	fl.cap = cap
	return &fl
}

// put always succeeds.  It is caller's job to make sure
// we never overflow the freelist
func (fl *freelist) put(ptr unsafe.Pointer) {
	n := node{val: ptr}
	if fl.len.Load() == fl.cap {
		return
	}
	for {
		top := fl.top.Load()
		n.next = top
		if fl.top.CompareAndSwap(top, &n) {
			fl.len.Add(1)
			return
		}
	}
}

// get may return nil if there is no free item
func (fl *freelist) get() unsafe.Pointer {
	for {
		top := fl.top.Load()
		if top == nil {
			return nil
		}
		next := top.next
		if fl.top.CompareAndSwap(top, next) {
			fl.len.Add(-1)
			return top.val
		}
	}
}
