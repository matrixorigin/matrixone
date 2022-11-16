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
	"sync"
	"unsafe"
)

// Poor man's sync.Pool.  golang's sync.Pool is not good for our usage
// because it has no guanrantees, it may silently drop entries.
//
// The freelist is implemented as a stack.  Both put and get spin.
type node struct {
	next *node
}

type freelist struct {
	mut    sync.Mutex
	poolid int64
	head   *node
}

func make_freelist(poolid int64) *freelist {
	var fl freelist
	fl.poolid = poolid
	return &fl
}

// put always succeeds.  It is caller's job to make sure
// we never overflow the freelist
func (fl *freelist) put(ptr *memHdr) {
	fl.mut.Lock()
	defer fl.mut.Unlock()

	n := (*node)(unsafe.Pointer(ptr))
	n.next = fl.head
	fl.head = n
}

// get may return nil if there is no free item
func (fl *freelist) get() *memHdr {
	fl.mut.Lock()
	defer fl.mut.Unlock()

	if fl.head == nil {
		return nil
	}

	n := fl.head
	fl.head = n.next
	ret := (*memHdr)(unsafe.Pointer(n))
	ret.poolId = fl.poolid
	return ret
}
