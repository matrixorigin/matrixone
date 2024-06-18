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
	"fmt"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"unsafe"
)

type checkedDeallocator struct {
	upstream      Deallocator
	allocateStack []byte
	deallocated   atomic.Bool
}

func newCheckedDeallocator(upstream Deallocator) *checkedDeallocator {
	ret := &checkedDeallocator{
		upstream:      upstream,
		allocateStack: debug.Stack(),
	}

	runtime.SetFinalizer(ret, func(dec *checkedDeallocator) {
		if !dec.deallocated.Load() {
			panic(fmt.Sprintf("malloc: memory leak\nallocated at %s", dec.allocateStack))
		}
	})

	return ret
}

var _ Deallocator = new(checkedDeallocator)

func (c *checkedDeallocator) Deallocate(ptr unsafe.Pointer) {
	if c.deallocated.CompareAndSwap(false, true) {
		// ok
		c.upstream.Deallocate(ptr)
	} else {
		// double free
		panic(fmt.Sprintf("malloc: double free\nallocated at %s", c.allocateStack))
	}
}
