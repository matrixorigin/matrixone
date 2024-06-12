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
	"strings"
	"testing"
)

func TestCheckedDeallocator(t *testing.T) {
	allocator := NewClassAllocator(ptrTo(uint32(1)))

	func() {
		defer func() {
			p := recover()
			if p == nil {
				t.Fatal("should panic")
			}
			msg := fmt.Sprintf("%v", p)
			if !strings.Contains(msg, "double free") {
				t.Fatalf("got %v", msg)
			}
		}()
		ptr, dec := allocator.Allocate(42)
		dec.Deallocate(ptr)
		dec.Deallocate(ptr)
	}()

	func() {
		ptr, dec := allocator.Allocate(42)
		_ = ptr
		_ = dec
		dec.Deallocate(ptr) // comment out this line to trigger memory leak panic
	}()
	runtime.GC()

}
