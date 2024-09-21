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

func TestCheckedAllocator(t *testing.T) {
	testAllocator(t, func() Allocator {
		return NewCheckedAllocator(
			newUpstreamAllocatorForTest(),
		)
	})

	// missing free
	t.Run("missing free", func(t *testing.T) {
		allocator := NewCheckedAllocator(
			newUpstreamAllocatorForTest(),
		)
		ptr, dec, err := allocator.Allocate(42, NoHints)
		if err != nil {
			t.Fatal(err)
		}
		// comment the following line to trigger a missing-free panic
		// this panic will be raised in SetFinalizer func so it's not recoverable and not testable
		dec.Deallocate(NoHints)
		_ = ptr
		_ = dec
		runtime.GC()
	})

	// double free
	t.Run("double free", func(t *testing.T) {
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
		allocator := NewCheckedAllocator(
			newUpstreamAllocatorForTest(),
		)
		_, dec, err := allocator.Allocate(42, NoHints)
		if err != nil {
			t.Fatal(err)
		}
		dec.Deallocate(NoHints)
		dec.Deallocate(NoHints)
	})

	// use after free
	t.Run("use after free", func(t *testing.T) {
		allocator := NewCheckedAllocator(
			newUpstreamAllocatorForTest(),
		)
		slice, dec, err := allocator.Allocate(42, NoHints)
		if err != nil {
			t.Fatal(err)
		}
		for i := range slice {
			slice[i] = byte(i)
		}
		dec.Deallocate(NoHints)
		// zero or segfault
		//for i := range slice {
		//	if slice[i] != 0 {
		//		t.Fatal("should zero")
		//	}
		//}
	})

}

func BenchmarkCheckedAllocator(b *testing.B) {
	for _, n := range benchNs {
		benchmarkAllocator(b, func() Allocator {
			return NewCheckedAllocator(
				newUpstreamAllocatorForTest(),
			)
		}, n)
	}
}

func FuzzCheckedAllocator(f *testing.F) {
	fuzzAllocator(f, func() Allocator {
		return NewCheckedAllocator(
			newUpstreamAllocatorForTest(),
		)
	})
}
