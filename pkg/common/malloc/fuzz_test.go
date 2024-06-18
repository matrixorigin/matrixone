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
	"testing"
	"time"
	"unsafe"
)

func fuzzAllocator(
	f *testing.F,
	newAllocator func() Allocator,
) {
	allocator := newAllocator()

	f.Fuzz(func(t *testing.T, i uint64) {
		if i == 0 {
			return
		}

		size := i % (8 * GB)
		ptr, dec, err := allocator.Allocate(size)
		if err != nil {
			t.Fatal(err)
		}
		defer dec.Deallocate(ptr)

		slice := unsafe.Slice((*byte)(ptr), size)
		for _, i := range slice {
			if i != 0 {
				t.Fatal()
			}
		}
		for i := range slice {
			slice[i] = uint8(i)
		}
		for i, j := range slice {
			if j != uint8(i) {
				t.Fatal()
			}
		}

		time.Sleep(time.Duration(i) % (time.Microsecond * 50))

	})
}
