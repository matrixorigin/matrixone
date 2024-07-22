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

		// allocate
		size := i % (8 * GB)
		slice, dec, err := allocator.Allocate(size, NoHints)
		if err != nil {
			t.Fatal(err)
		}
		// length
		if len(slice) != int(size) {
			t.Fatal()
		}
		// deallocate
		defer dec.Deallocate(NoHints)

		// read
		for _, i := range slice {
			if i != 0 {
				t.Fatal()
			}
		}
		// write
		for i := range slice {
			slice[i] = uint8(i)
		}
		// read
		for i, j := range slice {
			if j != uint8(i) {
				t.Fatal()
			}
		}

		// slice
		slice = slice[:len(slice)/2]
		for i, j := range slice {
			if j != uint8(i) {
				t.Fatal()
			}
		}

		time.Sleep(time.Duration(i) % (time.Microsecond * 50))

	})
}
