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
	"bytes"
	"math"
	"testing"
)

func testAllocator(
	t *testing.T,
	newAllocator func() Allocator,
) {
	t.Helper()

	t.Run("allocate", func(t *testing.T) {
		allocator := newAllocator()
		for i := uint64(1); i < 8*MB; i = uint64(math.Ceil(float64(i) * 1.1)) {
			func() {
				slice, dec, err := allocator.Allocate(i, NoHints)
				if err != nil {
					t.Fatal(err)
				}
				defer dec.Deallocate()
				if len(slice) != int(i) {
					t.Fatalf("allocation length: got %d, want %d", len(slice), i)
				}
				testAllocationContents(t, slice)
				var freeze Freezer
				if dec.As(&freeze) {
					freeze.Freeze()
				}
			}()
		}
	})

}

// Check every byte at the original allocation sizes without adding one race
// detector call per byte. The pattern still matches byte(offset), including
// the shortened slice used by ManagedAllocator's deallocation contract.
func testAllocationContents(t *testing.T, data []byte) []byte {
	t.Helper()
	var pattern [4096]byte
	for offset := 0; offset < len(data); offset += len(pattern) {
		n := min(len(pattern), len(data)-offset)
		if !bytes.Equal(data[offset:offset+n], pattern[:n]) {
			t.Fatalf("allocation is not zeroed at offset %d", offset)
		}
	}
	for i := range pattern {
		pattern[i] = byte(i)
	}
	for offset := 0; offset < len(data); offset += len(pattern) {
		copy(data[offset:], pattern[:])
	}
	for _, view := range [][]byte{data, data[:len(data)/2]} {
		for offset := 0; offset < len(view); offset += len(pattern) {
			n := min(len(pattern), len(view)-offset)
			if !bytes.Equal(view[offset:offset+n], pattern[:n]) {
				t.Fatalf("allocation contents differ at offset %d (slice length %d)", offset, len(view))
			}
		}
	}
	return data[:len(data)/2]
}
