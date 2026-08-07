// Copyright 2026 Matrix Origin
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

import "testing"

func TestBackingSizeMatchesClassAllocation(t *testing.T) {
	const request = 700 * 1024
	const want = 1 << 20

	allocator := NewClassAllocator(NewFixedSizeMakeAllocator)
	got, err := BackingSize(allocator, request)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("backing size = %d, want %d", got, want)
	}

	buf, dec, err := allocator.Allocate(request, NoHints)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Deallocate()
	if len(buf) != request || cap(buf) != want {
		t.Fatalf("allocated buffer len/cap = %d/%d, want %d/%d", len(buf), cap(buf), request, want)
	}
}

type unsizedAllocator struct{}

func (unsizedAllocator) Allocate(uint64, Hints) ([]byte, Deallocator, error) {
	return nil, nil, nil
}

func TestBackingSizeRejectsAllocatorWithoutCapacityContract(t *testing.T) {
	_, err := BackingSize(unsizedAllocator{}, 1)
	if err == nil {
		t.Fatal("expected allocator without BackingSizer to be rejected")
	}
}
