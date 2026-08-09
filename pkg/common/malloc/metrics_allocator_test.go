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

import "testing"

type cappedTestAllocator struct {
	capacity int
}

func (a cappedTestAllocator) Allocate(size uint64, _ Hints) ([]byte, Deallocator, error) {
	return make([]byte, int(size), a.capacity), FuncDeallocator(func() {}), nil
}

func TestMetricsAllocator(t *testing.T) {
	testAllocator(t, func() Allocator {
		return NewMetricsAllocator(
			newUpstreamAllocatorForTest(),
			nil, nil, nil, nil, nil,
		)
	})
}

func TestMetricsAllocatorAccountsBackingCapacity(t *testing.T) {
	allocator := NewMetricsAllocator(
		cappedTestAllocator{capacity: 1024},
		nil, nil, nil, nil, nil,
	)
	_, dec, err := allocator.Allocate(700, NoHints)
	if err != nil {
		t.Fatal(err)
	}
	if got := allocator.currentInuse.Load(); got != 1024 {
		t.Fatalf("in-use bytes = %d, want physical capacity 1024", got)
	}

	dec.Deallocate()
	if got := allocator.currentInuse.Load(); got != 0 {
		t.Fatalf("in-use bytes after release = %d, want 0", got)
	}
}

func BenchmarkMetricsAllocator(b *testing.B) {
	for _, n := range benchNs {
		benchmarkAllocator(b, func() Allocator {
			return NewMetricsAllocator(
				newUpstreamAllocatorForTest(),
				nil, nil, nil, nil, nil,
			)
		}, n)
	}
}

func FuzzMetricsAllocator(f *testing.F) {
	fuzzAllocator(f, func() Allocator {
		return NewMetricsAllocator(
			newUpstreamAllocatorForTest(),
			nil, nil, nil, nil, nil,
		)
	})
}
