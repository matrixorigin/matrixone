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
	"runtime"
	"testing"
)

func TestReadOnlyAllocator(t *testing.T) {
	testAllocator(t, func() Allocator {
		return NewReadOnlyAllocator(
			NewShardedAllocator(
				runtime.GOMAXPROCS(0),
				func() Allocator {
					return NewClassAllocator(
						NewFixedSizeMmapAllocator,
					)
				},
			),
		)
	})

	allocator := NewReadOnlyAllocator(
		NewShardedAllocator(
			runtime.GOMAXPROCS(0),
			func() Allocator {
				return NewClassAllocator(
					NewFixedSizeMmapAllocator,
				)
			},
		),
	)
	slice, dec, err := allocator.Allocate(42, NoHints)
	if err != nil {
		t.Fatal(err)
	}
	_ = slice
	defer dec.Deallocate(NoHints)

	var freeze Freezer
	if !dec.As(&freeze) {
		t.Fatal("should be freezable")
	}
	freeze.Freeze()

	// this will trigger a memory fault
	//slice[0] = 1
}

func BenchmarkReadOnlyAllocator(b *testing.B) {
	for _, n := range benchNs {
		benchmarkAllocator(b, func() Allocator {
			return NewReadOnlyAllocator(
				NewShardedAllocator(
					runtime.GOMAXPROCS(0),
					func() Allocator {
						return NewClassAllocator(
							NewFixedSizeMmapAllocator,
						)
					},
				),
			)
		}, n)
	}
}

func FuzzReadOnlyAllocator(f *testing.F) {
	fuzzAllocator(f, func() Allocator {
		return NewReadOnlyAllocator(
			NewShardedAllocator(
				runtime.GOMAXPROCS(0),
				func() Allocator {
					return NewClassAllocator(
						NewFixedSizeMmapAllocator,
					)
				},
			),
		)
	})
}
