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

	"github.com/stretchr/testify/assert"
)

func TestSizeBoundedAllocator(t *testing.T) {
	testAllocator(t, func() Allocator {
		return NewSizeBoundedAllocator(
			newUpstreamAllocatorForTest(),
			1<<40,
			nil,
		)
	})

	t.Run("out of space", func(t *testing.T) {
		allocator := NewSizeBoundedAllocator(
			newUpstreamAllocatorForTest(),
			24,
			nil,
		)
		_, d1, err := allocator.Allocate(12, NoHints)
		assert.Nil(t, err)
		_, _, err = allocator.Allocate(12, NoHints)
		assert.Nil(t, err)
		_, _, err = allocator.Allocate(12, NoHints)
		assert.ErrorContains(t, err, "out of space")
		d1.Deallocate(NoHints)
		_, _, err = allocator.Allocate(12, NoHints)
		assert.Nil(t, err)
	})
}

func BenchmarkSizeBoundedAllocator(b *testing.B) {
	for _, n := range benchNs {
		benchmarkAllocator(b, func() Allocator {
			return NewSizeBoundedAllocator(
				newUpstreamAllocatorForTest(),
				1<<40,
				nil,
			)
		}, n)
	}
}

func FuzzSizeBoundedAllocator(f *testing.F) {
	fuzzAllocator(f, func() Allocator {
		return NewSizeBoundedAllocator(
			newUpstreamAllocatorForTest(),
			1<<40,
			nil,
		)
	})
}
