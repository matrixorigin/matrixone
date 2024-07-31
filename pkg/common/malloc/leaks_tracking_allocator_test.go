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
	"testing"
)

func TestLeaksTrackingAllocator(t *testing.T) {
	testAllocator(t, func() Allocator {
		tracker := new(LeaksTracker)
		return NewLeaksTrackingAllocator(
			newUpstreamAllocatorForTest(),
			tracker,
		)
	})

	t.Run("report", func(t *testing.T) {
		tracker := new(LeaksTracker)
		allocator := NewLeaksTrackingAllocator(
			newUpstreamAllocatorForTest(),
			tracker,
		)

		_, dec, err := allocator.Allocate(42, NoHints)
		if err != nil {
			t.Fatal(err)
		}

		buf := new(bytes.Buffer)
		leaks := tracker.ReportLeaks(buf)
		if !leaks {
			t.Fatal()
		}
		if len(buf.Bytes()) == 0 {
			t.Fatal()
		}

		dec.Deallocate(NoHints)
		buf = new(bytes.Buffer)
		leaks = tracker.ReportLeaks(buf)
		if leaks {
			t.Fatal()
		}
		if len(buf.Bytes()) != 0 {
			t.Fatal()
		}

	})
}

func BenchmarkLeaksTrackingAllocator(b *testing.B) {
	for _, n := range benchNs {
		benchmarkAllocator(b, func() Allocator {
			tracker := new(LeaksTracker)
			return NewLeaksTrackingAllocator(
				newUpstreamAllocatorForTest(),
				tracker,
			)
		}, n)
	}
}

func FuzzLeaksTrackingAllocator(f *testing.F) {
	fuzzAllocator(f, func() Allocator {
		tracker := new(LeaksTracker)
		return NewLeaksTrackingAllocator(
			newUpstreamAllocatorForTest(),
			tracker,
		)
	})
}
