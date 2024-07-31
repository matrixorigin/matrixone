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
	"math"
	"testing"
)

func TestManagedAllocator(t *testing.T) {
	allocator := NewManagedAllocator(
		newUpstreamAllocatorForTest(),
	)
	for i := uint64(1); i < 128*MB; i = uint64(math.Ceil(float64(i) * 1.1)) {
		// allocate
		slice, err := allocator.Allocate(i, NoHints)
		if err != nil {
			t.Fatal(err)
		}
		// len
		if len(slice) != int(i) {
			t.Fatal()
		}
		// read
		for _, i := range slice {
			if i != 0 {
				t.Fatal("not zeroed")
			}
		}
		// write
		for i := range slice {
			slice[i] = byte(i)
		}
		// read
		for i := range slice {
			if slice[i] != byte(i) {
				t.Fatal()
			}
		}
		// slice
		slice = slice[:len(slice)/2]
		for i := range slice {
			if slice[i] != byte(i) {
				t.Fatal()
			}
		}
		// deallocate
		allocator.Deallocate(slice, NoHints)
	}
}

func BenchmarkManagedAllocator(b *testing.B) {

	newAllocator := func() *ManagedAllocator {
		return NewManagedAllocator(
			newUpstreamAllocatorForTest(),
		)
	}
	for _, n := range benchNs {

		b.Run(fmt.Sprintf("allocate: n %v", n), func(b *testing.B) {
			allcator := newAllocator()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				slice, err := allcator.Allocate(n, NoHints)
				if err != nil {
					b.Fatal(err)
				}
				allcator.Deallocate(slice, NoHints)
			}
		})

		b.Run(fmt.Sprintf("parallel: n %v", n), func(b *testing.B) {
			allcator := newAllocator()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					slice, err := allcator.Allocate(n, NoHints)
					if err != nil {
						b.Fatal(err)
					}
					allcator.Deallocate(slice, NoHints)
				}
			})
		})

	}

}
