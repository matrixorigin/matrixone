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

func benchmarkAllocator(
	b *testing.B,
	newAllocator func() Allocator,
) {

	b.Run("allocate", func(b *testing.B) {
		allcator := newAllocator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ptr, dec := allcator.Allocate(4096)
			dec.Deallocate(ptr)
		}
	})

	b.Run("parallel", func(b *testing.B) {
		allcator := newAllocator()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ptr, dec := allcator.Allocate(4096)
				dec.Deallocate(ptr)
			}
		})
	})

}
