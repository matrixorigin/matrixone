// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

package malloc

import "testing"

func BenchmarkSimpleCAllocatorReclaim(b *testing.B) {
	for _, size := range []uint64{64, 344064} {
		for _, cached := range []bool{false, true} {
			name := testNameForSize(size)
			if cached {
				name += "/cached"
			}
			b.Run(name, func(b *testing.B) {
				a := newTestSimpleCAllocator()
				if cached {
					a.EnableMmapCache(func() uint64 { return 16 << 20 }, nil)
					b.Cleanup(a.mmapCache.drain)
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						data, err := a.Allocate(size)
						if err != nil {
							b.Error(err)
							return
						}
						a.Deallocate(data, size)
					}
				})
			})
		}
	}
}
