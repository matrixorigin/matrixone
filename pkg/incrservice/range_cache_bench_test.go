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

package incrservice

import (
	"fmt"
	"testing"
)

func BenchmarkNextValueInRange(b *testing.B) {
	for _, step := range []uint64{1, 2} {
		for _, increment := range []uint64{3, 64, 65535} {
			b.Run(fmt.Sprintf("step_%d/increment_%d", step, increment), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					from := uint64(i%10000 + 1)
					value, _, ok := nextValueInRange(from, from+1000000, step, increment, 2)
					if !ok || value < from || (value-2)%increment != 0 || (value-from)%step != 0 {
						// Odd from with an even underlying step/increment is
						// an incompatible residue, not an allocator failure.
						if step == 2 && increment%2 == 0 && from%2 != 0 && !ok {
							continue
						}
						b.Fatalf("invalid next value %d (from=%d ok=%v)", value, from, ok)
					}
				}
			})
		}
	}
}
