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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

// BenchmarkPermute applies a sort permutation to one 8192-row vector, as
// order and top do. Permute allocates new values from the pool;
// PermuteWithScratch gathers into a reused Go-heap buffer and copies back.
func BenchmarkPermute(b *testing.B) {
	perm := gatherBenchRowLists()["perm"]
	for _, tc := range gatherBenchTypes {
		for _, withScratch := range []bool{false, true} {
			name := tc.name + "/Permute"
			if withScratch {
				name = tc.name + "/PermuteWithScratch"
			}
			b.Run(name, func(b *testing.B) {
				mp := mpool.MustNewZero()
				src := gatherBenchSource(b, tc.typ, gatherBenchRows, tc.nullEvery, mp)
				var scratch []byte
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					v, err := src.Dup(mp)
					require.NoError(b, err)
					b.StartTimer()
					if withScratch {
						err = v.PermuteWithScratch(perm, mp, &scratch)
					} else {
						err = v.Permute(perm, mp)
					}
					if err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					v.Free(mp)
					b.StartTimer()
				}
			})
		}
	}
}
