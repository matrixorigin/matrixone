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
// order and top do. Shuffle allocates new values and remaps the NULLs with
// nulls.Filter; GatherSwap gathers with Union into a scratch vector the owner
// reuses and swaps it in.
func BenchmarkPermute(b *testing.B) {
	perm := gatherBenchRowLists()["perm"]
	for _, tc := range gatherBenchTypes {
		b.Run(tc.name+"/Shuffle", func(b *testing.B) {
			mp := mpool.MustNewZero()
			src := gatherBenchSource(b, tc.typ, gatherBenchRows, tc.nullEvery, mp)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				v, err := src.Dup(mp)
				require.NoError(b, err)
				b.StartTimer()
				if err := v.Shuffle(perm, mp); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				v.Free(mp)
				b.StartTimer()
			}
		})
		b.Run(tc.name+"/GatherSwap", func(b *testing.B) {
			mp := mpool.MustNewZero()
			src := gatherBenchSource(b, tc.typ, gatherBenchRows, tc.nullEvery, mp)
			scratch := NewVec(tc.typ, mp)
			require.NoError(b, scratch.Union(src, perm, mp))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				v, err := src.Dup(mp)
				require.NoError(b, err)
				b.StartTimer()
				scratch.CleanOnlyData()
				if err := scratch.Union(v, perm, mp); err != nil {
					b.Fatal(err)
				}
				// The owner swaps scratch and v; the swap is two pointer
				// moves, not part of the cost measured here.
				b.StopTimer()
				v.Free(mp)
				b.StartTimer()
			}
		})
	}
}
