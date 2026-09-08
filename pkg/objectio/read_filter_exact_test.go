// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestReadFilterExactUnsortedMembership(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	defer func() { require.Zero(t, mp.CurrNB()) }()
	values := [][]byte{
		[]byte("z"), nil, []byte("key-00007"), {0, 255, 0},
		bytes.Repeat([]byte("long"), 12), []byte("key-00000"),
		[]byte("missing"), []byte("key-00007"), {},
	}
	for _, oid := range []types.T{types.T_varchar, types.T_varbinary} {
		for _, count := range []int{0, 1, 2, 8, 9, 64} {
			t.Run(fmt.Sprintf("%s/keys=%d", oid, count), func(t *testing.T) {
				vec := vector.NewVec(oid.ToType())
				defer vec.Free(mp)
				needles := make([][]byte, count)
				for i := range needles {
					needles[i] = []byte(fmt.Sprintf("key-%05d", count-1-i))
				}
				if count > 8 {
					// Unordered, duplicate, empty, binary and out-of-line keys.
					needles[0], needles[1], needles[2] = nil, []byte{}, bytes.Clone(values[3])
					needles[3], needles[4] = bytes.Clone(values[4]), []byte("key-00007")
				}
				sourceValues := append([][]byte(nil), values...)
				// Exercise every needle, on both sides of the short linear prefix,
				// while retaining misses and duplicate source rows.
				sourceValues = append(sourceValues, needles...)
				for _, value := range sourceValues {
					require.NoError(t, vector.AppendBytes(vec, value, false, mp))
				}
				var want []int64
				for row, value := range sourceValues {
					for _, needle := range needles {
						if bytes.Equal(value, needle) {
							want = append(want, int64(row))
							break
						}
					}
				}
				search := NewReadFilterSearch(oid, needles)
				for _, needle := range needles {
					clear(needle) // The descriptor must own its search values.
				}
				require.Equal(t, want, search.search(vec, false))
				require.Equal(t, want, search.search(vec, false), "reuse must not consume or reorder search state")
			})
		}
	}
}

// Include in-range misses and matches so a speedup cannot rely on all source
// values being outside the needle range. The sorted path is an unchanged control.
func BenchmarkReadFilterExactSearch(b *testing.B) {
	for _, count := range []int{1, 8, 9, 64, 1024, 4096} {
		for _, shape := range []string{"miss", "mixed", "first-hit", "prefix-hit", "sorted", "const"} {
			b.Run(fmt.Sprintf("keys=%d/%s", count, shape), func(b *testing.B) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				needles := make([][]byte, count)
				for i := range needles {
					needles[i] = []byte(fmt.Sprintf("key-%05d", 2*i))
				}
				search := NewReadFilterSearch(types.T_varchar, needles)
				var vec *vector.Vector
				const rows = 8192
				want := 0
				if shape == "const" {
					var err error
					vec, err = vector.NewConstBytes(types.T_varchar.ToType(), needles[count-1], rows, mp)
					require.NoError(b, err)
					want = rows
				} else {
					vec = vector.NewVec(types.T_varchar.ToType())
					for row := 0; row < rows; row++ {
						value := 2*((row*31)%count) + 1
						if shape == "first-hit" {
							value = 0
							want++
						} else if shape == "prefix-hit" {
							value = 2 * (row % min(count, 8))
							want++
						} else if shape != "miss" && row%4 == 0 {
							value--
							want++
						}
						require.NoError(b, vector.AppendBytes(vec, []byte(fmt.Sprintf("key-%05d", value)), false, mp))
					}
				}
				defer vec.Free(mp)
				if shape == "sorted" {
					vec.InplaceSort()
				}
				run := func(b *testing.B, find func() []int64) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if got := find(); len(got) != want {
							b.Fatalf("matched %d rows, want %d", len(got), want)
						}
					}
				}
				b.Run("current", func(b *testing.B) {
					run(b, func() []int64 { return search.search(vec, shape == "sorted") })
				})
				if shape == "miss" || shape == "mixed" || shape == "first-hit" || shape == "prefix-hit" {
					// Keep the previous algorithm in the same binary/run as a
					// control for machine load and compiler changes.
					linear := vector.VarlenLinearSearchOffsetByValFactory(needles)
					b.Run("linear", func(b *testing.B) {
						run(b, func() []int64 { return linear(vec) })
					})
				}
			})
		}
	}
}
