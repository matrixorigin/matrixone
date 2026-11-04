// Copyright 2021 Matrix Origin
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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// BenchmarkAppendFixedListReuse measures one public bulk append into a vector
// whose physical capacity is retained between iterations.
func BenchmarkAppendFixedListReuse(b *testing.B) {
	for _, size := range []int{1, 8, 15, 16, 128, 2048} {
		for _, mode := range []string{"flat", "flat_mask", "mixed"} {
			if mode == "mixed" && size < 128 {
				continue
			}
			b.Run(mode+"_"+strconv.Itoa(size), func(b *testing.B) {
				mp := mpool.MustNewZero()
				vec := NewVec(types.T_int64.ToType())
				b.Cleanup(func() {
					defer mpool.DeleteMPool(mp)
					vec.Free(mp)
					require.Zero(b, mp.CurrNB())
					require.Zero(b, mp.OnHeapCurrNB())
				})
				values := make([]int64, size)
				for i := range values {
					values[i] = int64(i)
				}
				var nulls []bool
				if mode != "flat" {
					nulls = make([]bool, size)
					if mode == "mixed" {
						for i := 0; i < size; i += 8 {
							nulls[i] = true
						}
					}
				}
				if err := vec.PreExtend(size, mp); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					vec.ResetWithSameType()
					if err := AppendFixedList(vec, values, nulls, mp); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				require.Equal(b, size, vec.Length())
				for i := range values {
					if mode == "mixed" && nulls[i] {
						require.True(b, vec.IsNull(uint64(i)))
					} else {
						require.False(b, vec.IsNull(uint64(i)))
						require.Equal(b, values[i], GetFixedAtNoTypeCheck[int64](vec, i))
					}
					require.Equal(b, PrepareParamNone, vec.GetPrepareParamKindAt(i))
					require.Equal(b, types.StringSourceExpression, vec.GetStringSourceAt(i))
				}
			})
		}
	}
}
