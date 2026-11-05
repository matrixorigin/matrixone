//go:build amd64 && go1.27 && goexperiment.simd

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

package aggexec

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"math"
	"simd/archsimd"
	"testing"
)

func TestDecimalReductionPrefix(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 unavailable")
	}
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, n := range []int{0, 255, 256, 257, 512} {
			t.Run(fmt.Sprintf("%s/rows%d", oid.String(), n), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
				v := vector.NewVec(types.New(oid, 38, 2))
				t.Cleanup(func() { v.Free(mp) })
				values := make([]int64, n+3)
				bounds := []int64{math.MinInt64, math.MaxInt64, -8, 8, -1, 0, 7, -7}
				for i := range values {
					values[i] = bounds[i%len(bounds)]
					switch oid {
					case types.T_decimal64:
						require.NoError(t, vector.AppendFixed(v, types.Decimal64(values[i]), false, mp))
					case types.T_decimal128:
						require.NoError(t, vector.AppendFixed(v, types.Decimal128FromInt64(values[i]), false, mp))
					case types.T_decimal256:
						require.NoError(t, vector.AppendFixed(v, types.Decimal256FromInt64(values[i]), false, mp))
					}
				}
				p := reduceDecimalPrefix(v, 3, n)
				wantRows := 0
				if n >= 256 {
					wantRows = n / 4 * 4
				}
				require.Equal(t, wantRows, p.rows)
				require.Equal(t, int64(wantRows), p.count)
				var sum types.Decimal128
				for i := 0; i < wantRows; i++ {
					sum = sum.Add128Unchecked(types.Decimal128FromInt64(values[i+3]))
				}
				require.Equal(t, sum, p.sum)
				if n != 512 || oid == types.T_decimal64 {
					return
				}
				var prefixSum types.Decimal128
				for _, value := range values[3:303] {
					prefixSum = prefixSum.Add128Unchecked(types.Decimal128FromInt64(value))
				}
				limbs := 1
				if oid == types.T_decimal256 {
					limbs = 3
				}
				for lane := 0; lane < 4; lane++ {
					for limb := 1; limb <= limbs; limb++ {
						t.Run(fmt.Sprintf("wide_lane%d_limb%d", lane, limb), func(t *testing.T) {
							at := 303 + lane
							if oid == types.T_decimal128 {
								args := vector.MustFixedColNoTypeCheck[types.Decimal128](v)
								old := args[at]
								t.Cleanup(func() { args[at] = old })
								args[at].B64_127 ^= 1
							} else {
								args := vector.MustFixedColNoTypeCheck[types.Decimal256](v)
								old := args[at]
								t.Cleanup(func() { args[at] = old })
								switch limb {
								case 1:
									args[at].B64_127 ^= 1
								case 2:
									args[at].B128_191 ^= 1
								case 3:
									args[at].B192_255 ^= 1
								}
							}
							got := reduceDecimalPrefix(v, 3, n)
							require.Equal(t, 300, got.rows)
							require.Equal(t, int64(300), got.count)
							require.Equal(t, prefixSum, got.sum)
						})
					}
				}
			})
		}
	}
}

func TestDecimalContiguousConsumers(t *testing.T) {
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, sum := range []bool{false, true} {
			for _, shape := range []string{"flat", "nullable", "late_wide"} {
				if oid == types.T_decimal64 && shape == "late_wide" {
					continue
				}
				t.Run(fmt.Sprintf("%s/sum%v/%s", oid.String(), sum, shape), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
					width := int32(15)
					if oid == types.T_decimal128 {
						width = 38
					}
					if oid == types.T_decimal256 {
						width = 65
					}
					typ := types.New(oid, width, 2)
					v := vector.NewVec(typ)
					t.Cleanup(func() { v.Free(mp) })
					for i := 0; i < 515; i++ {
						x := int64(i%17 - 8)
						null := shape == "nullable" && i%13 == 0
						switch oid {
						case types.T_decimal64:
							require.NoError(t, vector.AppendFixed(v, types.Decimal64(x), null, mp))
						case types.T_decimal128:
							value := types.Decimal128FromInt64(x)
							if shape == "late_wide" && i == 303 {
								value = types.Decimal128{B64_127: 1}
							}
							require.NoError(t, vector.AppendFixed(v, value, null, mp))
						case types.T_decimal256:
							value := types.Decimal256FromInt64(x)
							if shape == "late_wide" && i == 303 {
								value = types.Decimal256{B128_191: 1}
							}
							require.NoError(t, vector.AppendFixed(v, value, null, mp))
						}
					}
					id := AggIdOfAvg
					if sum {
						id = AggIdOfSum
					}
					actual := makeSumAvgExec(mp, sum, id, false, typ)
					t.Cleanup(actual.Free)
					expected := makeSumAvgExec(mp, sum, id, false, typ)
					t.Cleanup(expected.Free)
					require.NoError(t, actual.GroupGrow(1))
					require.NoError(t, expected.GroupGrow(1))
					input := []*vector.Vector{v}
					require.NoError(t, actual.BulkFill(0, input))
					for row := 0; row < v.Length(); row++ {
						require.NoError(t, expected.Fill(0, row, input))
					}
					got, err := actual.Flush()
					require.NoError(t, err)
					for _, v := range got {
						t.Cleanup(func() { v.Free(mp) })
					}
					want, err := expected.Flush()
					require.NoError(t, err)
					for _, v := range want {
						t.Cleanup(func() { v.Free(mp) })
					}
					require.Equal(t, len(want), len(got))
					for i := range want {
						require.Equal(t, *want[i].GetType(), *got[i].GetType())
						require.Equal(t, want[i].IsNull(0), got[i].IsNull(0))
						size := want[i].GetType().TypeSize()
						require.Equal(t, want[i].GetData()[:size], got[i].GetData()[:size])
					}
				})
			}
		}
	}
}
