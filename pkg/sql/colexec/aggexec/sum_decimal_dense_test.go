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

package aggexec

import (
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func assertDecimal64StateEqual(t *testing.T, actual, expected *sumDecimal64FastExec) {
	t.Helper()
	require.Equal(t, expected.GetNumGroups(), actual.GetNumGroups())
	for c := range expected.state {
		for v := range expected.state[c].vecs {
			require.Equal(t, expected.state[c].vecs[v].Length(), actual.state[c].vecs[v].Length())
			if v == 0 {
				require.Equal(t, vector.MustFixedColNoTypeCheck[types.Decimal128](expected.state[c].vecs[v]), vector.MustFixedColNoTypeCheck[types.Decimal128](actual.state[c].vecs[v]))
			} else {
				require.Equal(t, vector.MustFixedColNoTypeCheck[int64](expected.state[c].vecs[v]), vector.MustFixedColNoTypeCheck[int64](actual.state[c].vecs[v]))
			}
		}
	}
}

func TestDecimal64DenseBatchMatchesRowFill(t *testing.T) {
	for _, groups := range []int{0, 1, 256, 257} {
		for _, sum := range []bool{true, false} {
			t.Run(fmt.Sprintf("empty/groups%d/sum%v", groups, sum), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
				typ := types.New(types.T_decimal64, 12, 2)
				input := vector.NewVec(typ)
				defer input.Free(mp)
				id := AggIdOfSum
				if !sum {
					id = AggIdOfAvg
				}
				actual := newSumDecimal64FastExec(mp, sum, id, false, typ).(*sumDecimal64FastExec)
				defer actual.Free()
				expected := newSumDecimal64FastExec(mp, sum, id, false, typ).(*sumDecimal64FastExec)
				defer expected.Free()
				require.NoError(t, actual.GroupGrow(groups))
				require.NoError(t, expected.GroupGrow(groups))
				require.NoError(t, actual.BatchFill(0, nil, []*vector.Vector{input}))
				assertDecimal64StateEqual(t, actual, expected)
				if groups == 0 {
					for _, exec := range []*sumDecimal64FastExec{actual, expected} {
						results, err := exec.Flush()
						require.NoError(t, err)
						require.Empty(t, results)
					}
				} else {
					requireSingleAggResultEqual(t, mp, actual, expected, "")
				}
			})
		}
	}
	for _, groups := range []int{1, 100, 255, 256, 257, AggBatchSize + 1} {
		for _, kind := range []string{"ordinary", "nullable", "constant", "all-null"} {
			for _, sum := range []bool{true, false} {
				t.Run(fmt.Sprintf("groups%d/%s/sum%v", groups, kind, sum), func(t *testing.T) {
					mp := mpool.MustNewZero()
					typ := types.New(types.T_decimal64, 18, 2)
					const rows = 515
					var input *vector.Vector
					switch kind {
					case "constant":
						var err error
						negative := int64(-123)
						input, err = vector.NewConstFixed(typ, types.Decimal64(negative), rows, mp)
						require.NoError(t, err)
					case "all-null":
						input = vector.NewConstNull(typ, rows, mp)
					default:
						input = vector.NewVec(typ)
						for i := 0; i < rows; i++ {
							value := types.Decimal64((int64(i%19) - 9) * 123)
							if i%17 == 0 {
								value = types.Decimal64(math.MaxInt64)
							}
							require.NoError(t, vector.AppendFixed(input, value, kind == "nullable" && i%7 == 0, mp))
						}
					}
					before := append([]types.Decimal64(nil), vector.MustFixedColNoTypeCheck[types.Decimal64](input)...)
					nulls := make([]bool, rows)
					for i := range nulls {
						nulls[i] = input.IsNull(uint64(i))
					}
					id := AggIdOfSum
					if !sum {
						id = AggIdOfAvg
					}
					actual := newSumDecimal64FastExec(mp, sum, id, false, typ).(*sumDecimal64FastExec)
					expected := newSumDecimal64FastExec(mp, sum, id, false, typ).(*sumDecimal64FastExec)
					require.NoError(t, actual.GroupGrow(groups))
					require.NoError(t, expected.GroupGrow(groups))
					for offset := 1; offset < rows; {
						n := min(256, rows-offset)
						ids := make([]uint64, n)
						for i := range ids {
							ids[i] = uint64((offset+i)*73%groups + 1)
							if (offset+i)%13 == 0 {
								ids[i] = GroupNotMatched
							}
						}
						require.NoError(t, actual.BatchFill(offset, ids, []*vector.Vector{input}))
						for i, g := range ids {
							require.NoError(t, expected.Fill(int(g)-1, offset+i, []*vector.Vector{input}))
						}
						assertDecimal64StateEqual(t, actual, expected)
						offset += n
					}
					a, ea := actual.Flush()
					b, eb := expected.Flush()
					require.Equal(t, eb, ea)
					if ea == nil {
						for i := range a {
							require.Equal(t, b[i].GetType(), a[i].GetType())
							require.Equal(t, b[i].Length(), a[i].Length())
							for row := 0; row < a[i].Length(); row++ {
								require.Equal(t, b[i].IsNull(uint64(row)), a[i].IsNull(uint64(row)))
							}
							switch a[i].GetType().Oid {
							case types.T_decimal128:
								require.Equal(t, vector.MustFixedColNoTypeCheck[types.Decimal128](b[i]), vector.MustFixedColNoTypeCheck[types.Decimal128](a[i]))
							case types.T_float64:
								require.Equal(t, vector.MustFixedColNoTypeCheck[float64](b[i]), vector.MustFixedColNoTypeCheck[float64](a[i]))
							}
						}
					}
					for _, v := range a {
						v.Free(mp)
					}
					for _, v := range b {
						v.Free(mp)
					}
					require.Equal(t, before, vector.MustFixedColNoTypeCheck[types.Decimal64](input))
					for i := range nulls {
						require.Equal(t, nulls[i], input.IsNull(uint64(i)))
					}
					actual.Free()
					expected.Free()
					input.Free(mp)
					require.Zero(t, mp.CurrNB())
				})
			}
		}
	}
}

func TestDecimal64DenseBatchGrowthBoundary(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal64, 18, 0)
	input := vector.NewVec(typ)
	for i := 0; i < 513; i++ {
		require.NoError(t, vector.AppendFixed(input, types.Decimal64(i-256), false, mp))
	}
	actual := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, typ).(*sumDecimal64FastExec)
	expected := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, typ).(*sumDecimal64FastExec)
	require.NoError(t, actual.GroupGrow(256))
	require.NoError(t, expected.GroupGrow(256))
	for phase := 0; phase < 2; phase++ {
		n, offset := 256, 0
		if phase == 1 {
			n = 257
			offset = 256
			require.NoError(t, actual.GroupGrow(1))
			require.NoError(t, expected.GroupGrow(1))
		}
		ids := make([]uint64, n)
		for i := range ids {
			ids[i] = uint64(i + 1)
		}
		require.NoError(t, actual.BatchFill(offset, ids, []*vector.Vector{input}))
		for i, g := range ids {
			require.NoError(t, expected.Fill(int(g)-1, offset+i, []*vector.Vector{input}))
		}
		assertDecimal64StateEqual(t, actual, expected)
	}
	actual.Free()
	expected.Free()
	input.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func BenchmarkDecimal64DenseBatch(b *testing.B) {
	for _, size := range []int{1, 256, 8192} {
		for _, groups := range []int{1, 100, 256, 257} {
			b.Run(fmt.Sprintf("rows%d/groups%d", size, groups), func(b *testing.B) {
				mp := mpool.MustNewZero()
				typ := types.New(types.T_decimal64, 18, 2)
				input := vector.NewVec(typ)
				for i := 0; i < size; i++ {
					if err := vector.AppendFixed(input, types.Decimal64(i%23-11), false, mp); err != nil {
						b.Fatal(err)
					}
				}
				ids := make([]uint64, min(size, 256))
				for i := range ids {
					ids[i] = uint64(i%groups + 1)
				}
				exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, typ)
				if err := exec.GroupGrow(groups); err != nil {
					b.Fatal(err)
				}
				vectors := []*vector.Vector{input}
				b.ReportAllocs()
				b.SetBytes(int64(size) * 8)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for offset := 0; offset < size; offset += 256 {
						if err := exec.BatchFill(offset, ids[:min(len(ids), size-offset)], vectors); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				exec.Free()
				input.Free(mp)
			})
		}
	}
}

func TestDecimal64DenseBatchPreservesUncheckedPrefixSemantics(t *testing.T) {
	for _, isSum := range []bool{true, false} {
		t.Run(fmt.Sprintf("sum%v", isSum), func(t *testing.T) {
			mp := mpool.MustNewZero()
			param := types.New(types.T_decimal64, 18, 0)
			id := AggIdOfSum
			if !isSum {
				id = AggIdOfAvg
			}
			actual := newSumDecimal64FastExec(mp, isSum, id, false, param).(*sumDecimal64FastExec)
			expected := newSumDecimal64FastExec(mp, isSum, id, false, param).(*sumDecimal64FastExec)
			require.NoError(t, actual.GroupGrow(1))
			require.NoError(t, expected.GroupGrow(1))
			one := types.Decimal128FromInt64(1)
			nearMax, err := types.Decimal128Max.Sub128(one)
			require.NoError(t, err)
			for _, exec := range []*sumDecimal64FastExec{actual, expected} {
				chunkArr[types.Decimal128](exec.state[0].vecs[0])[0] = nearMax
				vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])[0] = 1
			}
			input := vector.NewVec(param)
			for _, value := range []int64{1, 1, -2} {
				require.NoError(t, vector.AppendFixed(input, types.Decimal64(value), false, mp))
			}
			require.NoError(t, actual.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{input}))
			for row := 0; row < 3; row++ {
				require.NoError(t, expected.Fill(0, row, []*vector.Vector{input}))
			}
			assertDecimal64StateEqual(t, actual, expected)
			wantErr := ""
			if !isSum {
				wantErr = "Decimal128 Div overflow"
			}
			requireSingleAggResultEqual(t, mp, actual, expected, wantErr)
			actual.Free()
			expected.Free()
			input.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}
