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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// makeBitAggExec creates a bit aggregate exec directly without going through
// the function registry (which is not set up in unit tests).
func makeBitAggExec(mp *mpool.MPool, aggID int64, param types.Type) AggFuncExec {
	switch aggID {
	case AggIdOfBitAnd:
		return makeBitAndExec(mp, aggID, false, param)
	case AggIdOfBitOr:
		return makeBitOrExec(mp, aggID, false, param)
	case AggIdOfBitXor:
		return makeBitXorExec(mp, aggID, false, param)
	default:
		panic("unknown bit agg id")
	}
}

func TestBitOpsInt64(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int64.ToType()

	makeInputVec := func(values []int64, nulls []bool) *vector.Vector {
		vec := vector.NewVec(typ)
		for i, v := range values {
			isNull := false
			if nulls != nil && nulls[i] {
				isNull = true
			}
			require.NoError(t, vector.AppendFixed(vec, v, isNull, mp))
		}
		return vec
	}

	t.Run("normal values", func(t *testing.T) {
		// BIT_AND: 3 & 5 & 7 = 1
		// BIT_OR:  3 | 5 | 7 = 7
		// BIT_XOR: 3 ^ 5 ^ 7 = 1 (3^5=6, 6^7=1)
		vec := makeInputVec([]int64{3, 5, 7}, nil)
		defer vec.Free(mp)

		for _, tc := range []struct {
			name   string
			aggID  int64
			expect uint64
		}{
			{"bit_and", AggIdOfBitAnd, 1},
			{"bit_or", AggIdOfBitOr, 7},
			{"bit_xor", AggIdOfBitXor, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := makeBitAggExec(mp, tc.aggID, typ)
				require.NoError(t, exec.GroupGrow(1))
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

				results, err := exec.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Equal(t, tc.expect, vector.MustFixedColNoTypeCheck[uint64](results[0])[0])

				exec.Free()
				for _, r := range results {
					r.Free(mp)
				}
			})
		}
	})

	t.Run("empty group returns neutral values", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			aggID  int64
			expect uint64
		}{
			{"bit_and empty", AggIdOfBitAnd, math.MaxUint64},
			{"bit_or empty", AggIdOfBitOr, 0},
			{"bit_xor empty", AggIdOfBitXor, 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := makeBitAggExec(mp, tc.aggID, typ)
				require.NoError(t, exec.GroupGrow(1))
				// no Fill/BulkFill/BatchFill called — simulating empty group

				results, err := exec.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Equal(t, tc.expect, vector.MustFixedColNoTypeCheck[uint64](results[0])[0])

				exec.Free()
				for _, r := range results {
					r.Free(mp)
				}
			})
		}
	})

	t.Run("all-NULL group returns neutral values", func(t *testing.T) {
		vec := makeInputVec([]int64{0, 0, 0}, []bool{true, true, true})
		defer vec.Free(mp)

		for _, tc := range []struct {
			name   string
			aggID  int64
			expect uint64
		}{
			{"bit_and all null", AggIdOfBitAnd, math.MaxUint64},
			{"bit_or all null", AggIdOfBitOr, 0},
			{"bit_xor all null", AggIdOfBitXor, 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := makeBitAggExec(mp, tc.aggID, typ)
				require.NoError(t, exec.GroupGrow(1))
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

				results, err := exec.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Equal(t, tc.expect, vector.MustFixedColNoTypeCheck[uint64](results[0])[0])

				exec.Free()
				for _, r := range results {
					r.Free(mp)
				}
			})
		}
	})

	t.Run("mixed null and non-null", func(t *testing.T) {
		// BIT_AND: 6 & NULL & 7 = 6 & 7 = 6
		// BIT_OR:  6 | NULL | 7 = 6 | 7 = 7
		// BIT_XOR: 6 ^ NULL ^ 7 = 6 ^ 7 = 1
		vec := makeInputVec([]int64{6, 0, 7}, []bool{false, true, false})
		defer vec.Free(mp)

		for _, tc := range []struct {
			name   string
			aggID  int64
			expect uint64
		}{
			{"bit_and mixed", AggIdOfBitAnd, 6},
			{"bit_or mixed", AggIdOfBitOr, 7},
			{"bit_xor mixed", AggIdOfBitXor, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := makeBitAggExec(mp, tc.aggID, typ)
				require.NoError(t, exec.GroupGrow(1))
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

				results, err := exec.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Equal(t, tc.expect, vector.MustFixedColNoTypeCheck[uint64](results[0])[0])

				exec.Free()
				for _, r := range results {
					r.Free(mp)
				}
			})
		}
	})
}

func TestBitOpsMultipleGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int64.ToType()

	makeInputVec := func(values []int64) *vector.Vector {
		vec := vector.NewVec(typ)
		for _, v := range values {
			require.NoError(t, vector.AppendFixed(vec, v, false, mp))
		}
		return vec
	}

	t.Run("multiple groups with mixed data", func(t *testing.T) {
		// Group 1: 3 & 5 & 7 = 1
		// Group 2: empty (no values) → neutral
		// Group 3: 6 & 2 = 2 for AND, 6|2=6 for OR, 6^2=4 for XOR
		vec := makeInputVec([]int64{3, 5, 7, 6, 2})
		defer vec.Free(mp)
		// Row 0 → group 1, Row 1 → group 1, Row 2 → group 1
		// Row 3 → group 3, Row 4 → group 3
		// Group 2 intentionally left empty
		groups := []uint64{1, 1, 1, 3, 3}

		for _, tc := range []struct {
			name         string
			aggID        int64
			expectGroup1 uint64
			expectGroup2 uint64
			expectGroup3 uint64
		}{
			{"bit_and multi-group", AggIdOfBitAnd, 1, math.MaxUint64, 2},
			{"bit_or multi-group", AggIdOfBitOr, 7, 0, 6},
			{"bit_xor multi-group", AggIdOfBitXor, 1, 0, 4},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := makeBitAggExec(mp, tc.aggID, typ)
				require.NoError(t, exec.GroupGrow(3))
				require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))

				results, err := exec.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)

				aggs := vector.MustFixedColNoTypeCheck[uint64](results[0])
				require.Equal(t, tc.expectGroup1, aggs[0])
				require.Equal(t, tc.expectGroup2, aggs[1])
				require.Equal(t, tc.expectGroup3, aggs[2])

				exec.Free()
				for _, r := range results {
					r.Free(mp)
				}
			})
		}
	})
}

func TestBitOpsBytes(t *testing.T) {
	mp := mpool.MustNewZero()

	makeInputVec := func(typ types.Type, values [][]byte, nulls []bool) *vector.Vector {
		vec := vector.NewVec(typ)
		for i, v := range values {
			isNull := false
			if nulls != nil && nulls[i] {
				isNull = true
			}
			require.NoError(t, vector.AppendBytes(vec, v, isNull, mp))
		}
		return vec
	}

	// bytesN builds a slice of n bytes filled with b.
	bytesN := func(b byte, n int) []byte {
		out := make([]byte, n)
		for i := range out {
			out[i] = b
		}
		return out
	}

	t.Run("normal values byte bit_or", func(t *testing.T) {
		typ := types.New(types.T_binary, 1, 0)
		// BIT_OR: 0x01 | 0x02 | 0x04 = 0x07
		vec := makeInputVec(typ, [][]byte{{0x01}, {0x02}, {0x04}}, nil)
		defer vec.Free(mp)

		exec := makeBitAggExec(mp, AggIdOfBitOr, typ)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, []byte{0x07}, results[0].GetBytesAt(0))

		exec.Free()
		for _, r := range results {
			r.Free(mp)
		}
	})

	// Neutral values for empty or all-NULL groups must preserve the argument
	// byte length: BIT_AND -> width bytes of 0xFF, BIT_OR/BIT_XOR -> width
	// bytes of 0x00. Cover both BINARY(N) and VARBINARY(N) for several widths.
	for _, dim := range []struct {
		name  string
		oid   types.T
		width int32
	}{
		{"binary(3)", types.T_binary, 3},
		{"binary(10)", types.T_binary, 10},
		{"varbinary(4)", types.T_varbinary, 4},
	} {
		typ := types.New(dim.oid, dim.width, 0)
		w := int(dim.width)

		for _, tc := range []struct {
			name   string
			aggID  int64
			expect []byte
		}{
			{"bit_and", AggIdOfBitAnd, bytesN(0xFF, w)},
			{"bit_or", AggIdOfBitOr, bytesN(0x00, w)},
			{"bit_xor", AggIdOfBitXor, bytesN(0x00, w)},
		} {
			t.Run(dim.name+" empty group "+tc.name, func(t *testing.T) {
				exec := makeBitAggExec(mp, tc.aggID, typ)
				require.NoError(t, exec.GroupGrow(1))

				results, err := exec.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.False(t, results[0].IsNull(0))
				require.Equal(t, tc.expect, results[0].GetBytesAt(0))

				exec.Free()
				for _, r := range results {
					r.Free(mp)
				}
			})

			t.Run(dim.name+" all-NULL group "+tc.name, func(t *testing.T) {
				vec := makeInputVec(typ, [][]byte{{0x01}, {0x02}}, []bool{true, true})
				defer vec.Free(mp)

				exec := makeBitAggExec(mp, tc.aggID, typ)
				require.NoError(t, exec.GroupGrow(1))
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

				results, err := exec.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.False(t, results[0].IsNull(0))
				require.Equal(t, tc.expect, results[0].GetBytesAt(0))

				exec.Free()
				for _, r := range results {
					r.Free(mp)
				}
			})
		}
	}

	t.Run("multiple groups one empty preserves width", func(t *testing.T) {
		typ := types.New(types.T_binary, 3, 0)
		// group 1 has values, group 2 is empty -> neutral, group 3 has values
		vec := makeInputVec(typ, [][]byte{{0x0F, 0x0F, 0x0F}, {0xF0, 0xF0, 0xF0}}, nil)
		defer vec.Free(mp)
		groups := []uint64{1, 3}

		exec := makeBitAggExec(mp, AggIdOfBitOr, typ)
		require.NoError(t, exec.GroupGrow(3))
		require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, []byte{0x0F, 0x0F, 0x0F}, results[0].GetBytesAt(0))
		require.False(t, results[0].IsNull(1))
		require.Equal(t, bytesN(0x00, 3), results[0].GetBytesAt(1))
		require.Equal(t, []byte{0xF0, 0xF0, 0xF0}, results[0].GetBytesAt(2))

		exec.Free()
		for _, r := range results {
			r.Free(mp)
		}
	})

	// Merging an all-NULL group into an empty group and then flushing must
	// still produce a width-length neutral value (covers the bytes Merge path).
	t.Run("merge all-NULL groups preserves neutral width", func(t *testing.T) {
		typ := types.New(types.T_binary, 4, 0)

		exec1 := makeBitAggExec(mp, AggIdOfBitAnd, typ)
		require.NoError(t, exec1.GroupGrow(1))
		exec2 := makeBitAggExec(mp, AggIdOfBitAnd, typ)
		require.NoError(t, exec2.GroupGrow(1))
		// feed exec2 an all-NULL row so its single group stays NULL
		nullVec := makeInputVec(typ, [][]byte{{0x01}}, []bool{true})
		defer nullVec.Free(mp)
		require.NoError(t, exec2.BulkFill(0, []*vector.Vector{nullVec}))

		// merge exec2's group 0 into exec1's group 0
		require.NoError(t, exec1.Merge(exec2, 0, 0))

		results, err := exec1.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.False(t, results[0].IsNull(0))
		require.Equal(t, bytesN(0xFF, 4), results[0].GetBytesAt(0))

		exec1.Free()
		exec2.Free()
		for _, r := range results {
			r.Free(mp)
		}
	})
}
