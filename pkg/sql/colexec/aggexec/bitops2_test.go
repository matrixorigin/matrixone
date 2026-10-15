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
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBitwiseAggregateRejectsOversizedBinaryOperand(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	for _, aggID := range []int64{AggIdOfBitAnd, AggIdOfBitOr, AggIdOfBitXor} {
		for _, typ := range []types.Type{
			types.New(types.T_varbinary, 512, 0),
			types.New(types.T_blob, 512, 0),
			types.New(types.T_blob, 65535, 0),
		} {
			exec, ok, err := makeSpecialAggExec(mp, aggID, false, false, false, false, false, typ)
			require.True(t, ok)
			require.Nil(t, exec)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidBitwiseAggregateOperandsSize), "%v", err)
		}
	}
}

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
				require.False(t, results[0].HasNull(), "unused aggregate capacity must not escape as NULL")

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
				require.False(t, results[0].HasNull(), "empty group must materialize the non-NULL identity")

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
				require.False(t, results[0].HasNull(), "all-NULL group must materialize the non-NULL identity")

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
				require.False(t, results[0].HasNull(), "unused aggregate capacity must not escape as NULL")

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
		{"blob(511)", types.T_blob, 511},
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
				require.False(t, results[0].HasNull(), "unused aggregate capacity must not escape as NULL")
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
				require.False(t, results[0].HasNull(), "unused aggregate capacity must not escape as NULL")
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
		require.False(t, results[0].HasNull(), "unused aggregate capacity must not escape as NULL")
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

	// When SetBytesAt fails during BulkFill (e.g. OOM on varlena allocation),
	// the state must stay NULL and the error must propagate — never leave a
	// non-NULL slot with a default/empty payload.
	t.Run("BulkFill OOM leaves state NULL", func(t *testing.T) {
		for _, dim := range []struct {
			name  string
			oid   types.T
			width int32
		}{
			{"binary(64)", types.T_binary, 64},
			{"varbinary(64)", types.T_varbinary, 64},
			{"blob", types.T_blob, 0},
		} {
			t.Run(dim.name, func(t *testing.T) {
				origCap := mpool.GlobalCap()
				mpool.InitCap(100 * 1024 * 1024) // 100 MiB
				t.Cleanup(func() { mpool.InitCap(origCap) })
				capped, err := mpool.NewMPool("test", 2*1024*1024, 0) // 2 MiB
				require.NoError(t, err)
				t.Cleanup(func() { mpool.DeleteMPool(capped) })

				typ := types.New(dim.oid, dim.width, 0)
				exec := makeBitAggExec(capped, AggIdOfBitAnd, typ)
				t.Cleanup(exec.Free)
				require.NoError(t, exec.GroupGrow(1))

				// Exhaust the off-heap capacity. Track allocations so
				// they can be freed after the test (t.Cleanup runs LIFO,
				// so they are released before exec.Free and DeleteMPool).
				var exhaust [][]byte
				t.Cleanup(func() {
					for _, b := range exhaust {
						capped.Free(b)
					}
				})
				for {
					b, e := capped.Alloc(4096, true)
					if e != nil {
						break
					}
					exhaust = append(exhaust, b)
				}

				// Input vector from a normal (unlimited) mpool.
				inputVec := vector.NewVec(typ)
				t.Cleanup(func() { inputVec.Free(mp) })
				val := make([]byte, 64)
				for i := range val {
					val[i] = byte(i)
				}
				require.NoError(t, vector.AppendBytes(inputVec, val, false, mp))

				err = exec.BulkFill(0, []*vector.Vector{inputVec})
				require.Error(t, err, "BulkFill should fail when SetBytesAt cannot allocate")

				// Verify the state slot is still NULL directly.
				require.True(t, exec.(*bitOpExecBytes).state[0].vecs[0].IsNull(0),
					"state slot must remain NULL after failed SetBytesAt")
			})
		}
	})

	// Same contract for Merge: a failed SetBytesAt while copying the first
	// non-NULL value into an empty target group must leave the target NULL.
	t.Run("Merge OOM leaves state NULL", func(t *testing.T) {
		for _, dim := range []struct {
			name  string
			oid   types.T
			width int32
		}{
			{"binary(64)", types.T_binary, 64},
			{"varbinary(64)", types.T_varbinary, 64},
			{"blob", types.T_blob, 0},
		} {
			t.Run(dim.name, func(t *testing.T) {
				origCap := mpool.GlobalCap()
				mpool.InitCap(100 * 1024 * 1024) // 100 MiB
				t.Cleanup(func() { mpool.InitCap(origCap) })
				capped, err := mpool.NewMPool("test", 2*1024*1024, 0) // 2 MiB
				require.NoError(t, err)
				t.Cleanup(func() { mpool.DeleteMPool(capped) })

				typ := types.New(dim.oid, dim.width, 0)

				// exec1: empty target (all-NULL), uses capped mpool.
				exec1 := makeBitAggExec(capped, AggIdOfBitAnd, typ)
				t.Cleanup(exec1.Free)
				require.NoError(t, exec1.GroupGrow(1))

				// exec2: has a real value, uses normal (unlimited) mpool.
				exec2 := makeBitAggExec(mp, AggIdOfBitAnd, typ)
				t.Cleanup(exec2.Free)
				require.NoError(t, exec2.GroupGrow(1))
				inputVec := vector.NewVec(typ)
				t.Cleanup(func() { inputVec.Free(mp) })
				val := make([]byte, 64)
				for i := range val {
					val[i] = byte(i)
				}
				require.NoError(t, vector.AppendBytes(inputVec, val, false, mp))
				require.NoError(t, exec2.BulkFill(0, []*vector.Vector{inputVec}))

				// Exhaust the off-heap capacity. Freed via t.Cleanup (LIFO)
				// before the execs and the capped pool are torn down.
				var exhaust [][]byte
				t.Cleanup(func() {
					for _, b := range exhaust {
						capped.Free(b)
					}
				})
				for {
					b, e := capped.Alloc(4096, true)
					if e != nil {
						break
					}
					exhaust = append(exhaust, b)
				}

				err = exec1.Merge(exec2, 0, 0)
				require.Error(t, err, "Merge should fail when SetBytesAt cannot allocate")

				// Verify the state slot is still NULL directly.
				require.True(t, exec1.(*bitOpExecBytes).state[0].vecs[0].IsNull(0),
					"target slot must remain NULL after failed Merge SetBytesAt")
			})
		}
	})
}

func TestBitOpsBlob(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int64
		want []byte
	}{
		{"and", AggIdOfBitAnd, []byte{0, 0}},
		{"or", AggIdOfBitOr, []byte{255, 255}},
		{"xor", AggIdOfBitXor, []byte{255, 255}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			typ := types.T_blob.ToType()
			input := vector.NewVec(typ)
			t.Cleanup(func() { input.Free(mp) })
			require.NoError(t, vector.AppendBytesList(input,
				[][]byte{{0x0F, 0}, {0xF0, 255}, {}, nil, bytes.Repeat([]byte{0xAA}, 511)},
				[]bool{false, false, false, true, false}, mp))
			exec, err := MakeAgg(mp, tc.id, false, typ)
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			require.NoError(t, exec.GroupGrow(5))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 2, 3, 5}, []*vector.Vector{input}))
			// An empty partial is an identity, including for a real empty string.
			empty := makeBitAggExec(mp, tc.id, typ)
			t.Cleanup(empty.Free)
			require.NoError(t, empty.GroupGrow(5))
			require.NoError(t, exec.BatchMerge(empty, 0, []uint64{1, 2, 3, 4, 5}))
			result, err := exec.Flush()
			require.NoError(t, err)
			for _, v := range result {
				t.Cleanup(func() { v.Free(mp) })
			}
			require.Equal(t, typ, *result[0].GetType())
			require.False(t, result[0].HasNull())
			require.Equal(t, tc.want, result[0].GetBytesAt(0))
			for _, row := range []int{1, 2, 3} {
				require.Empty(t, result[0].GetBytesAt(row))
			}
			require.Equal(t, bytes.Repeat([]byte{0xAA}, 511), result[0].GetBytesAt(4))
		})
	}
}

func TestBitOpsBlobPartialRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		id   int64
		want byte
	}{
		{AggIdOfBitAnd, 0}, {AggIdOfBitOr, 255}, {AggIdOfBitXor, 255},
	} {
		t.Run(fmt.Sprintf("aggregate=%d", tc.id), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			typ := types.T_blob.ToType()
			newExec := func() AggFuncExec {
				e, err := MakeAgg(mp, tc.id, false, typ)
				require.NoError(t, err)
				t.Cleanup(e.Free)
				return e
			}
			input := vector.NewVec(typ)
			t.Cleanup(func() { input.Free(mp) })
			require.NoError(t, vector.AppendBytesList(input, [][]byte{{15}, {240}}, nil, mp))
			source := newExec()
			require.NoError(t, source.GroupGrow(1))
			require.NoError(t, source.Fill(0, 0, []*vector.Vector{input}))
			var wire bytes.Buffer
			require.NoError(t, source.SaveIntermediateResultOfChunk(0, &wire))
			restored := newExec()
			require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))
			target := newExec()
			require.NoError(t, target.GroupGrow(1))
			require.NoError(t, target.Merge(restored, 0, 0))
			second := newExec()
			require.NoError(t, second.GroupGrow(1))
			require.NoError(t, second.Fill(0, 1, []*vector.Vector{input}))
			require.NoError(t, target.Merge(second, 0, 0))
			result, err := target.Flush()
			require.NoError(t, err)
			for _, v := range result {
				t.Cleanup(func() { v.Free(mp) })
			}
			require.Equal(t, []byte{tc.want}, result[0].GetBytesAt(0))
			require.Equal(t, []byte{15}, restored.(*bitOpExecBytes).state[0].vecs[0].GetBytesAt(0), "merge must copy partial bytes")
		})
	}
}

func TestBitOpsBlobOperandValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  [][]byte
		nulls   []bool
		groups  []uint64
		errCode uint16
	}{
		{"oversized first", [][]byte{make([]byte, 512)}, nil, []uint64{1}, moerr.ErrInvalidBitwiseAggregateOperandsSize},
		{"oversized later", [][]byte{make([]byte, 511), make([]byte, 512)}, nil, []uint64{1, 1}, moerr.ErrInvalidBitwiseAggregateOperandsSize},
		{"unequal", [][]byte{{1}, {1, 2}}, nil, []uint64{1, 1}, moerr.ErrInvalidBitwiseOperandsSize},
		{"empty then nonempty", [][]byte{{}, {1}}, nil, []uint64{1, 1}, moerr.ErrInvalidBitwiseOperandsSize},
		{"ignored null and unmatched", [][]byte{make([]byte, 512), make([]byte, 512), {15}}, []bool{true, false, false}, []uint64{1, GroupNotMatched, 1}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			typ := types.T_blob.ToType()
			input := vector.NewVec(typ)
			t.Cleanup(func() { input.Free(mp) })
			require.NoError(t, vector.AppendBytesList(input, tc.values, tc.nulls, mp))
			exec := makeBitAggExec(mp, AggIdOfBitOr, typ)
			t.Cleanup(exec.Free)
			require.NoError(t, exec.GroupGrow(1))
			err := exec.BatchFill(0, tc.groups, []*vector.Vector{input})
			if tc.errCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.errCode), "%v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBitOpsBlobPreflightValidation(t *testing.T) {
	for _, initialized := range []bool{false, true} {
		t.Run(fmt.Sprintf("initialized=%t", initialized), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			registry, account, allocation := newTestAggregateAllocation(t)
			t.Cleanup(func() { finishTestAggregateAllocation(t, registry, account) })
			typ := types.T_blob.ToType()
			exec := makeBitAggExec(mp, AggIdOfBitOr, typ).(*bitOpExecBytes)
			require.NoError(t, exec.SetAllocationAccount(allocation))
			t.Cleanup(func() { exec.Free(); require.NoError(t, exec.ClearAllocationAccount(allocation)) })
			require.NoError(t, exec.GroupGrow(1))
			input := vector.NewVec(typ)
			t.Cleanup(func() { input.Free(mp) })
			require.NoError(t, vector.AppendBytesList(input, [][]byte{make([]byte, 511), make([]byte, 512)}, nil, mp))
			if initialized {
				require.NoError(t, exec.Fill(0, 0, []*vector.Vector{input}))
			}
			used := account.Snapshot().Used
			for _, groups := range [][]uint64{{1}, {1, 1}} {
				offset := 2 - len(groups)
				err := exec.PreflightBatchFill(offset, groups, []*vector.Vector{input})
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidBitwiseAggregateOperandsSize), "%v", err)
				require.Equal(t, used, account.Snapshot().Used, "reject before area reservation")
			}
			// Simulate a restored partial violating the new producer invariant.
			source := makeBitAggExec(mp, AggIdOfBitOr, typ).(*bitOpExecBytes)
			t.Cleanup(source.Free)
			require.NoError(t, source.GroupGrow(2))
			for row, value := range [][]byte{make([]byte, 511), make([]byte, 512)} {
				require.NoError(t, vector.SetBytesAt(source.state[0].vecs[0], row, value, mp))
				source.state[0].vecs[0].UnsetNull(uint64(row))
			}
			for _, groups := range [][]uint64{{1}, {1, 1}} {
				err := exec.PreflightBatchMerge(source, 2-len(groups), groups)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidBitwiseAggregateOperandsSize), "%v", err)
				require.Equal(t, used, account.Snapshot().Used)
			}
			err := exec.Merge(source, 0, 1)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidBitwiseAggregateOperandsSize), "%v", err)
		})
	}
}

// BenchmarkBitOpsAccountedPreflight measures steady-state batches with an
// initialized destination, where preflight should not reserve more capacity.
func BenchmarkBitOpsAccountedPreflight(b *testing.B) {
	for _, tc := range []struct {
		name  string
		typ   types.Type
		width int
	}{
		{"binary-1", types.New(types.T_binary, 1, 0), 1},
		{"binary-64", types.New(types.T_binary, 64, 0), 64},
		{"binary-511", types.New(types.T_binary, 511, 0), 511},
		{"varbinary-64", types.New(types.T_varbinary, 64, 0), 64},
		{"blob-64", types.T_blob.ToType(), 64},
		{"uint64", types.T_uint64.ToType(), 0},
	} {
		for _, merge := range []bool{false, true} {
			path := "fill"
			if merge {
				path = "merge"
			}
			b.Run(tc.name+"/"+path, func(b *testing.B) {
				const rows = 256
				mp := mpool.MustNewZero()
				b.Cleanup(func() { require.Zero(b, mp.CurrNB()) })
				registry, account, allocation := newTestAggregateAllocation(b)
				b.Cleanup(func() { finishTestAggregateAllocation(b, registry, account) })
				input := vector.NewVec(tc.typ)
				b.Cleanup(func() { input.Free(mp) })
				value := bytes.Repeat([]byte{0xA5}, tc.width)
				groups := make([]uint64, rows)
				sourceGroups := make([]uint64, rows)
				for row := range groups {
					groups[row] = 1
					sourceGroups[row] = uint64(row + 1)
					if tc.width == 0 {
						require.NoError(b, vector.AppendFixed(input, uint64(0xA5), false, mp))
					} else {
						require.NoError(b, vector.AppendBytes(input, value, false, mp))
					}
				}
				vectors := []*vector.Vector{input}
				newExec := func(groupCount int) AggFuncExec {
					exec := makeBitAggExec(mp, AggIdOfBitOr, tc.typ)
					owner := exec.(AllocationAccountOwner)
					require.NoError(b, owner.SetAllocationAccount(allocation))
					b.Cleanup(func() {
						exec.Free()
						require.NoError(b, owner.ClearAllocationAccount(allocation))
					})
					require.NoError(b, exec.GroupGrow(groupCount))
					return exec
				}
				target := newExec(1)
				preflight := target.(BatchCapacityPreflight)
				require.NoError(b, preflight.PreflightBatchFill(0, groups, vectors))
				require.NoError(b, target.BatchFill(0, groups, vectors))
				var source AggFuncExec
				if merge {
					source = newExec(rows)
					require.NoError(b, source.(BatchCapacityPreflight).PreflightBatchFill(0, sourceGroups, vectors))
					require.NoError(b, source.BatchFill(0, sourceGroups, vectors))
				}
				used := account.Snapshot().Used
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if merge {
						if err := preflight.PreflightBatchMerge(source, 0, groups); err != nil {
							b.Fatal(err)
						}
						if err := target.BatchMerge(source, 0, groups); err != nil {
							b.Fatal(err)
						}
					} else {
						if err := preflight.PreflightBatchFill(0, groups, vectors); err != nil {
							b.Fatal(err)
						}
						if err := target.BatchFill(0, groups, vectors); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				require.Equal(b, used, account.Snapshot().Used)
				result, err := target.Flush()
				require.NoError(b, err)
				for _, vec := range result {
					b.Cleanup(func() { vec.Free(mp) })
				}
				require.Len(b, result, 1)
				if tc.width == 0 {
					require.Equal(b, uint64(0xA5), vector.MustFixedColNoTypeCheck[uint64](result[0])[0])
				} else {
					require.Equal(b, value, result[0].GetBytesAt(0))
				}
			})
		}
	}
}
