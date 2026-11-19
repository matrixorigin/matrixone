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

package top

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

type orderedTestRow struct {
	a       int64
	null    bool
	b       int64
	payload int64
}

func TestTopVerifiedBatchSuffix(t *testing.T) {
	for _, tt := range []struct {
		name   string
		values []int64
		desc   bool
		k      uint64
		want   int
	}{
		{"reverse order", []int64{1, 2, 3, 4, 5}, true, 2, 3},
		{"same direction", []int64{1, 2, 3, 4, 5}, false, 2, 0},
		{"endpoint trap", []int64{5, 1, 4, 2, 3}, false, 1, 0},
		{"equal endpoints hide interior", []int64{5, 0, 5}, false, 1, 0},
		{"ties at cutoff", []int64{9, 8, 7, 6, 6, 4, 4, 4, 3}, false, 3, 6},
		{"tied sample with a winning tail", []int64{5, 5, 5, 5, 3}, false, 2, 0},
		{"limit fills batch", []int64{5, 4, 3}, false, 3, 0},
		{"limit exceeds batch", []int64{5, 4, 3}, false, 4, 0},
		{"zero limit", []int64{5, 4, 3}, false, 0, 0},
		{"one row", []int64{5}, false, 1, 0},
		{"empty", nil, false, 1, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			tc := newTestCase(t, mp, []types.Type{types.T_int64.ToType()}, 1, []*plan.OrderBySpec{{Expr: newTypedInt64Expression(0)}})
			defer tc.proc.Free()
			bat := batch.NewWithSize(1)
			defer bat.Clean(mp)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			for _, v := range tt.values {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, false, mp))
			}
			bat.SetRowCount(len(tt.values))
			ctr := container{limit: tt.k, poses: []int32{0}, cmps: []compare.Compare{compare.NewOrder(types.T_int64.ToType(), tt.desc, false)}}
			got, err := ctr.verifiedBatchSuffix(bat, tc.proc)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTopBatchPruningUnsupportedKeysDoNotBind(t *testing.T) {
	for _, kind := range []string{"constant", "array", "json"} {
		t.Run(kind, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			tc := newTestCase(t, mp, nil, 1, nil)
			defer tc.proc.Free()
			bat := batch.NewWithSize(2)
			defer bat.Clean(mp)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			for row := range 5 {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row), false, mp))
			}
			switch kind {
			case "constant":
				var err error
				bat.Vecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), int64(7), 5, mp)
				require.NoError(t, err)
			case "array":
				bat.Vecs[1] = vector.NewVec(types.T_array_float64.ToType())
				for row := range 5 {
					require.NoError(t, vector.AppendBytes(bat.Vecs[1], types.ArrayToBytes([]float64{float64(row)}), false, mp))
				}
			case "json":
				bat.Vecs[1] = vector.NewVec(types.T_json.ToType())
				for row := range 5 {
					value, err := bytejson.CreateByteJSON(int64(row))
					require.NoError(t, err)
					require.NoError(t, vector.AppendByteJson(bat.Vecs[1], value, false, mp))
				}
			}
			bat.SetRowCount(5)
			// Nil comparators prove that even the earlier eligible key is not
			// bound before an unsupported key causes fallback.
			ctr := container{limit: 1, poses: []int32{0, 1}, cmps: make([]compare.Compare, 2)}
			start, err := ctr.verifiedBatchSuffix(bat, tc.proc)
			require.NoError(t, err)
			require.Zero(t, start)
		})
	}
}

func TestTopBatchPruningJSONNumericFallback(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	expr := newExpression(0)
	expr.Typ.Id = int32(types.T_json)
	tc := newTestCase(t, mp, []types.Type{types.T_json.ToType(), types.T_int64.ToType()}, 1,
		[]*plan.OrderBySpec{{Expr: expr}})
	defer tc.proc.Free()
	defer tc.arg.Free(tc.proc, false, nil)
	child := colexec.NewMockOperator()
	defer child.Free(tc.proc, false, nil)
	tc.arg.AppendChild(child)
	bat := batch.NewWithSize(2)
	child.WithBatchs([]*batch.Batch{bat})
	bat.Vecs[0] = vector.NewVec(types.T_json.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	high, err := bytejson.CreateByteJSON(int64(1000000000000000200))
	require.NoError(t, err)
	integer, err := bytejson.CreateByteJSON(int64(1000000000000000100))
	require.NoError(t, err)
	floating, err := bytejson.CreateByteJSON(float64(1000000000000000100))
	require.NoError(t, err)
	text := "1000000000000000100"
	encoded := make([]byte, binary.MaxVarintLen64+len(text))
	n := binary.PutUvarint(encoded, uint64(len(text)))
	copy(encoded[n:], text)
	decimal := bytejson.ByteJson{Type: bytejson.TpCodeDecimal, Data: encoded[:n+len(text)]}
	for id, value := range []bytejson.ByteJson{high, integer, decimal, floating} {
		require.NoError(t, vector.AppendByteJson(bat.Vecs[0], value, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(id), false, mp))
	}
	bat.SetRowCount(4)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
}

func TestTopOrderedBatchMixedKeysAndPayload(t *testing.T) {
	// Each batch is reverse-ordered for (a ASC NULLS FIRST, b DESC).
	batches := [][]orderedTestRow{
		{{3, false, 1, 31}, {3, false, 2, 32}, {2, false, 1, 21}, {2, false, 2, 22}, {1, false, 1, 11}, {1, false, 2, 12}},
		{{4, false, 1, 41}, {4, false, 2, 42}, {3, false, 3, 33}, {1, false, 3, 13}, {0, true, 1, 101}, {0, true, 2, 102}},
	}
	want := []orderedTestRow{{0, true, 2, 102}, {0, true, 1, 101}, {1, false, 3, 13}}
	for _, mode := range []string{"fixed", "resident", "spill"} {
		t.Run(mode, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			payloadType := types.T_int64.ToType()
			if mode != "fixed" {
				payloadType = types.T_varchar.ToType()
			}
			tc := newTestCase(t, mp, []types.Type{types.T_int64.ToType(), types.T_int64.ToType(), payloadType}, 3,
				[]*plan.OrderBySpec{{Expr: newTypedInt64Expression(0), Flag: plan.OrderBySpec_NULLS_FIRST}, {Expr: newTypedInt64Expression(1), Flag: plan.OrderBySpec_DESC}})
			defer tc.proc.Free()
			defer tc.arg.Free(tc.proc, false, nil)
			// Rebuild inputs and reset the same operator to cover the next generation.
			for range 2 {
				func() {
					child := colexec.NewMockOperator()
					defer child.Free(tc.proc, false, nil)
					tc.arg.Children = nil
					tc.arg.AppendChild(child)
					for _, rows := range batches {
						bat := batch.NewWithSize(3)
						child.WithBatchs([]*batch.Batch{bat})
						bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
						bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
						bat.Vecs[2] = vector.NewVec(payloadType)
						for _, row := range rows {
							require.NoError(t, vector.AppendFixed(bat.Vecs[0], row.a, row.null, mp))
							require.NoError(t, vector.AppendFixed(bat.Vecs[1], row.b, false, mp))
							if mode == "fixed" {
								require.NoError(t, vector.AppendFixed(bat.Vecs[2], row.payload, false, mp))
							} else {
								require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte(fmt.Sprintf("p%d", row.payload)), false, mp))
							}
						}
						bat.SetRowCount(len(rows))
					}
					require.NoError(t, tc.arg.Prepare(tc.proc))
					if mode == "spill" {
						tc.arg.ctr.spilling = true
					}
					defer tc.arg.Reset(tc.proc, false, nil)
					var got []orderedTestRow
					for {
						result, err := vm.Exec(tc.arg, tc.proc)
						require.NoError(t, err)
						if result.Batch == nil {
							break
						}
						for i := 0; i < result.Batch.RowCount(); i++ {
							row := orderedTestRow{null: result.Batch.Vecs[0].IsNull(uint64(i)), b: vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[1], i)}
							if !row.null {
								row.a = vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[0], i)
							}
							if mode == "fixed" {
								row.payload = vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[2], i)
							} else {
								n, err := fmt.Sscanf(string(result.Batch.Vecs[2].GetBytesAt(i)), "p%d", &row.payload)
								require.NoError(t, err)
								require.Equal(t, 1, n)
							}
							got = append(got, row)
						}
					}
					require.Equal(t, want, got)
					if mode == "resident" {
						require.True(t, tc.arg.ctr.boundedResident)
						require.False(t, tc.arg.ctr.spilling)
					}
					if mode == "spill" {
						require.True(t, tc.arg.ctr.spilling)
					}
				}()
			}
		})
	}
}

// Scalar ORDER BY defines a total NaN order; array comparison does not.
func TestTopOrderedBatchFloatNaNAndArrayFallback(t *testing.T) {
	for _, array := range []bool{false, true} {
		name := "scalar"
		if array {
			name = "array fallback"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			typ := types.T_float64.ToType()
			if array {
				typ = types.New(types.T_array_float64, 1, 0)
			}
			expr := newExpression(0)
			expr.Typ.Id = int32(typ.Oid)
			tc := newTestCase(t, mp, []types.Type{typ, types.T_int64.ToType()}, 1, []*plan.OrderBySpec{{Expr: expr}})
			defer tc.proc.Free()
			defer tc.arg.Free(tc.proc, false, nil)
			child := colexec.NewMockOperator()
			defer child.Free(tc.proc, false, nil)
			tc.arg.AppendChild(child)
			bat := batch.NewWithSize(2)
			child.WithBatchs([]*batch.Batch{bat})
			bat.Vecs[0] = vector.NewVec(typ)
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			vals := []float64{math.NaN(), 3, 2, 1}
			if array {
				vals = []float64{2, math.NaN(), 0, math.NaN(), 1}
			}
			for i, v := range vals {
				if array {
					require.NoError(t, vector.AppendBytes(bat.Vecs[0], types.ArrayToBytes([]float64{v}), false, mp))
				} else {
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, false, mp))
				}
				require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(i), false, mp))
			}
			bat.SetRowCount(len(vals))
			require.NoError(t, tc.arg.Prepare(tc.proc))
			result, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			require.NotNil(t, result.Batch)
			want := int64(3)
			if array {
				want = 2
			}
			require.Equal(t, []int64{want}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
		})
	}
}

func TestTopBatchVerificationCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	tc := newTestCase(t, mp, []types.Type{types.T_int64.ToType()}, 1, []*plan.OrderBySpec{{Expr: newTypedInt64Expression(0), Flag: plan.OrderBySpec_DESC}})
	defer tc.proc.Free()
	defer tc.arg.Free(tc.proc, true, context.Canceled)
	child := colexec.NewMockOperator()
	defer child.Free(tc.proc, true, context.Canceled)
	tc.arg.AppendChild(child)
	bat := batch.NewWithSize(1)
	child.WithBatchs([]*batch.Batch{bat})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for i := 0; i < evalSpillChunkSize+1; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, mp))
	}
	bat.SetRowCount(evalSpillChunkSize + 1)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	cancelCtx := newCancelOnDoneCheckContext(tc.proc.Ctx, 2)
	child.WithBatchCallback(func(int) { tc.proc.Ctx = cancelCtx })
	result, err := vm.Exec(tc.arg, tc.proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Zero(t, cancelCtx.remaining)
}

func TestTopBatchPruningKeepsDistinctTies(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	tc := newTestCase(t, mp, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, 3, []*plan.OrderBySpec{{Expr: newTypedInt64Expression(0)}})
	defer tc.proc.Free()
	defer tc.arg.Free(tc.proc, false, nil)
	child := colexec.NewMockOperator()
	defer child.Free(tc.proc, false, nil)
	tc.arg.AppendChild(child)
	bat := batch.NewWithSize(2)
	child.WithBatchs([]*batch.Batch{bat})
	for i := range 2 {
		bat.Vecs[i] = vector.NewVec(types.T_int64.ToType())
	}
	for id, key := range []int64{9, 8, 7, 6, 6, 4, 4, 4, 3} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], key, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(id), false, mp))
	}
	bat.SetRowCount(9)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{3, 4, 4}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0]))
	ids := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])
	require.Equal(t, int64(8), ids[0])
	require.Contains(t, []int64{5, 6, 7}, ids[1])
	require.Contains(t, []int64{5, 6, 7}, ids[2])
	require.NotEqual(t, ids[1], ids[2])
}

func TestTopBatchPruningResidentToSpill(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	tc := newTestCase(t, mp, []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, 3, []*plan.OrderBySpec{{Expr: newTypedInt64Expression(0), Flag: plan.OrderBySpec_DESC}})
	tc.arg.ctr.residentByteLimit = 512
	defer tc.proc.Free()
	defer tc.arg.Free(tc.proc, false, nil)
	for range 2 {
		func() {
			child := colexec.NewMockOperator()
			defer child.Free(tc.proc, false, nil)
			tc.arg.Children = nil
			tc.arg.AppendChild(child)
			for start := 0; start < 16; start += 8 {
				bat := batch.NewWithSize(2)
				child.WithBatchs([]*batch.Batch{bat})
				bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
				for value := start; value < start+8; value++ {
					payload := []byte(fmt.Sprintf("v%d", value))
					if value == 15 {
						payload = make([]byte, 1024)
						for i := range payload {
							payload[i] = 'x'
						}
					}
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(value), false, mp))
					require.NoError(t, vector.AppendBytes(bat.Vecs[1], payload, false, mp))
				}
				bat.SetRowCount(8)
			}
			require.NoError(t, tc.arg.Prepare(tc.proc))
			defer tc.arg.Reset(tc.proc, false, nil)
			var keys []int64
			var payloads []string
			for {
				result, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				keys = append(keys, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])...)
				for i := 0; i < result.Batch.RowCount(); i++ {
					payloads = append(payloads, string(result.Batch.Vecs[1].GetBytesAt(i)))
				}
			}
			require.Equal(t, []int64{15, 14, 13}, keys)
			require.Equal(t, 1024, len(payloads[0]))
			require.Equal(t, "v14", payloads[1])
			require.Equal(t, "v13", payloads[2])
			require.True(t, tc.arg.ctr.spilling)
		}()
	}
}

func TestTopBatchPruningComputedAndRemappedKeys(t *testing.T) {
	for _, name := range []string{"fixed", "resident to spill", "constant key"} {
		migrate := name == "resident to spill"
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			payloadType := types.T_int64.ToType()
			if migrate {
				payloadType = types.T_varchar.ToType()
			}
			tc := newTestCase(t, mp, []types.Type{types.T_int64.ToType(), types.T_int64.ToType(), payloadType}, 3, nil)
			defer tc.proc.Free()
			defer tc.arg.Free(tc.proc, false, nil)
			computed, err := plan2.BindFuncExprImplByPlanExpr(tc.proc.Ctx, "+", []*plan.Expr{newTypedInt64Expression(1), plan2.MakePlan2Int64ConstExprWithType(1)})
			require.NoError(t, err)
			tc.arg.Fs = []*plan.OrderBySpec{
				{Expr: computed, Flag: plan.OrderBySpec_DESC},
				{Expr: newTypedInt64Expression(0)},
				{Expr: newTypedInt64Expression(0)},
			}
			if name == "constant key" {
				tc.arg.Fs = append([]*plan.OrderBySpec{{Expr: plan2.MakePlan2Int64ConstExprWithType(7)}}, tc.arg.Fs...)
			}
			if migrate {
				tc.arg.ctr.residentByteLimit = 512
			}
			child := colexec.NewMockOperator()
			defer child.Free(tc.proc, false, nil)
			tc.arg.AppendChild(child)
			for base := 0; base < 16; base += 8 {
				bat := batch.NewWithSize(3)
				child.WithBatchs([]*batch.Batch{bat})
				bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
				bat.Vecs[2] = vector.NewVec(payloadType)
				for rank := base; rank < base+8; rank++ {
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(100-rank), false, mp))
					require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(rank), false, mp))
					if migrate {
						payload := []byte(fmt.Sprintf("v%d", rank))
						if rank == 15 {
							payload = make([]byte, 1024)
							for i := range payload {
								payload[i] = 'x'
							}
						}
						require.NoError(t, vector.AppendBytes(bat.Vecs[2], payload, false, mp))
					} else {
						require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(1000+rank), false, mp))
					}
				}
				bat.SetRowCount(8)
			}
			require.NoError(t, tc.arg.Prepare(tc.proc))
			var ranks, ids []int64
			var payloads []string
			for {
				result, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				ids = append(ids, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])...)
				ranks = append(ranks, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])...)
				if migrate {
					for i := 0; i < result.Batch.RowCount(); i++ {
						payloads = append(payloads, string(result.Batch.Vecs[2].GetBytesAt(i)))
					}
				} else {
					for _, v := range vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2]) {
						payloads = append(payloads, fmt.Sprint(v))
					}
				}
			}
			require.Equal(t, []int64{15, 14, 13}, ranks)
			require.Equal(t, []int64{85, 86, 87}, ids)
			if migrate {
				require.True(t, tc.arg.ctr.spilling)
				require.Len(t, payloads[0], 1024)
				require.Equal(t, []string{"v14", "v13"}, payloads[1:])
			} else {
				require.Equal(t, []string{"1015", "1014", "1013"}, payloads)
			}
		})
	}
}

func TestTopBatchPruningFallbackKeepsLateWinners(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	tc := newTestCase(t, mp, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, 2, []*plan.OrderBySpec{{Expr: newTypedInt64Expression(0)}})
	defer tc.proc.Free()
	defer tc.arg.Free(tc.proc, false, nil)
	child := colexec.NewMockOperator()
	defer child.Free(tc.proc, false, nil)
	tc.arg.AppendChild(child)
	for _, rows := range [][]int64{{100}, {5, 5, 5, 5, 3}} {
		bat := batch.NewWithSize(2)
		child.WithBatchs([]*batch.Batch{bat})
		for i := range 2 {
			bat.Vecs[i] = vector.NewVec(types.T_int64.ToType())
		}
		for id, key := range rows {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], key, false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(id), false, mp))
		}
		bat.SetRowCount(len(rows))
	}
	require.NoError(t, tc.arg.Prepare(tc.proc))
	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{3, 5}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0]))
	ids := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])
	require.Equal(t, int64(4), ids[0])
	require.Contains(t, []int64{0, 1, 2, 3}, ids[1])
}

func TestTopBatchPruningKeepsConstantPayloads(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	typesIn := []types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType()}
	tc := newTestCase(t, mp, typesIn, 2,
		[]*plan.OrderBySpec{{Expr: newTypedInt64Expression(0), Flag: plan.OrderBySpec_DESC}})
	defer tc.proc.Free()
	defer tc.arg.Free(tc.proc, false, nil)
	child := colexec.NewMockOperator()
	defer child.Free(tc.proc, false, nil)
	tc.arg.AppendChild(child)
	bat := batch.NewWithSize(3)
	child.WithBatchs([]*batch.Batch{bat})
	bat.Vecs[0] = vector.NewVec(typesIn[0])
	for row := range 6 {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row), false, mp))
	}
	var err error
	bat.Vecs[1], err = vector.NewConstFixed(typesIn[1], int64(42), 6, mp)
	require.NoError(t, err)
	bat.Vecs[2] = vector.NewConstNull(typesIn[2], 6, mp)
	bat.SetRowCount(6)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{5, 4}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0]))
	require.Equal(t, []int64{42, 42}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
	require.True(t, result.Batch.Vecs[2].IsNull(0))
	require.True(t, result.Batch.Vecs[2].IsNull(1))
}
