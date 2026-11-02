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

func TestSingleGroupReuseMatchesBulkFlush(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    types.Type
		values []any
	}{
		{"int8", types.T_int8.ToType(), []any{int8(9), int8(-1), int8(2), int8(0), int8(-7)}},
		{"int16", types.T_int16.ToType(), []any{int16(9), int16(-1), int16(2), int16(0), int16(-7)}},
		{"int32", types.T_int32.ToType(), []any{int32(9), int32(-1), int32(2), int32(0), int32(-7)}},
		{"wide-int32-avg", types.New(types.T_int32, 35, 0), []any{int32(9), int32(-1), int32(2), int32(0), int32(-7)}},
		{"uint8", types.T_uint8.ToType(), []any{uint8(9), uint8(1), uint8(2), uint8(0), uint8(7)}},
		{"uint16", types.T_uint16.ToType(), []any{uint16(9), uint16(1), uint16(2), uint16(0), uint16(7)}},
		{"uint32", types.T_uint32.ToType(), []any{uint32(9), uint32(1), uint32(2), uint32(0), uint32(7)}},
		{"year", types.T_year.ToType(), []any{types.MoYear(9), types.MoYear(1), types.MoYear(2), types.MoYear(0), types.MoYear(7)}},
		{"bit", types.T_bit.ToType(), []any{uint64(9), uint64(1), uint64(2), uint64(0), uint64(7)}},
		{"float32", types.T_float32.ToType(), []any{float32(9), float32(-1), float32(2), float32(0), float32(-7)}},
		{"float64", types.T_float64.ToType(), []any{float64(9), float64(-1e20), float64(2), float64(0), float64(1e20)}},
		{"varchar-count", types.T_varchar.ToType(), []any{[]byte("x"), []byte("a"), []byte("b"), []byte(""), []byte("c")}},
	} {
		for _, id := range []int64{AggIdOfCountStar, AggIdOfCountColumn, AggIdOfSum, AggIdOfAvg} {
			if tc.typ.Oid == types.T_varchar && id != AggIdOfCountColumn && id != AggIdOfCountStar {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s", tc.name, map[int64]string{AggIdOfCountStar: "count-star", AggIdOfCountColumn: "count-column", AggIdOfSum: "sum", AggIdOfAvg: "avg"}[id]), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				registry, account, allocation := newTestAggregateAllocation(t)
				defer func() {
					finishTestAggregateAllocation(t, registry, account)
					require.Zero(t, mp.CurrNB())
				}()
				input := vector.NewVec(tc.typ)
				defer input.Free(mp)
				for i, v := range tc.values {
					require.NoError(t, vector.AppendAny(input, v, i == 2, mp))
				}
				constant := vector.NewVec(tc.typ)
				defer constant.Free(mp)
				require.NoError(t, vector.AppendAny(constant, tc.values[1], false, mp))
				constant.ToConst()
				constant.SetLength(1)
				nulls := vector.NewConstNull(tc.typ, 1, mp)
				defer nulls.Free(mp)
				makeExec := func() GroupAggFuncExec {
					params := []types.Type{tc.typ}
					if id == AggIdOfCountStar {
						params = nil
					}
					exec, err := MakeSingleGroupAgg(mp, id, false, allocation, nil, params...)
					require.NoError(t, err)
					SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
					if err := exec.GroupGrow(1); err != nil {
						exec.Free()
						require.NoError(t, err)
					}
					return exec
				}
				exec := makeExec()
				defer exec.Free()
				require.True(t, SupportsSingleGroupReuse([]GroupAggFuncExec{exec}))
				var dst *vector.Vector
				defer func() {
					if dst != nil {
						dst.Free(mp)
					}
				}()
				var expected []any
				var expectedNull []bool
				for _, group := range [][]struct {
					v           *vector.Vector
					start, rows int
				}{
					{{input, 1, 2}, {input, 3, 2}}, // two runs, preserving local addition order
					{{nulls, 1, 3}},
					{{constant, 2, 3}},
					{{input, 0, 0}},
					{{input, 1, 1}},
				} {
					func() {
						oracle := makeExec()
						defer oracle.Free()
						for _, run := range group {
							require.NoError(t, BulkFillSingleGroupRange(exec, run.start, run.rows, []*vector.Vector{run.v}))
							window, err := run.v.WindowByLogicalRows(run.start, run.start+run.rows)
							require.NoError(t, err)
							defer window.Free(mp)
							require.NoError(t, oracle.BulkFill(0, []*vector.Vector{window}))
						}
						results, err := oracle.Flush()
						require.NoError(t, err)
						defer func() {
							for _, result := range results {
								result.Free(mp)
							}
						}()
						require.Len(t, results, 1)
						result, err := AppendSingleGroupResultAndReset(exec, dst)
						require.NoError(t, err)
						dst = result
						require.Equal(t, *results[0].GetType(), *dst.GetType())
						expectedNull = append(expectedNull, results[0].IsNull(0))
						expected = append(expected, vector.GetAny(results[0], 0, true))
					}()
					for row := range expected {
						require.Equal(t, expectedNull[row], dst.IsNull(uint64(row)))
						if !expectedNull[row] {
							require.Equal(t, expected[row], vector.GetAny(dst, row, true))
						}
					}
				}
				exec.Free() // retained results must survive state destruction too.
				for row := range expected {
					if !expectedNull[row] {
						require.Equal(t, expected[row], vector.GetAny(dst, row, true))
					}
				}
			})
		}
	}
}

func TestSingleGroupReuseRejectsUnsupported(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	defer func() { require.Zero(t, mp.CurrNB()) }()
	supported, err := MakeSingleGroupAgg(mp, AggIdOfCountStar, false, nil, nil)
	require.NoError(t, err)
	defer supported.Free()
	require.False(t, SupportsSingleGroupReuse([]GroupAggFuncExec{supported}))
	SyncAggregatorsToChunkSize([]GroupAggFuncExec{supported}, 1)
	require.NoError(t, supported.GroupGrow(1))
	require.False(t, SupportsSingleGroupReuse(nil))
	for _, tc := range []struct {
		name               string
		id                 int64
		distinct, ordinary bool
		typ                types.Type
		extra              any
	}{
		{"distinct", AggIdOfCountColumn, true, false, types.T_int32.ToType(), nil},
		{"decimal-sum", AggIdOfSum, false, false, types.T_int64.ToType(), nil},
		{"decimal-avg", AggIdOfAvg, false, false, types.T_decimal64.ToType(), nil},
		{"min", AggIdOfMin, false, false, types.T_int32.ToType(), nil},
		{"extra", AggIdOfCountColumn, false, false, types.T_int32.ToType(), int64(2)},
		{"star-extra", AggIdOfCountStar, false, false, types.T_int32.ToType(), int64(2)},
		{"ordinary", AggIdOfAvg, false, true, types.T_int32.ToType(), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var agg GroupAggFuncExec
			var err error
			if tc.ordinary {
				agg, err = MakeGroupAgg(mp, tc.id, tc.distinct, nil, tc.extra, tc.typ)
			} else {
				agg, err = MakeSingleGroupAgg(mp, tc.id, tc.distinct, nil, tc.extra, tc.typ)
			}
			require.NoError(t, err)
			defer agg.Free()
			if !tc.ordinary {
				SyncAggregatorsToChunkSize([]GroupAggFuncExec{agg}, 1)
			}
			require.NoError(t, agg.GroupGrow(1))
			require.False(t, SupportsSingleGroupReuse([]GroupAggFuncExec{supported, agg}))
			_, err = AppendSingleGroupResultAndReset(agg, nil)
			require.Error(t, err)
			require.Error(t, BulkFillSingleGroupRange(agg, 0, 0, nil))
			results, err := agg.Flush() // rejection leaves the ordinary owner intact.
			require.NoError(t, err)
			for _, result := range results {
				result.Free(mp)
			}
		})
	}
	for _, params := range [][]types.Type{nil, {types.T_int32.ToType(), types.T_int32.ToType()}} {
		agg, err := MakeSingleGroupAgg(mp, AggIdOfCountColumn, false, nil, nil, params...)
		require.NoError(t, err)
		defer agg.Free()
		SyncAggregatorsToChunkSize([]GroupAggFuncExec{agg}, 1)
		require.NoError(t, agg.GroupGrow(1))
		require.False(t, SupportsSingleGroupReuse([]GroupAggFuncExec{supported, agg}))
	}
	var typedNil *countStarExec
	require.False(t, SupportsSingleGroupReuse([]GroupAggFuncExec{typedNil}))
}

func TestSingleGroupReuseRangeErrorsAndOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	defer func() { require.Zero(t, mp.CurrNB()) }()
	input := buildAvgFixedVector(t, mp, types.T_int32.ToType(), []int32{1, 2})
	defer input.Free(mp)
	for _, sum := range []bool{true, false} {
		exec := newSumAvgExec[int64, int32](mp, int64OfCheck, sum, AggIdOfSum, false, types.T_int32.ToType()).(*sumAvgExec[int64, int32])
		defer exec.Free()
		exec.modifyChunkSize(1)
		require.NoError(t, exec.GroupGrow(1))
		for _, bounds := range [][2]int{{-1, 1}, {0, -1}, {2, 1}, {math.MaxInt, 1}, {1, math.MaxInt}} {
			require.Error(t, BulkFillSingleGroupRange(exec, bounds[0], bounds[1], []*vector.Vector{input}))
		}
		vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[0])[0] = math.MaxInt64
		require.Error(t, BulkFillSingleGroupRange(exec, 0, 1, []*vector.Vector{input}))
		require.Equal(t, int64(math.MaxInt64), vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[0])[0])
		wrong := vector.NewVec(types.T_float64.ToType())
		defer wrong.Free(mp)
		_, err := AppendSingleGroupResultAndReset(exec, wrong)
		require.Error(t, err)
		_, err = AppendSingleGroupResultAndReset(exec, exec.state[0].vecs[0])
		require.Error(t, err)
	}
}

func TestSingleGroupReuseAllocationFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		id         int64
		null       bool
		failOffset int
	}{
		{"new-data", AggIdOfCountColumn, false, 1},
		{"new-nulls", AggIdOfSum, true, 2},
		{"existing-growth", AggIdOfAvg, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			controller := &rejectNthAggregateAllocation{}
			registry, err := mpool.NewAllocationAccountRegistry(1, 512)
			require.NoError(t, err)
			account, err := registry.OpenWithController(128<<20, controller)
			require.NoError(t, err)
			defer func() {
				finishTestAggregateAllocation(t, registry, account)
				require.Zero(t, controller.used)
				require.Zero(t, mp.CurrNB())
			}()
			allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{VectorData: 1, VectorArea: 2, VectorNulls: 3, VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6})
			require.NoError(t, err)
			exec, err := MakeSingleGroupAgg(mp, tc.id, false, allocation, nil, types.T_int32.ToType())
			require.NoError(t, err)
			defer exec.Free()
			SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
			require.NoError(t, exec.GroupGrow(1))
			input := buildAvgFixedVector(t, mp, types.T_int32.ToType(), []int32{7})
			defer input.Free(mp)
			var dst *vector.Vector
			defer func() {
				if dst != nil {
					dst.Free(mp)
				}
			}()
			if tc.name == "existing-growth" {
				dst, err = AppendSingleGroupResultAndReset(exec, nil)
				require.NoError(t, err)
				for dst.Length() < dst.Capacity() {
					_, err = AppendSingleGroupResultAndReset(exec, dst)
					require.NoError(t, err)
				}
			}
			if !tc.null {
				require.NoError(t, BulkFillSingleGroupRange(exec, 0, 1, []*vector.Vector{input}))
			}
			before := account.Snapshot().Used
			oldLen := 0
			if dst != nil {
				oldLen = dst.Length()
			}
			controller.failAt = controller.calls + tc.failOffset
			result, err := AppendSingleGroupResultAndReset(exec, dst)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.True(t, controller.rejected)
			require.Nil(t, result)
			require.Equal(t, before, account.Snapshot().Used)
			if dst != nil {
				require.Equal(t, oldLen, dst.Length())
			}
			controller.failAt = 0
			result, err = AppendSingleGroupResultAndReset(exec, dst)
			require.NoError(t, err)
			dst = result
			require.Equal(t, tc.null, dst.IsNull(uint64(oldLen)))
			if !tc.null {
				if tc.id == AggIdOfCountColumn {
					require.Equal(t, int64(1), vector.GetAny(dst, oldLen, false))
				} else {
					require.NoError(t, (&expectedResult{expected: float64(7)}).checkVecAt(dst, oldLen))
				}
			}
			// A later group appends independently after denial and successful reset.
			require.NoError(t, BulkFillSingleGroupRange(exec, 0, 1, []*vector.Vector{input}))
			_, err = AppendSingleGroupResultAndReset(exec, dst)
			require.NoError(t, err)
			require.Equal(t, oldLen+2, dst.Length())
			wrong := vector.NewVec(*dst.GetType()) // matching type, wrong allocation generation
			defer wrong.Free(mp)
			_, err = AppendSingleGroupResultAndReset(exec, wrong)
			require.Error(t, err)
		})
	}
}

func TestSingleGroupReuseExactAvgRounding(t *testing.T) {
	for _, typ := range []types.Type{types.T_int32.ToType(), types.New(types.T_int32, 35, 0)} {
		for _, sign := range []int32{1, -1} {
			t.Run(fmt.Sprintf("%v/%d", typ, sign), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				defer func() { require.Zero(t, mp.CurrNB()) }()
				exec, err := MakeSingleGroupAgg(mp, AggIdOfAvg, false, nil, nil, typ)
				require.NoError(t, err)
				defer exec.Free()
				SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
				require.NoError(t, exec.GroupGrow(1))
				values := make([]int32, 113)
				values[1] = sign
				input := buildAvgFixedVector(t, mp, typ, values)
				defer input.Free(mp)
				require.NoError(t, BulkFillSingleGroupRange(exec, 0, 113, []*vector.Vector{input}))
				dst, err := AppendSingleGroupResultAndReset(exec, nil)
				require.NoError(t, err)
				defer dst.Free(mp)
				want := "0.0088"
				if sign < 0 {
					want = "-0.0088"
				}
				if dst.GetType().Oid == types.T_decimal128 {
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal128](dst, 0).Format(4))
				} else {
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal256](dst, 0).Format(4))
				}
			})
		}
	}
}
