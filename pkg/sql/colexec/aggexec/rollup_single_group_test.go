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
	"strings"
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
		{"bool", types.T_bool.ToType(), []any{true, false, true, false, true}},
		{"int64", types.T_int64.ToType(), []any{int64(9), int64(-1), int64(2), int64(0), int64(-7)}},
		{"uint64", types.T_uint64.ToType(), []any{uint64(9), uint64(1), uint64(2), uint64(0), uint64(7)}},
		{"date", types.T_date.ToType(), []any{types.Date(9), types.Date(1), types.Date(2), types.Date(0), types.Date(7)}},
		{"time", types.T_time.ToType(), []any{types.Time(9), types.Time(1), types.Time(2), types.Time(0), types.Time(7)}},
		{"datetime", types.T_datetime.ToType(), []any{types.Datetime(9), types.Datetime(1), types.Datetime(2), types.Datetime(0), types.Datetime(7)}},
		{"timestamp", types.T_timestamp.ToType(), []any{types.Timestamp(9), types.Timestamp(1), types.Timestamp(2), types.Timestamp(0), types.Timestamp(7)}},
		{"enum", types.T_enum.ToType(), []any{types.Enum(9), types.Enum(1), types.Enum(2), types.Enum(0), types.Enum(7)}},
		{"decimal64", types.T_decimal64.ToType(), []any{types.Decimal64(9), types.Decimal64(1), types.Decimal64(2), types.Decimal64(0), types.Decimal64(7)}},
		{"decimal128", types.T_decimal128.ToType(), []any{types.Decimal128FromInt64(9), types.Decimal128FromInt64(1), types.Decimal128FromInt64(2), types.Decimal128FromInt64(0), types.Decimal128FromInt64(7)}},
		{"decimal256", types.T_decimal256.ToType(), []any{types.Decimal256FromInt64(9), types.Decimal256FromInt64(1), types.Decimal256FromInt64(2), types.Decimal256FromInt64(0), types.Decimal256FromInt64(7)}},
		{"uuid", types.T_uuid.ToType(), []any{types.Uuid{9}, types.Uuid{1}, types.Uuid{2}, types.Uuid{}, types.Uuid{7}}},
	} {
		for _, id := range []int64{AggIdOfCountStar, AggIdOfCountColumn, AggIdOfSum, AggIdOfAvg, AggIdOfMin, AggIdOfMax} {
			switch tc.typ.Oid {
			case types.T_bool, types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_enum, types.T_uuid:
				if id != AggIdOfMin && id != AggIdOfMax {
					continue
				}
			}
			if tc.typ.Oid == types.T_decimal64 && id == AggIdOfAvg {
				continue // independent decimal64 fast executor remains fallback
			}
			if tc.typ.Oid == types.T_varchar && id != AggIdOfCountColumn && id != AggIdOfCountStar {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s", tc.name, map[int64]string{AggIdOfCountStar: "count-star", AggIdOfCountColumn: "count-column", AggIdOfSum: "sum", AggIdOfAvg: "avg", AggIdOfMin: "min", AggIdOfMax: "max"}[id]), func(t *testing.T) {
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
		{"decimal-avg", AggIdOfAvg, false, false, types.T_decimal64.ToType(), nil},
		{"min-bytes", AggIdOfMin, false, false, types.T_varchar.ToType(), nil},
		{"min-extra", AggIdOfMin, false, false, types.T_int32.ToType(), int32(2)},
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
		{"min-new-data", AggIdOfMin, false, 1},
		{"min-new-nulls", AggIdOfMin, true, 2},
		{"min-existing-growth", AggIdOfMin, false, 1},
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
			if tc.name == "existing-growth" || tc.name == "min-existing-growth" {
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
				} else if tc.id == AggIdOfMin {
					require.Equal(t, int32(7), vector.GetAny(dst, oldLen, false))
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

func TestSingleGroupMinMaxFloatSpecialValuesMatchBulkFill(t *testing.T) {
	for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
		for _, values := range [][]float64{
			{1, math.NaN(), -1, math.Inf(1)},
			{math.NaN(), 2, math.Inf(-1), 1},
			{0, math.Copysign(0, -1), 0, math.Copysign(0, -1)},
		} {
			mp := mpool.MustNewZero()
			makeExec := func() GroupAggFuncExec {
				a, err := MakeSingleGroupAgg(mp, id, false, nil, nil, types.T_float64.ToType())
				require.NoError(t, err)
				SyncAggregatorsToChunkSize([]GroupAggFuncExec{a}, 1)
				require.NoError(t, a.GroupGrow(1))
				return a
			}
			fast, old := makeExec(), makeExec()
			input := buildAvgFixedVector(t, mp, types.T_float64.ToType(), values)
			for _, r := range [][2]int{{0, 1}, {1, 3}} {
				require.NoError(t, BulkFillSingleGroupRange(fast, r[0], r[1], []*vector.Vector{input}))
				window, err := input.WindowByLogicalRows(r[0], r[0]+r[1])
				require.NoError(t, err)
				require.NoError(t, old.BulkFill(0, []*vector.Vector{window}))
				window.Free(mp)
			}
			out, err := AppendSingleGroupResultAndReset(fast, nil)
			require.NoError(t, err)
			ref, err := old.Flush()
			require.NoError(t, err)
			require.Equal(t, math.Float64bits(vector.GetFixedAtNoTypeCheck[float64](ref[0], 0)), math.Float64bits(vector.GetFixedAtNoTypeCheck[float64](out, 0)))
			out.Free(mp)
			ref[0].Free(mp)
			fast.Free()
			old.Free()
			input.Free(mp)
			require.Zero(t, mp.CurrNB())
			mpool.DeleteMPool(mp)
		}
	}
}

func TestSingleGroupMinRejectsRichMetadata(t *testing.T) {
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprintf("source=%t", source), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			defer func() { require.Zero(t, mp.CurrNB()) }()
			a, err := MakeSingleGroupAgg(mp, AggIdOfMin, false, nil, nil, types.T_int32.ToType())
			require.NoError(t, err)
			defer a.Free()
			SyncAggregatorsToChunkSize([]GroupAggFuncExec{a}, 1)
			require.NoError(t, a.GroupGrow(1))
			v := buildAvgFixedVector(t, mp, types.T_int32.ToType(), []int32{7})
			defer v.Free(mp)
			if source {
				require.NoError(t, v.SetStringSource(types.StringSourceSQLPrepare))
			} else {
				v.SetPrepareParamKind(vector.PrepareParamFloat)
			}
			require.False(t, SupportsSingleGroupReuseInput(a, []*vector.Vector{v}))
			require.Error(t, BulkFillSingleGroupRange(a, 0, 1, []*vector.Vector{v}))
			require.NoError(t, a.BulkFill(0, []*vector.Vector{v}))
			out, err := a.Flush()
			require.NoError(t, err)
			defer out[0].Free(mp)
			require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](out[0], 0))
			if source {
				require.Equal(t, types.StringSourceSQLPrepare, out[0].GetStringSourceAt(0))
			} else {
				require.Equal(t, vector.PrepareParamFloat, out[0].GetPrepareParamKindAt(0))
			}
		})
	}
}

func TestSingleGroupDecimalAvgRoundingAndRetry(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.New(types.T_int64, 35, 0)} {
		for _, sign := range []int64{1, -1} {
			t.Run(fmt.Sprintf("%v/%d", typ, sign), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				controller := &rejectNthAggregateAllocation{}
				registry, err := mpool.NewAllocationAccountRegistry(1, 512)
				require.NoError(t, err)
				account, err := registry.OpenWithController(128<<20, controller)
				require.NoError(t, err)
				defer func() { finishTestAggregateAllocation(t, registry, account); require.Zero(t, mp.CurrNB()) }()
				allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{VectorData: 1, VectorArea: 2, VectorNulls: 3, VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6})
				require.NoError(t, err)
				exec, err := MakeSingleGroupAgg(mp, AggIdOfAvg, false, allocation, nil, typ)
				require.NoError(t, err)
				defer exec.Free()
				SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
				require.NoError(t, exec.GroupGrow(1))
				values := make([]int64, 113)
				values[1] = sign
				input := buildAvgFixedVector(t, mp, typ, values)
				defer input.Free(mp)
				require.NoError(t, BulkFillSingleGroupRange(exec, 0, len(values), []*vector.Vector{input}))
				controller.failAt = controller.calls + 1
				_, err = AppendSingleGroupResultAndReset(exec, nil)
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				controller.failAt = 0
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

func TestSingleGroupDecimalSumFinalPrecisionAndRetry(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	defer func() { require.Zero(t, mp.CurrNB()) }()
	typ := types.New(types.T_decimal256, 65, 0)
	maxValue, err := types.ParseDecimal256(strings.Repeat("9", 65), 65, 0)
	require.NoError(t, err)
	one := types.Decimal256FromInt64(1)
	input := buildDecimal256Vector(t, mp, typ, nil, []types.Decimal256{maxValue, one, one.Minus()})
	defer input.Free(mp)
	exec, err := MakeSingleGroupAgg(mp, AggIdOfSum, false, nil, nil, typ)
	require.NoError(t, err)
	defer exec.Free()
	SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, BulkFillSingleGroupRange(exec, 0, 1, []*vector.Vector{input}))
	require.NoError(t, BulkFillSingleGroupRange(exec, 1, 1, []*vector.Vector{input}))
	for i := 0; i < 2; i++ {
		result, err := AppendSingleGroupResultAndReset(exec, nil)
		require.Nil(t, result)
		require.ErrorContains(t, err, "beyond the range")
	}
	require.NoError(t, BulkFillSingleGroupRange(exec, 2, 1, []*vector.Vector{input}))
	result, err := AppendSingleGroupResultAndReset(exec, nil)
	require.NoError(t, err)
	defer result.Free(mp)
	require.Equal(t, maxValue, vector.GetFixedAtNoTypeCheck[types.Decimal256](result, 0))
}

func TestSingleGroupDecimalScale(t *testing.T) {
	for _, scale := range []int32{2, 30} {
		for _, id := range []int64{AggIdOfSum, AggIdOfAvg} {
			t.Run(fmt.Sprintf("scale=%d/id=%d", scale, id), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				defer func() { require.Zero(t, mp.CurrNB()) }()
				typ := types.New(types.T_decimal256, 65, scale)
				one, err := types.ParseDecimal256("1."+strings.Repeat("0", int(scale)), 65, scale)
				require.NoError(t, err)
				input := buildDecimal256Vector(t, mp, typ, nil, []types.Decimal256{one, one, one.Minus()})
				defer input.Free(mp)
				exec, err := MakeSingleGroupAgg(mp, id, false, nil, nil, typ)
				require.NoError(t, err)
				defer exec.Free()
				SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
				require.NoError(t, exec.GroupGrow(1))
				require.True(t, SupportsSingleGroupReuse([]GroupAggFuncExec{exec}))
				require.NoError(t, BulkFillSingleGroupRange(exec, 0, 2, []*vector.Vector{input}))
				require.NoError(t, BulkFillSingleGroupRange(exec, 2, 1, []*vector.Vector{input}))
				result, err := AppendSingleGroupResultAndReset(exec, nil)
				require.NoError(t, err)
				defer result.Free(mp)
				want := "1." + strings.Repeat("0", int(result.GetType().Scale))
				if id == AggIdOfAvg {
					want = "0." + strings.Repeat("3", int(result.GetType().Scale))
				}
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal256](result, 0).Format(result.GetType().Scale))
			})
		}
	}
}

func TestSingleGroupDecimalOutputAllocationFailure(t *testing.T) {
	for _, growth := range []bool{false, true} {
		t.Run(fmt.Sprintf("growth=%t", growth), func(t *testing.T) {
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
			exec, err := MakeSingleGroupAgg(mp, AggIdOfSum, false, allocation, nil, types.T_int64.ToType())
			require.NoError(t, err)
			defer exec.Free()
			SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
			require.NoError(t, exec.GroupGrow(1))
			input := buildAvgFixedVector(t, mp, types.T_int64.ToType(), []int64{7})
			defer input.Free(mp)
			var dst *vector.Vector
			defer func() {
				if dst != nil {
					dst.Free(mp)
				}
			}()
			if growth {
				dst, err = AppendSingleGroupResultAndReset(exec, nil)
				require.NoError(t, err)
				for dst.Length() < dst.Capacity() {
					_, err = AppendSingleGroupResultAndReset(exec, dst)
					require.NoError(t, err)
				}
				require.NoError(t, BulkFillSingleGroupRange(exec, 0, 1, []*vector.Vector{input}))
			}
			oldLen := 0
			if dst != nil {
				oldLen = dst.Length()
			}
			before := account.Snapshot().Used
			controller.failAt = controller.calls + 1
			if !growth {
				controller.failAt++
			}
			result, err := AppendSingleGroupResultAndReset(exec, dst)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.True(t, controller.rejected)
			require.Nil(t, result)
			require.Equal(t, before, account.Snapshot().Used)
			if dst != nil {
				require.Equal(t, oldLen, dst.Length())
			}
			controller.failAt = 0
			dst, err = AppendSingleGroupResultAndReset(exec, dst)
			require.NoError(t, err)
			require.Equal(t, !growth, dst.IsNull(uint64(oldLen)))
			if growth {
				require.Equal(t, types.Decimal128FromInt64(7), vector.GetFixedAtNoTypeCheck[types.Decimal128](dst, oldLen))
			}
		})
	}
}

func TestSingleGroupDecimalPhysicalOverflow(t *testing.T) {
	// Deliberately outside SQL's 65-digit input envelope to exercise physical
	// overflow without requiring hundreds of billions of valid maximum values.
	max := types.Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0) >> 1}
	for _, id := range []int64{AggIdOfSum, AggIdOfAvg} {
		for _, split := range []bool{false, true} {
			t.Run(fmt.Sprintf("id=%d/split=%t", id, split), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				defer func() { require.Zero(t, mp.CurrNB()) }()
				typ := types.New(types.T_decimal256, 65, 0)
				input := buildDecimal256Vector(t, mp, typ, nil, []types.Decimal256{max, types.Decimal256FromInt64(1)})
				defer input.Free(mp)
				exec, err := MakeSingleGroupAgg(mp, id, false, nil, nil, typ)
				require.NoError(t, err)
				defer exec.Free()
				SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
				require.NoError(t, exec.GroupGrow(1))
				if split {
					require.NoError(t, BulkFillSingleGroupRange(exec, 0, 1, []*vector.Vector{input}))
					err = BulkFillSingleGroupRange(exec, 1, 1, []*vector.Vector{input})
				} else {
					err = BulkFillSingleGroupRange(exec, 0, 2, []*vector.Vector{input})
				}
				require.ErrorContains(t, err, "overflow")
			})
		}
	}
}

func TestSingleGroupUint64Boundary(t *testing.T) {
	for _, tc := range []struct {
		id   int64
		want string
	}{{AggIdOfSum, "36893488147419103230"}, {AggIdOfAvg, "18446744073709551615.0000"}} {
		t.Run(fmt.Sprint(tc.id), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			defer func() { require.Zero(t, mp.CurrNB()) }()
			typ := types.T_uint64.ToType()
			input := vector.NewVec(typ)
			defer input.Free(mp)
			for i := 0; i < 2; i++ {
				require.NoError(t, vector.AppendAny(input, ^uint64(0), false, mp))
			}
			exec, err := MakeSingleGroupAgg(mp, tc.id, false, nil, nil, typ)
			require.NoError(t, err)
			defer exec.Free()
			SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
			require.NoError(t, exec.GroupGrow(1))
			require.True(t, SupportsSingleGroupReuse([]GroupAggFuncExec{exec}))
			for i := 0; i < 2; i++ {
				require.NoError(t, BulkFillSingleGroupRange(exec, i, 1, []*vector.Vector{input}))
			}
			result, err := AppendSingleGroupResultAndReset(exec, nil)
			require.NoError(t, err)
			defer result.Free(mp)
			require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0).Format(result.GetType().Scale))
		})
	}
}
