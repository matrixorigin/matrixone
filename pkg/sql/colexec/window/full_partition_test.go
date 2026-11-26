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

package window

import (
	"context"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// Isolate this test with -covermode=count to measure actual Fill calls in
// processAggregateFuncRange and evaluateFullPartitions on the same OVER() plan.
func TestBitmapOrWindowFillWork(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	defer func() { proc.Free(); require.Zero(t, mp.CurrNB()) }()
	child := colexec.NewMockOperator()
	defer child.Free(proc, false, nil)
	const rows = 1600
	typ := types.T_varbinary.ToType()
	input := batch.NewWithSize(1)
	child.WithBatchs([]*batch.Batch{input})
	input.Vecs[0] = vector.NewVec(typ, mp)
	for row := range rows {
		data, err := roaring.BitmapOf(uint32(row % 8)).ToBytes()
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(input.Vecs[0], data, false, mp))
	}
	input.SetRowCount(rows)
	spec := makeAggWindowSpec("bitmap_or_agg")
	spec.GetW().Frame.Type = plan.FrameClause_RANGE
	arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfBitmapOr, false, []*plan.Expr{newColExprWithType(0, typ)}, nil),
	}}
	arg.AppendChild(child)
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	want := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
	seen := 0
	for {
		result, err := arg.Call(proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			require.Nil(t, result.Batch)
			break
		}
		require.NotNil(t, result.Batch)
		require.Positive(t, result.Batch.RowCount())
		out := result.Batch.Vecs[1]
		for row := range result.Batch.RowCount() {
			checkBitmapWindowRow(t, out, row, want)
		}
		seen += result.Batch.RowCount()
		require.LessOrEqual(t, seen, rows)
	}
	require.Equal(t, rows, seen)
}

func TestBitmapConstructWindowBudget(t *testing.T) {
	const rows, budget = 1600, 1 << 20
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	t.Cleanup(func() {
		proc.Free()
		t.Logf("after child/process release: pool bytes=%d", mp.CurrNB())
		require.Zero(t, mp.CurrNB())
	})
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	require.NoError(t, err)
	account, err := registry.Open(budget)
	require.NoError(t, err)
	t.Cleanup(func() {
		terminal, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		t.Logf("after terminal completion: used=%d live allocations=%d", terminal.Used, terminal.LiveAllocations)
		require.Zero(t, terminal.Used)
		require.Zero(t, terminal.LiveAllocations)
	})

	input := batch.NewWithSize(1)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	t.Cleanup(func() { child.Free(proc, false, nil) })
	typ := types.T_uint64.ToType()
	input.Vecs[0] = vector.NewVec(typ, mp)
	want := make([]uint32, rows)
	for row := range rows {
		want[row] = uint32(row)
		require.NoError(t, vector.AppendFixed(input.Vecs[0], uint64(row), false, mp))
	}
	input.SetRowCount(rows)
	spec := makeAggWindowSpec("bitmap_construct_agg")
	// RANGE UNBOUNDED PRECEDING TO CURRENT ROW without ordering is OVER().
	spec.GetW().Frame.Type = plan.FrameClause_RANGE
	spec.GetW().Frame.End = &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}
	arg := newBitmapConstructWindow(spec, typ)
	arg.AppendChild(child)
	var callErr error
	t.Cleanup(func() {
		arg.Reset(proc, callErr != nil, callErr)
		t.Logf("after Window.Reset: used=%d", account.Snapshot().Used)
		require.Zero(t, account.Snapshot().Used)
	})
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.Prepare(proc))

	seen := 0
	for {
		result, err := arg.Call(proc)
		callErr = err
		if err != nil {
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Nil(t, result.Batch)
			require.Zero(t, seen)
			t.Fatalf("Window.Call rejected the %d-byte budget with nil batch and %d output rows: %v", budget, seen, err)
		}
		if result.Status == vm.ExecStop {
			require.Nil(t, result.Batch)
			break
		}
		require.NotNil(t, result.Batch)
		count := result.Batch.RowCount()
		require.Positive(t, count)
		require.LessOrEqual(t, seen+count, rows)
		for row := range count {
			checkBitmapWindowRow(t, result.Batch.Vecs[1], row, want)
		}
		seen += count
	}
	require.Equal(t, rows, seen)
}

func TestBitmapConstructWindowFrames(t *testing.T) {
	for _, tc := range []struct {
		name string
		full bool
		want [][]uint32
	}{
		{name: "whole partitions", full: true, want: [][]uint32{{7, 9}, {7, 9}, {7, 9}, {7, 9}, {7, 9}, nil, nil}},
		{name: "finite sliding", want: [][]uint32{{7}, {7}, {9}, {9}, nil, nil, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			t.Cleanup(func() { proc.Free(); require.Zero(t, mp.CurrNB()) })
			typ := types.T_uint64.ToType()
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(mp) })
			input.Vecs[0] = vector.NewVec(typ, mp)
			// In physical ROWS order: [7, NULL, 9, NULL, NULL] | [NULL, NULL].
			for _, value := range []uint64{7, 0, 9, 0, 0, 0, 0} {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], value, value == 0, mp))
			}
			input.SetRowCount(7)
			spec := makeAggWindowSpec("bitmap_construct_agg")
			if !tc.full {
				spec.GetW().Frame = &plan.FrameClause{
					Type:  plan.FrameClause_ROWS,
					Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}}}},
					End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
				}
			}
			arg := newBitmapConstructWindow(spec, typ)
			ctr := &container{
				bat: input, ps: []int64{0, 5},
				aggVecs: []colexec.ExprEvalVector{{Vec: input.Vecs}},
			}
			t.Cleanup(func() { ctr.freeFullPartitionResults(proc) })
			check := func(start, end int) {
				t.Helper()
				out, err := ctr.processAggregateFuncRange(0, arg, proc, start, end)
				require.NoError(t, err)
				defer out.Free(out.MPool())
				require.Equal(t, end-start, out.Length())
				for row, want := range tc.want[start:end] {
					checkBitmapWindowRow(t, out, row, want)
				}
			}
			check(0, 2) // The first partition is still partially emitted.
			if tc.full {
				require.NotNil(t, ctr.fullPartitionResults)
			} else {
				require.Nil(t, ctr.fullPartitionResults)
			}
			check(2, len(tc.want))
			require.Nil(t, ctr.fullPartitionResults)
		})
	}
}

func newBitmapConstructWindow(spec *plan.Expr, typ types.Type) *Window {
	return &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfBitmapConstruct, false, []*plan.Expr{newColExprWithType(0, typ)}, nil),
	}}
}

func TestFullPartitionSumAcrossChunksAndNullPartitions(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	const firstRows = colexec.DefaultBatchSize + 8
	values := make([]int32, firstRows+5)
	for i := 0; i < firstRows; i++ {
		values[i] = 1
	}
	values[firstRows+3], values[firstRows+4] = -3, 5
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, []uint64{firstRows, firstRows + 1, firstRows + 2}, proc.Mp())
	bat.SetRowCount(len(values))
	defer bat.Clean(proc.Mp())
	arg := &Window{WinSpecList: []*plan.Expr{makeAggWindowSpec("sum")}, Aggs: []aggexec.AggFuncExecExpression{newAggExpr()}}
	ctr := &container{
		bat: bat, ps: []int64{0, firstRows, firstRows + 3},
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}
	defer ctr.freeFullPartitionResults(proc)

	first, err := ctr.processAggregateFuncRange(0, arg, proc, 0, colexec.DefaultBatchSize)
	require.NoError(t, err)
	defer first.Free(first.MPool())
	require.NotNil(t, ctr.fullPartitionResults)
	for i, value := range vector.MustFixedColWithTypeCheck[types.Decimal128](first) {
		require.Equal(t, types.Decimal128FromInt64(firstRows), value, "row %d", i)
	}

	second, err := ctr.processAggregateFuncRange(0, arg, proc, colexec.DefaultBatchSize, len(values))
	require.NoError(t, err)
	defer second.Free(second.MPool())
	got := vector.MustFixedColWithTypeCheck[types.Decimal128](second)
	for i := 0; i < 8; i++ {
		require.Equal(t, types.Decimal128FromInt64(firstRows), got[i])
		require.False(t, second.IsNull(uint64(i)))
	}
	for i := 8; i < 11; i++ {
		require.True(t, second.IsNull(uint64(i)))
	}
	for i := 11; i < 13; i++ {
		require.Equal(t, types.Decimal128FromInt64(2), got[i])
		require.False(t, second.IsNull(uint64(i)))
	}
	require.Nil(t, ctr.fullPartitionResults)
}

func TestFullPartitionResultsAdvanceAcrossOutputChunks(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	values := make([]int32, 13)
	for i := range values {
		values[i] = 1
	}
	bat := makeInt32Batch(proc.Mp(), values)
	defer bat.Clean(proc.Mp())
	arg := &Window{WinSpecList: []*plan.Expr{makeAggWindowSpec("sum")}, Aggs: []aggexec.AggFuncExecExpression{newAggExpr()}}
	ctr := &container{
		bat: bat, ps: []int64{0, 2, 4, 10, 12},
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}
	defer ctr.freeFullPartitionResults(proc)
	want := []int64{2, 2, 2, 2, 6, 6, 6, 6, 6, 6, 2, 2, 1}
	start := 0
	for _, end := range []int{5, 11, 13} {
		func() {
			result, err := ctr.processAggregateFuncRange(0, arg, proc, start, end)
			require.NoError(t, err)
			defer result.Free(result.MPool())
			values := vector.MustFixedColWithTypeCheck[types.Decimal128](result)
			for i, value := range values {
				require.Equal(t, types.Decimal128FromInt64(want[start+i]), value)
			}
		}()
		start = end
	}
	require.Nil(t, ctr.fullPartitionResults)
}

func TestFullPartitionCommonAggregates(t *testing.T) {
	for _, tc := range []struct {
		name     string
		distinct bool
		wantInt  int64
		wantAvg  bool
	}{
		{name: "sum", wantInt: 5},
		{name: "sum", distinct: true, wantInt: 4},
		{name: "avg", wantAvg: true},
		{name: "count", wantInt: 3},
		{name: "starcount", wantInt: 4},
		{name: "min", wantInt: 1},
		{name: "max", wantInt: 3},
	} {
		t.Run(tc.name+map[bool]string{true: "_distinct"}[tc.distinct], func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 0, 3, 1}, []uint64{1}, proc.Mp())
			bat.SetRowCount(4)
			defer bat.Clean(proc.Mp())
			typ := types.T_int32.ToType()
			e, err := function.GetFunctionByName(context.Background(), tc.name, []types.Type{typ})
			require.NoError(t, err)
			arg := &Window{
				WinSpecList: []*plan.Expr{makeAggWindowSpec(tc.name)},
				Aggs: []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
					e.GetEncodedOverloadID(), tc.distinct, []*plan.Expr{newColExprWithType(0, typ)}, nil)},
			}
			ctr := &container{bat: bat, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
			defer ctr.freeFullPartitionResults(proc)
			result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 4)
			require.NoError(t, err)
			defer result.Free(result.MPool())
			for i := 0; i < 4; i++ {
				require.False(t, result.IsNull(uint64(i)))
				switch {
				case tc.wantAvg:
					require.Equal(t, int32(4), result.GetType().Scale)
					require.Equal(t, types.Decimal128FromInt64(16667), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, i))
				case tc.name == "min" || tc.name == "max":
					require.Equal(t, int32(tc.wantInt), vector.GetFixedAtNoTypeCheck[int32](result, i))
				case tc.name == "sum":
					require.Equal(t, types.Decimal128FromInt64(tc.wantInt), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, i))
				default:
					require.Equal(t, tc.wantInt, vector.GetFixedAtNoTypeCheck[int64](result, i))
				}
			}
		})
	}
}

func TestFullPartitionDoesNotEvaluateLaterPartitionEarly(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	typ := types.New(types.T_decimal256, 65, 0)
	maxValue, err := types.ParseDecimal256(strings.Repeat("9", 65), 65, 0)
	require.NoError(t, err)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(typ, proc.Mp())
	for _, v := range []types.Decimal256{types.Decimal256FromInt64(1), types.Decimal256FromInt64(1), maxValue, types.Decimal256FromInt64(1)} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, false, proc.Mp()))
	}
	bat.SetRowCount(4)
	defer bat.Clean(proc.Mp())
	arg := &Window{WinSpecList: []*plan.Expr{makeAggWindowSpec("sum")}, Aggs: []aggexec.AggFuncExecExpression{newTypedSumAggExpr(t, 0, typ)}}
	ctr := &container{bat: bat, ps: []int64{0, 2}, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
	defer ctr.freeFullPartitionResults(proc)
	first, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 1)
	require.NoError(t, err, "the unrequested overflowing partition must not be evaluated")
	defer first.Free(first.MPool())
	require.Equal(t, types.Decimal256FromInt64(2), vector.GetFixedAtNoTypeCheck[types.Decimal256](first, 0))
	second, err := ctr.processAggregateFuncRange(0, arg, proc, 1, 2)
	require.NoError(t, err)
	defer second.Free(second.MPool())
	_, err = ctr.processAggregateFuncRange(0, arg, proc, 2, 4)
	require.ErrorContains(t, err, "beyond the range")
	require.Nil(t, ctr.fullPartitionResults)
	require.Nil(t, ctr.batAggs)
	// A failed generation must not contaminate reuse.
	reused, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 1)
	require.NoError(t, err)
	defer reused.Free(reused.MPool())
}

func TestFullPartitionCancellationReleasesAggregate(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	bat := makeInt32Batch(proc.Mp(), make([]int32, 2048))
	defer bat.Clean(proc.Mp())
	arg := &Window{WinSpecList: []*plan.Expr{makeAggWindowSpec("sum")}, Aggs: []aggexec.AggFuncExecExpression{newAggExpr()}}
	ctr := &container{bat: bat, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
	defer ctr.freeFullPartitionResults(proc)
	proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 2)
	_, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ctr.fullPartitionResults)
	require.Nil(t, ctr.batAggs)
}

func TestUnorderedRangeFullPartitionAcrossMultiplePartitions(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	bat := makeInt32Batch(proc.Mp(), []int32{1, 2, 4, 8, 16})
	defer bat.Clean(proc.Mp())
	spec := makeAggWindowSpec("sum")
	spec.GetW().Frame = &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{newAggExpr()}}
	ctr := &container{bat: bat, ps: []int64{0, 2}, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
	defer ctr.freeFullPartitionResults(proc)
	first, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 3)
	require.NoError(t, err)
	defer first.Free(first.MPool())
	require.Equal(t, []types.Decimal128{types.Decimal128FromInt64(3), types.Decimal128FromInt64(3), types.Decimal128FromInt64(28)}, vector.MustFixedColWithTypeCheck[types.Decimal128](first))
	require.NotNil(t, ctr.fullPartitionResults)
	second, err := ctr.processAggregateFuncRange(0, arg, proc, 3, 5)
	require.NoError(t, err)
	defer second.Free(second.MPool())
	require.Equal(t, []types.Decimal128{types.Decimal128FromInt64(28), types.Decimal128FromInt64(28)}, vector.MustFixedColWithTypeCheck[types.Decimal128](second))
	require.Nil(t, ctr.fullPartitionResults)
}
