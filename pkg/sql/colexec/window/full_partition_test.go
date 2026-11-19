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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

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
	defer ctr.freeFullPartitionResults(proc.Mp())

	first, err := ctr.processAggregateFuncRange(0, arg, proc, 0, colexec.DefaultBatchSize)
	require.NoError(t, err)
	defer first.Free(proc.Mp())
	require.NotNil(t, ctr.fullPartitionResults)
	for i, value := range vector.MustFixedColWithTypeCheck[int64](first) {
		require.Equal(t, int64(firstRows), value, "row %d", i)
	}

	second, err := ctr.processAggregateFuncRange(0, arg, proc, colexec.DefaultBatchSize, len(values))
	require.NoError(t, err)
	defer second.Free(proc.Mp())
	got := vector.MustFixedColWithTypeCheck[int64](second)
	for i := 0; i < 8; i++ {
		require.Equal(t, int64(firstRows), got[i])
		require.False(t, second.IsNull(uint64(i)))
	}
	for i := 8; i < 11; i++ {
		require.True(t, second.IsNull(uint64(i)))
	}
	for i := 11; i < 13; i++ {
		require.Equal(t, int64(2), got[i])
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
	defer ctr.freeFullPartitionResults(proc.Mp())
	want := []int64{2, 2, 2, 2, 6, 6, 6, 6, 6, 6, 2, 2, 1}
	start := 0
	for _, end := range []int{5, 11, 13} {
		func() {
			result, err := ctr.processAggregateFuncRange(0, arg, proc, start, end)
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			require.Equal(t, want[start:end], vector.MustFixedColWithTypeCheck[int64](result))
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
			defer ctr.freeFullPartitionResults(proc.Mp())
			result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 4)
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			for i := 0; i < 4; i++ {
				require.False(t, result.IsNull(uint64(i)))
				switch {
				case tc.wantAvg:
					require.Equal(t, int32(4), result.GetType().Scale)
					require.Equal(t, types.Decimal128FromInt64(16667), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, i))
				case tc.name == "min" || tc.name == "max":
					require.Equal(t, int32(tc.wantInt), vector.GetFixedAtNoTypeCheck[int32](result, i))
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
	bat.Vecs[0] = vector.NewVec(typ)
	for _, v := range []types.Decimal256{types.Decimal256FromInt64(1), types.Decimal256FromInt64(1), maxValue, types.Decimal256FromInt64(1)} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, false, proc.Mp()))
	}
	bat.SetRowCount(4)
	defer bat.Clean(proc.Mp())
	arg := &Window{WinSpecList: []*plan.Expr{makeAggWindowSpec("sum")}, Aggs: []aggexec.AggFuncExecExpression{newTypedSumAggExpr(t, 0, typ)}}
	ctr := &container{bat: bat, ps: []int64{0, 2}, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
	defer ctr.freeFullPartitionResults(proc.Mp())
	first, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 1)
	require.NoError(t, err, "the unrequested overflowing partition must not be evaluated")
	defer first.Free(proc.Mp())
	require.Equal(t, types.Decimal256FromInt64(2), vector.GetFixedAtNoTypeCheck[types.Decimal256](first, 0))
	second, err := ctr.processAggregateFuncRange(0, arg, proc, 1, 2)
	require.NoError(t, err)
	defer second.Free(proc.Mp())
	_, err = ctr.processAggregateFuncRange(0, arg, proc, 2, 4)
	require.ErrorContains(t, err, "beyond the range")
	require.Nil(t, ctr.fullPartitionResults)
	require.Nil(t, ctr.batAggs)
	// A failed generation must not contaminate reuse.
	reused, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 1)
	require.NoError(t, err)
	defer reused.Free(proc.Mp())
}

func TestFullPartitionCancellationReleasesAggregate(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	bat := makeInt32Batch(proc.Mp(), make([]int32, 2048))
	defer bat.Clean(proc.Mp())
	arg := &Window{WinSpecList: []*plan.Expr{makeAggWindowSpec("sum")}, Aggs: []aggexec.AggFuncExecExpression{newAggExpr()}}
	ctr := &container{bat: bat, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
	defer ctr.freeFullPartitionResults(proc.Mp())
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
	defer ctr.freeFullPartitionResults(proc.Mp())
	first, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 3)
	require.NoError(t, err)
	defer first.Free(proc.Mp())
	require.Equal(t, []int64{3, 3, 28}, vector.MustFixedColWithTypeCheck[int64](first))
	require.NotNil(t, ctr.fullPartitionResults)
	second, err := ctr.processAggregateFuncRange(0, arg, proc, 3, 5)
	require.NoError(t, err)
	defer second.Free(proc.Mp())
	require.Equal(t, []int64{28, 28}, vector.MustFixedColWithTypeCheck[int64](second))
	require.Nil(t, ctr.fullPartitionResults)
}
