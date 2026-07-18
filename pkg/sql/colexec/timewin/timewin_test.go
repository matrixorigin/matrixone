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

package timewin

import (
	"bytes"
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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// add unit tests for cases
type timeWinTestCase struct {
	arg  *TimeWin
	proc *process.Process
}

func makeTestCases(t *testing.T) []timeWinTestCase {
	return []timeWinTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &TimeWin{
				WStart: true,
				WEnd:   true,
				Types: []types.Type{
					types.T_int32.ToType(),
				},
				Aggs: []aggexec.AggFuncExecExpression{
					aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
				},
				TsType:   plan.Type{Id: int32(types.T_datetime)},
				Ts:       newExpression(0),
				EndExpr:  newExpression(0),
				Interval: makeInterval(),
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &TimeWin{
				WStart: true,
				WEnd:   false,
				Types: []types.Type{
					types.T_int32.ToType(),
				},
				Aggs: []aggexec.AggFuncExecExpression{
					aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
				},
				TsType:   plan.Type{Id: int32(types.T_datetime)},
				Ts:       newExpression(0),
				EndExpr:  newExpression(0),
				Interval: makeInterval(),
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &TimeWin{
				WStart: false,
				WEnd:   false,
				Types: []types.Type{
					types.T_int32.ToType(),
				},
				Aggs: []aggexec.AggFuncExecExpression{
					aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
				},
				TsType:   plan.Type{Id: int32(types.T_datetime)},
				Ts:       newExpression(0),
				EndExpr:  newExpression(0),
				Interval: makeInterval(),
			},
		},
	}
}

func makePrepareErrorCases(t *testing.T) []timeWinTestCase {
	return []timeWinTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &TimeWin{
				WStart: true,
				WEnd:   true,
				Types: []types.Type{
					types.T_int32.ToType(),
				},
				Aggs: []aggexec.AggFuncExecExpression{
					aggexec.MakeAggFunctionExpression(
						-9999,
						false,
						[]*plan.Expr{newExpression(1)},
						nil,
					),
				},
				TsType:   plan.Type{Id: int32(types.T_datetime)},
				Ts:       newExpression(0),
				EndExpr:  newExpression(0),
				Interval: makeInterval(),
			},
		},
	}
}

func TestPrepareError(t *testing.T) {
	for _, tc := range makePrepareErrorCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.Error(t, err)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestTimeWin(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

// TestTimeWinSplitDistinctResultAndReplace verifies the complete non-final
// flush transition: split physical results are materialized as one logical
// batch, and the flushed DISTINCT executor is freed before its replacement is
// installed.
func TestTimeWinSplitDistinctResultAndReplace(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := aggexec.AggBatchSize + 17
	values := make([]int32, rows)
	groups := make([]uint64, rows)
	for i := range values {
		values[i] = int32(i + 1)
		groups[i] = uint64(i + 1)
	}
	input := testutil.MakeInt32Vector(values, nil, proc.Mp())

	agg, err := aggexec.MakeAgg(proc.Mp(), function.AggSumOverloadID, true, types.T_int32.ToType())
	require.NoError(t, err)
	require.NoError(t, agg.GroupGrow(rows))
	require.NoError(t, agg.BatchFill(0, groups, []*vector.Vector{input}))

	arg := &TimeWin{
		Types: []types.Type{types.T_int32.ToType()},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				function.AggSumOverloadID, true, []*plan.Expr{newExpression(0)}, nil),
		},
	}
	arg.ctr.status = flush
	arg.ctr.colCnt = 1
	arg.ctr.aggs = []aggexec.AggFuncExec{agg}
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	resultValues := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])
	require.Len(t, resultValues, rows)
	for _, idx := range []int{0, aggexec.AggBatchSize - 1, aggexec.AggBatchSize, rows - 1} {
		require.Equal(t, int64(values[idx]), resultValues[idx], "row %d", idx)
	}

	require.Equal(t, int32(nextWindow), arg.ctr.status)
	require.Len(t, arg.ctr.aggs, 1)
	require.NotSame(t, agg, arg.ctr.aggs[0])

	// A second intermediate flush verifies both generations: the first output
	// batch is released on the next Call, and the first replacement executor is
	// released when the second replacement is installed.
	require.NoError(t, arg.ctr.aggs[0].Fill(0, 0, []*vector.Vector{input}))
	arg.ctr.status = flush
	secondResult, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, secondResult.Batch)
	require.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](secondResult.Batch.Vecs[0]))

	arg.Free(proc, false, nil)
	input.Free(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestTimeWinReplacementFailurePreservesOwnership(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())

	agg, err := aggexec.MakeAgg(proc.Mp(), function.AggSumOverloadID, true, types.T_int32.ToType())
	require.NoError(t, err)
	require.NoError(t, agg.GroupGrow(1))
	require.NoError(t, agg.Fill(0, 0, []*vector.Vector{input}))

	arg := &TimeWin{
		Types: []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				function.AggSumOverloadID, true, []*plan.Expr{newExpression(0)}, nil),
			aggexec.MakeAggFunctionExpression(-9999, false, []*plan.Expr{newExpression(0)}, nil),
		},
	}
	arg.ctr.status = flush
	arg.ctr.colCnt = 1
	arg.ctr.aggs = []aggexec.AggFuncExec{agg}

	_, err = arg.Call(proc)
	require.Error(t, err)
	require.Len(t, arg.ctr.aggs, 1)
	require.Same(t, agg, arg.ctr.aggs[0], "failed replacement must not overwrite the owned executor")

	arg.Free(proc, true, err)
	input.Free(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func resetChildren(arg *TimeWin, m *mpool.MPool) {
	bat := colexec.MakeMockTimeWinBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func makeInterval() types.Datetime {
	t, _ := calcDatetime(5, 2)
	return t
}

// singleAggInfo is the basic information of single column agg.
type singleAggInfo struct {
	aggID    int64
	distinct bool
	argType  types.Type
	retType  types.Type

	// emptyNull indicates that whether we should return null for a group without any input value.
	emptyNull bool
}

func TestAvgTwCache(t *testing.T) {
	mg := mpool.MustNewZeroNoFixed()

	info := singleAggInfo{
		aggID:     function.AggAvgTwCacheOverloadID,
		distinct:  false,
		argType:   types.T_int32.ToType(),
		retType:   types.T_char.ToType(),
		emptyNull: false,
	}
	//registerTheTestingCount(info.aggID, info.emptyNull)
	executor, err := aggexec.MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)
	require.Nil(t, err)

	inputType := info.argType
	inputs := make([]*vector.Vector, 5)
	{
		// prepare the input data.
		var err error

		vec := vector.NewVec(inputType)
		require.NoError(t, vector.AppendFixedList[int32](vec, []int32{3, 0, 4, 5}, []bool{false, true, false, false}, mg))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vector.NewConstNull(inputType, 2, mg)
		inputs[3], err = vector.NewConstFixed[int32](inputType, 1, 3, mg)
		require.NoError(t, err)
		inputs[4] = vector.NewVec(inputType)
		require.NoError(t, vector.AppendFixedList[int32](inputs[4], []int32{1, 2, 3, 4}, nil, mg))
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.BatchFill(1, []uint64{1}, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[2]}))
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[3]}))
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[4]}))
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
		}
		v[0].Free(mg)
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg)
		}
		require.Equal(t, int64(0), mg.CurrNB())
	}
}

func TestAvgTwCacheDecimal64(t *testing.T) {
	mg := mpool.MustNewZeroNoFixed()

	info := singleAggInfo{
		aggID:     function.AggAvgTwCacheOverloadID,
		distinct:  false,
		argType:   types.T_decimal64.ToType(),
		retType:   types.T_varchar.ToType(),
		emptyNull: false,
	}
	//registerTheTestingCount(info.aggID, info.emptyNull)
	executor, err := aggexec.MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)
	require.Nil(t, err)

	inputs := make([]*vector.Vector, 3)
	{
		vs := make([]types.Decimal64, 4)
		vec := vector.NewVec(types.T_decimal64.ToType())
		require.NoError(t, vector.AppendFixedList(vec, vs, nil, mg))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vec
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.BatchFill(1, []uint64{1}, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[2]}))
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
		}
		v[0].Free(mg)
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg)
		}
		require.Equal(t, int64(0), mg.CurrNB())
	}
}

func TestAvgTwCacheDecimal128(t *testing.T) {
	mg := mpool.MustNewZeroNoFixed()

	info := singleAggInfo{
		aggID:     function.AggAvgTwCacheOverloadID,
		distinct:  false,
		argType:   types.T_decimal128.ToType(),
		retType:   types.T_varchar.ToType(),
		emptyNull: false,
	}
	//registerTheTestingCount(info.aggID, info.emptyNull)
	executor, err := aggexec.MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)
	require.Nil(t, err)

	inputs := make([]*vector.Vector, 3)
	{
		vs := make([]types.Decimal128, 4)
		vec := vector.NewVec(types.T_decimal128.ToType())
		require.NoError(t, vector.AppendFixedList(vec, vs, nil, mg))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vec
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.BatchFill(1, []uint64{1}, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[2]}))
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
		}
		v[0].Free(mg)
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg)
		}
		require.Equal(t, int64(0), mg.CurrNB())
	}
}

func TestAvgTwResult(t *testing.T) {
	mg := mpool.MustNewZeroNoFixed()

	info := singleAggInfo{
		aggID:     function.AggAvgTwResultOverloadID,
		distinct:  false,
		argType:   types.T_char.ToType(),
		retType:   types.T_float64.ToType(),
		emptyNull: false,
	}
	//registerTheTestingCount(info.aggID, info.emptyNull)
	executor, err := aggexec.MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)
	require.Nil(t, err)

	inputType := info.argType
	inputs := make([]*vector.Vector, 5)
	{
		// prepare the input data.
		var err error

		vec := vector.NewVec(inputType)
		require.NoError(t, vector.AppendStringList(vec, []string{"sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf"}, []bool{false, true, false, false}, mg))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vector.NewConstNull(inputType, 2, mg)
		inputs[3], err = vector.NewConstBytes(inputType, []byte("sdfasdfsadfasdfadf"), 3, mg)
		require.NoError(t, err)
		inputs[4] = vector.NewVec(inputType)
		require.NoError(t, vector.AppendStringList(inputs[4], []string{"sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf"}, nil, mg))
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.BatchFill(1, []uint64{1}, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[2]}))
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[3]}))
		require.NoError(t, executor.BatchFill(0, []uint64{1}, []*vector.Vector{inputs[4]}))
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
		}
		v[0].Free(mg)
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg)
		}
		require.Equal(t, int64(0), mg.CurrNB())
	}
}
