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

func TestIntervalResultPreservesAccountedInputVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(
		account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	value := vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, value.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(value, int64(42), false, mp))
	ctr := container{
		colCnt: 1,
		i:      1,
		aggVec: [][][]*vector.Vector{{{value}}},
	}
	proc := testutil.NewProcessWithMPool(t, "", mp)
	require.NoError(t, ctr.calResForInterval(&TimeWin{}, proc))
	require.Same(t, value, ctr.bat.Vecs[0])
	require.Equal(t, []int64{42}, vector.MustFixedColWithTypeCheck[int64](ctr.bat.Vecs[0]))

	// The interval buffer, not the forwarding batch, owns the vector.
	ctr.bat = nil
	value.Free(mp)
	snapshot := account.Seal()
	require.Zero(t, snapshot.Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.Zero(t, mp.CurrNB())
}

func TestTimeWinResetReleasesInheritedAccountedInput(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(
		account, mpool.AllocationOwnerOrder, 1, 2, 3, 4)
	require.NoError(t, err)

	ts := vector.NewOffHeapVecWithType(types.T_datetime.ToType())
	require.NoError(t, ts.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(ts, types.Datetime(1), false, mp))
	value := vector.NewOffHeapVecWithType(types.T_int32.ToType())
	require.NoError(t, value.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(value, int32(42), false, mp))
	input := batch.NewOffHeapWithSize(2)
	input.SetVector(0, ts)
	input.SetVector(1, value)
	input.SetRowCount(1)

	proc := testutil.NewProcessWithMPool(t, "", mp)
	arg := &TimeWin{
		Types:    []types.Type{types.T_int32.ToType()},
		Aggs:     []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil)},
		TsType:   plan.Type{Id: int32(types.T_datetime)},
		Ts:       newExpression(0),
		EndExpr:  newExpression(0),
		Interval: makeInterval(),
	}
	require.NoError(t, arg.Prepare(proc))
	ok, err := arg.ctr.evalVector(input, proc)
	require.NoError(t, err)
	require.True(t, ok)

	// Pipeline cleanup resets children before parents. Once the input owner is
	// gone, only TimeWin's inherited duplicate capacity remains in the attempt.
	input.Clean(mp)
	require.Positive(t, account.Snapshot().Used)
	arg.Reset(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)

	require.NoError(t, arg.Prepare(proc))
	arg.Free(proc, false, nil)
	snapshot := account.Seal()
	require.Zero(t, snapshot.Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	proc.Free()
	require.Zero(t, mp.CurrNB())
}

func TestEvalVectorSkipsNullTimeRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &TimeWin{
		Types: []types.Type{types.T_int32.ToType()},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
		},
		TsType:   plan.Type{Id: int32(types.T_datetime)},
		Ts:       newExpression(0),
		EndExpr:  newExpression(0),
		Interval: makeInterval(),
	}
	require.NoError(t, arg.Prepare(proc))

	bat := batch.New([]string{"ts", "v"})
	bat.Vecs = []*vector.Vector{
		testutil.MakeDatetimeVector([]string{
			"2026-01-01 00:01:00",
			"2026-01-01 00:02:00",
			"2026-01-01 00:03:00",
		}, []uint64{1}, proc.Mp()),
		testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp()),
	}
	bat.SetRowCount(3)

	ok, err := arg.ctr.evalVector(bat, proc)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, 2, arg.ctr.tsVec[0].Length())
	require.Equal(t, []int32{10, 30}, vector.MustFixedColWithTypeCheck[int32](arg.ctr.aggVec[0][0][0]))

	bat.Clean(proc.Mp())
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestTimeWinCallSkipsAllNullTimeBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &TimeWin{
		WStart: true,
		WEnd:   true,
		Types:  []types.Type{types.T_int32.ToType()},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
		},
		TsType:   plan.Type{Id: int32(types.T_datetime)},
		Ts:       newExpression(0),
		EndExpr:  nil,
		Interval: types.Datetime(types.SecsPerMinute * types.MicroSecsPerSec),
		Sliding:  types.Datetime(types.SecsPerMinute * types.MicroSecsPerSec),
	}

	nullBatch := batch.New([]string{"ts", "v"})
	nullBatch.Vecs = []*vector.Vector{
		testutil.MakeDatetimeVector([]string{
			"2026-01-01 00:00:00",
			"2026-01-01 00:01:00",
		}, []uint64{0, 1}, proc.Mp()),
		testutil.MakeInt32Vector([]int32{999, 999}, nil, proc.Mp()),
	}
	nullBatch.SetRowCount(2)

	validBatch := batch.New([]string{"ts", "v"})
	validBatch.Vecs = []*vector.Vector{
		testutil.MakeDatetimeVector([]string{
			"2026-01-01 00:01:00",
			"2026-01-01 00:02:00",
		}, nil, proc.Mp()),
		testutil.MakeInt32Vector([]int32{10, 20}, nil, proc.Mp()),
	}
	validBatch.SetRowCount(2)

	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{nullBatch, validBatch})
	arg.AppendChild(op)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{10, 20}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0]))
	first, err := types.ParseDatetime("2026-01-01 00:01:00", 6)
	require.NoError(t, err)
	second, err := types.ParseDatetime("2026-01-01 00:02:00", 6)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{
		first,
		second,
	}, vector.MustFixedColWithTypeCheck[types.Datetime](result.Batch.Vecs[1]))

	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestTimeWinApproxPercentileEndpointConfigs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config string
		want   float64
	}{
		{name: "lower endpoint", config: "0", want: 1},
		{name: "upper endpoint", config: "1", want: 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			input := testutil.MakeInt32Vector([]int32{1, 4, 5, 1000}, nil, proc.Mp())
			arg := &TimeWin{
				Types: []types.Type{types.T_int32.ToType()},
				Aggs: []aggexec.AggFuncExecExpression{
					aggexec.MakeAggFunctionExpression(
						aggexec.AggIdOfApproxPercentile,
						false,
						[]*plan.Expr{newExpression(1)},
						[]byte(tc.config)),
				},
			}

			aggs, err := makeAggExecutors(arg, proc, false)
			require.NoError(t, err)
			require.Len(t, aggs, 1)
			require.NoError(t, aggs[0].GroupGrow(1))
			require.NoError(t, aggs[0].BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{input}))
			results, err := aggs[0].Flush()
			require.NoError(t, err)
			require.Equal(t, []float64{tc.want}, vector.MustFixedColWithTypeCheck[float64](results[0]))

			results[0].Free(proc.Mp())
			aggs[0].Free()
			input.Free(proc.Mp())
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestTimeWinApproxPercentileRejectsInvalidExecutorConfig(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &TimeWin{
		Types: []types.Type{types.T_int32.ToType()},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfApproxPercentile,
				false,
				[]*plan.Expr{newExpression(1)},
				[]byte("1.01")),
		},
	}

	_, err := makeAggExecutors(arg, proc, false)
	require.ErrorContains(t, err, "percentile must be in [0,1]")
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
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
	arg.ctr.wStart = make([]types.Datetime, rows)
	arg.ctr.wEnd = make([]types.Datetime, rows)
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Empty(t, arg.ctr.wStart)
	require.Empty(t, arg.ctr.wEnd)
	resultValues := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])
	require.Len(t, resultValues, rows)
	for _, idx := range []int{0, aggexec.AggBatchSize - 1, aggexec.AggBatchSize, rows - 1} {
		require.Equal(t, int64(values[idx]), resultValues[idx], "row %d", idx)
	}

	require.Equal(t, int32(resumeAfterFlush), arg.ctr.status)
	require.Len(t, arg.ctr.aggs, 1)
	require.NotSame(t, agg, arg.ctr.aggs[0])

	// A second intermediate flush verifies both generations: the first output
	// batch is released on the next Call, and the first replacement executor is
	// released when the second replacement is installed.
	require.NoError(t, arg.ctr.aggs[0].Fill(0, 0, []*vector.Vector{input}))
	arg.ctr.status = flush
	arg.ctr.wStart = make([]types.Datetime, maxTimeWindowRows+1)
	arg.ctr.wEnd = make([]types.Datetime, maxTimeWindowRows+1)
	secondResult, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, secondResult.Batch)
	require.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](secondResult.Batch.Vecs[0]))
	require.Empty(t, arg.ctr.wStart)
	require.Empty(t, arg.ctr.wEnd)

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

func TestFirstWindowKeepsZeroDatetimeDistinctFromEpoch(t *testing.T) {
	proc := testutil.NewProcess(t)
	ts := vector.NewVec(types.T_datetime.ToType())
	require.NoError(t, vector.AppendFixedList(ts, []types.Datetime{types.ZeroDatetime}, nil, proc.Mp()))
	ts.SetLength(1)
	defer ts.Free(proc.Mp())

	window := &TimeWin{Interval: types.Datetime(types.MicroSecsPerSec), Sliding: types.Datetime(types.MicroSecsPerSec)}
	ctr := container{tsVec: []*vector.Vector{ts}}
	require.NoError(t, ctr.firstWindow(window))

	require.Equal(t, types.ZeroDatetime, ctr.left)
	require.Equal(t, types.ZeroDatetime, ctr.right)
	require.Equal(t, types.ZeroDatetime, ctr.nextLeft)
	require.Equal(t, types.ZeroDatetime, ctr.nextRight)
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
