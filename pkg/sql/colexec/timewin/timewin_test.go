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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

// add unit tests for cases
type timeWinTestCase struct {
	arg  *TimeWin
	proc *process.Process
}

var (
	tcs []timeWinTestCase
)

func init() {
	tcs = []timeWinTestCase{
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
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
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
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
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
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

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestTimeWin(t *testing.T) {
	for _, tc := range tcs {
		resetChildren(tc.arg)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = tc.arg.Call(tc.proc)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = tc.arg.Call(tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *TimeWin) {
	bat := colexec.MakeMockTimeWinBatchs()
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

type testAggMemoryManager struct {
	mp *mpool.MPool
}

func (m *testAggMemoryManager) Mp() *mpool.MPool {
	return m.mp
}
func newTestAggMemoryManager() aggexec.AggMemoryManager {
	return &testAggMemoryManager{mp: mpool.MustNewNoFixed("test_agg_exec")}
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
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     function.AggAvgTwCacheOverloadID,
		distinct:  false,
		argType:   types.T_int32.ToType(),
		retType:   types.T_char.ToType(),
		emptyNull: false,
	}
	//registerTheTestingCount(info.aggID, info.emptyNull)
	executor := aggexec.MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)

	inputType := info.argType
	inputs := make([]*vector.Vector, 5)
	{
		// prepare the input data.
		var err error

		vec := vector.NewVec(inputType)
		require.NoError(t, vector.AppendFixedList[int32](vec, []int32{3, 0, 4, 5}, []bool{false, true, false, false}, mg.Mp()))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vector.NewConstNull(inputType, 2, mg.Mp())
		inputs[3], err = vector.NewConstFixed[int32](inputType, 1, 3, mg.Mp())
		require.NoError(t, err)
		inputs[4] = vector.NewVec(inputType)
		require.NoError(t, vector.AppendFixedList[int32](inputs[4], []int32{1, 2, 3, 4}, nil, mg.Mp()))
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.Fill(0, 0, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.Fill(0, 1, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[2]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[3]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[4]}))
	}
	{
		bs, err := aggexec.MarshalAggFuncExec(executor)
		require.NoError(t, err)
		ag, err := aggexec.UnmarshalAggFuncExec(aggexec.NewSimpleAggMemoryManager(mg.Mp()), bs)
		require.NoError(t, err)
		ag.Free()
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
		}
		v.Free(mg.Mp())
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg.Mp())
		}
		require.Equal(t, int64(0), mg.Mp().CurrNB())
	}
}

func TestAvgTwCacheDecimal64(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     function.AggAvgTwCacheOverloadID,
		distinct:  false,
		argType:   types.T_decimal64.ToType(),
		retType:   types.T_varchar.ToType(),
		emptyNull: false,
	}
	//registerTheTestingCount(info.aggID, info.emptyNull)
	executor := aggexec.MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)

	inputs := make([]*vector.Vector, 3)
	{
		vs := make([]types.Decimal64, 4)
		vec := vector.NewVec(types.T_decimal64.ToType())
		require.NoError(t, vector.AppendFixedList(vec, vs, nil, mg.Mp()))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vec
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.Fill(0, 0, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.Fill(0, 1, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[2]}))
	}
	{
		bs, err := aggexec.MarshalAggFuncExec(executor)
		require.NoError(t, err)
		ag, err := aggexec.UnmarshalAggFuncExec(aggexec.NewSimpleAggMemoryManager(mg.Mp()), bs)
		require.NoError(t, err)
		ag.Free()
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
		}
		v.Free(mg.Mp())
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg.Mp())
		}
		require.Equal(t, int64(0), mg.Mp().CurrNB())
	}
}

func TestAvgTwCacheDecimal128(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     function.AggAvgTwCacheOverloadID,
		distinct:  false,
		argType:   types.T_decimal128.ToType(),
		retType:   types.T_varchar.ToType(),
		emptyNull: false,
	}
	//registerTheTestingCount(info.aggID, info.emptyNull)
	executor := aggexec.MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)

	inputs := make([]*vector.Vector, 3)
	{
		vs := make([]types.Decimal128, 4)
		vec := vector.NewVec(types.T_decimal128.ToType())
		require.NoError(t, vector.AppendFixedList(vec, vs, nil, mg.Mp()))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vec
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.Fill(0, 0, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.Fill(0, 1, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[2]}))
	}
	{
		bs, err := aggexec.MarshalAggFuncExec(executor)
		require.NoError(t, err)
		ag, err := aggexec.UnmarshalAggFuncExec(aggexec.NewSimpleAggMemoryManager(mg.Mp()), bs)
		require.NoError(t, err)
		ag.Free()
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
		}
		v.Free(mg.Mp())
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg.Mp())
		}
		require.Equal(t, int64(0), mg.Mp().CurrNB())
	}
}

func TestAvgTwResult(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     function.AggAvgTwResultOverloadID,
		distinct:  false,
		argType:   types.T_char.ToType(),
		retType:   types.T_float64.ToType(),
		emptyNull: false,
	}
	//registerTheTestingCount(info.aggID, info.emptyNull)
	executor := aggexec.MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)

	inputType := info.argType
	inputs := make([]*vector.Vector, 5)
	{
		// prepare the input data.
		var err error

		vec := vector.NewVec(inputType)
		require.NoError(t, vector.AppendStringList(vec, []string{"sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf"}, []bool{false, true, false, false}, mg.Mp()))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vector.NewConstNull(inputType, 2, mg.Mp())
		inputs[3], err = vector.NewConstBytes(inputType, []byte("sdfasdfsadfasdfadf"), 3, mg.Mp())
		require.NoError(t, err)
		inputs[4] = vector.NewVec(inputType)
		require.NoError(t, vector.AppendStringList(inputs[4], []string{"sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf", "sdfasdfsadfasdfadf"}, nil, mg.Mp()))
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.Fill(0, 0, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.Fill(0, 1, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[2]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[3]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[4]}))
	}
	{
		bs, err := aggexec.MarshalAggFuncExec(executor)
		require.NoError(t, err)
		ag, err := aggexec.UnmarshalAggFuncExec(aggexec.NewSimpleAggMemoryManager(mg.Mp()), bs)
		require.NoError(t, err)
		ag.Free()
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
		}
		v.Free(mg.Mp())
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg.Mp())
		}
		require.Equal(t, int64(0), mg.Mp().CurrNB())
	}
}

func TestAvgTwResultDecimal(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     function.AggAvgTwResultOverloadID,
		distinct:  false,
		argType:   types.T_varchar.ToType(),
		retType:   types.T_decimal128.ToType(),
		emptyNull: false,
	}
	//registerTheTestingCount(info.aggID, info.emptyNull)
	executor := aggexec.MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)

	inputType := info.argType
	inputs := make([]*vector.Vector, 5)
	{
		// prepare the input data.
		var err error

		str := "agg test str: AppendStringList NewConstBytes AppendStringList"

		vec := vector.NewVec(inputType)
		require.NoError(t, vector.AppendStringList(vec, []string{str, str, str, str}, []bool{false, true, false, false}, mg.Mp()))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vector.NewConstNull(inputType, 2, mg.Mp())
		inputs[3], err = vector.NewConstBytes(inputType, []byte(str), 3, mg.Mp())
		require.NoError(t, err)
		inputs[4] = vector.NewVec(inputType)
		require.NoError(t, vector.AppendStringList(inputs[4], []string{str, str, str, str}, nil, mg.Mp()))
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.Fill(0, 0, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.Fill(0, 1, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[2]}))
		require.Error(t, executor.BulkFill(0, []*vector.Vector{inputs[3]}))
		require.Error(t, executor.BulkFill(0, []*vector.Vector{inputs[4]}))
	}
	{
		bs, err := aggexec.MarshalAggFuncExec(executor)
		require.NoError(t, err)
		ag, err := aggexec.UnmarshalAggFuncExec(aggexec.NewSimpleAggMemoryManager(mg.Mp()), bs)
		require.NoError(t, err)
		ag.Free()
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
		}
		v.Free(mg.Mp())
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg.Mp())
		}
		require.Equal(t, int64(0), mg.Mp().CurrNB())
	}
}
