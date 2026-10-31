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

package plan

import (
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalIndexReplanSurvivesCopyAndWire(t *testing.T) {
	query := &plan.Query{StmtType: plan.Query_SELECT, TemporalIndexNeedsReplan: true}
	copied := DeepCopyQuery(query)
	require.True(t, copied.GetTemporalIndexNeedsReplan())
	data, err := proto.Marshal(copied)
	require.NoError(t, err)
	var restored plan.Query
	require.NoError(t, proto.Unmarshal(data, &restored))
	require.True(t, restored.GetTemporalIndexNeedsReplan())
}

func TestTemporalCandidateProvenanceSurvivesFoldAndCopy(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	guard, err := BindFuncExprImplByPlanExpr(proc.Ctx, "__mo_temporal_bound_not_empty", []*Expr{
		now, MakePlan2Int32ConstExprWithType(0), MakePlan2Int32ConstExprWithType(0),
	})
	require.NoError(t, err)
	guard.TemporalCandidateGuard = true
	column := GetColExpr(Type{Id: int32(types.T_bool)}, 0, 0)
	filter, err := BindFuncExprImplByPlanExpr(proc.Ctx, "and", []*Expr{column, guard})
	require.NoError(t, err)
	filter.TemporalCandidateFilter = true
	filter = DeepCopyExpr(filter)
	require.True(t, filter.TemporalCandidateFilter)
	require.True(t, filter.GetF().Args[1].TemporalCandidateGuard)
	var executors []colexec.ExpressionExecutor
	defer func() {
		for _, executor := range executors {
			executor.Free()
		}
	}()
	_, err = ReplaceFoldExpr(proc, filter, &executors)
	require.NoError(t, err)
	require.True(t, filter.GetF().Args[1].GetFold().TemporalCandidateGuard)
	require.NoError(t, EvalFoldExpr(proc, filter, &executors))
	for _, executor := range executors {
		executor.ResetForNextQuery()
	}
	require.NoError(t, EvalFoldExpr(proc, filter, &executors))
	require.True(t, filter.GetF().Args[1].GetFold().TemporalCandidateGuard)
	copy := DeepCopyExpr(filter)
	require.True(t, copy.TemporalCandidateFilter)
	require.True(t, copy.GetF().Args[1].GetFold().TemporalCandidateGuard)
	data, err := proto.Marshal(copy)
	require.NoError(t, err)
	var decoded Expr
	require.NoError(t, proto.Unmarshal(data, &decoded))
	require.True(t, decoded.TemporalCandidateFilter)
	require.True(t, decoded.GetF().Args[1].GetFold().TemporalCandidateGuard)
}

func TestTemporalBoundKeepsClockThroughPlanningFold(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	bound, err := BindFuncExprImplByPlanExpr(proc.Ctx, "__mo_temporal_datetime_bound", []*Expr{
		now, MakePlan2Int32ConstExprWithType(int32(0)), MakePlan2Int32ConstExprWithType(0),
		MakePlan2Int32ConstExprWithType(0), MakePlan2BoolConstExprWithType(true),
	})
	require.NoError(t, err)
	serial, err := BindFuncExprImplByPlanExpr(proc.Ctx, "serial", []*Expr{bound})
	require.NoError(t, err)
	require.True(t, function.ContainsTemporalBound(serial))
	require.True(t, ExprIsZonemappable(proc.Ctx, serial))
	for _, prepared := range []bool{false, true} {
		copy := DeepCopyExpr(serial)
		folded, err := ConstantFold(batch.EmptyForConstFoldBatch, copy, proc, prepared, true)
		require.NoError(t, err)
		require.NotNil(t, folded.GetF())
		require.NotNil(t, folded.GetF().Args[0].GetF())
		require.NotNil(t, folded.GetF().Args[0].GetF().Args[0].GetF(), "statement clock was frozen")
	}
}

func TestTemporalBoundRuntimeFoldRefreshesClock(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.Base.UnixTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	bound, err := BindFuncExprImplByPlanExpr(proc.Ctx, "__mo_temporal_datetime_bound", []*Expr{
		now, MakePlan2Int32ConstExprWithType(0), MakePlan2Int32ConstExprWithType(0),
		MakePlan2Int32ConstExprWithType(0), MakePlan2BoolConstExprWithType(true),
	})
	require.NoError(t, err)
	serial, err := BindFuncExprImplByPlanExpr(proc.Ctx, "serial", []*Expr{bound})
	require.NoError(t, err)
	column := GetColExpr(Type{Id: int32(types.T_varchar)}, 0, 0)
	filter, err := BindFuncExprImplByPlanExpr(proc.Ctx, "prefix_eq", []*Expr{column, serial})
	require.NoError(t, err)
	var executors []colexec.ExpressionExecutor
	defer func() {
		for _, executor := range executors {
			executor.Free()
		}
	}()
	_, err = ReplaceFoldExpr(proc, filter, &executors)
	require.NoError(t, err)
	require.NotEmpty(t, executors)
	require.NoError(t, EvalFoldExpr(proc, filter, &executors))
	first := append([]byte(nil), filter.GetF().Args[1].GetFold().Data...)
	require.NotEmpty(t, first)
	for _, executor := range executors {
		executor.ResetForNextQuery()
	}
	proc.Base.UnixTime += int64(time.Hour)
	require.NoError(t, EvalFoldExpr(proc, filter, &executors))
	require.NotEqual(t, first, filter.GetF().Args[1].GetFold().Data)
}

func TestTemporalFoldUsesRuntimeTimestampScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", []*Expr{MakePlan2Int64ConstExprWithType(3)})
	require.NoError(t, err)
	// A parameterized NOW can carry a static scale of 6 even when its runtime
	// result is rounded to the execution's requested scale.
	now.Typ.Scale = 6
	column := GetColExpr(Type{Id: int32(types.T_datetime), Scale: 6}, 0, 0)
	filter, err := BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*Expr{column, now})
	require.NoError(t, err)
	var executors []colexec.ExpressionExecutor
	defer func() {
		for _, executor := range executors {
			executor.Free()
		}
	}()
	_, err = ReplaceFoldExpr(proc, filter, &executors)
	require.NoError(t, err)
	require.NotNil(t, filter.GetF().Args[1].GetFold())
	require.NoError(t, EvalFoldExpr(proc, filter, &executors))
	require.Equal(t, int32(3), filter.GetF().Args[1].Typ.Scale)
}

func TestTemporalFoldRefreshesPreparedTimestampScale(t *testing.T) {
	for _, tc := range []struct {
		value string
		scale int32
	}{{"0", 0}, {"3", 3}, {"6", 6}} {
		t.Run(tc.value, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.GetSessionInfo().TimeZone = time.UTC
			params := vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), false, proc.Mp()))
			proc.SetPrepareParams(params)
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			param := &Expr{Typ: Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
			target := &Expr{Typ: Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
			fsp, err := BindFuncExprImplByPlanExpr(proc.Ctx, "cast", []*Expr{param, target})
			require.NoError(t, err)
			now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", []*Expr{fsp})
			require.NoError(t, err)
			now.Typ.Scale = 6
			column := GetColExpr(Type{Id: int32(types.T_datetime), Scale: 6}, 0, 0)
			filter, err := BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*Expr{column, now})
			require.NoError(t, err)
			var executors []colexec.ExpressionExecutor
			defer func() {
				for _, executor := range executors {
					executor.Free()
				}
			}()
			_, err = ReplaceFoldExpr(proc, filter, &executors)
			require.NoError(t, err)
			require.NotNil(t, filter.GetF().Args[1].GetFold())
			require.NoError(t, EvalFoldExpr(proc, filter, &executors))
			require.Equal(t, tc.scale, filter.GetF().Args[1].Typ.Scale)
			require.Equal(t, int32(6), now.Typ.Scale, "folding must not change the raw template")
			require.NoError(t, vector.SetBytesAt(params, 0, []byte("6"), proc.Mp()))
			for _, executor := range executors {
				executor.ResetForNextQuery()
			}
			require.NoError(t, EvalFoldExpr(proc, filter, &executors))
			require.Equal(t, int32(6), filter.GetF().Args[1].Typ.Scale, "each execution refreshes scale")
			params.SetNull(0)
			for _, executor := range executors {
				executor.ResetForNextQuery()
			}
			require.NoError(t, EvalFoldExpr(proc, filter, &executors))
			rowResult, free, err := colexec.GetReadonlyResultFromExpression(proc, now, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			rowData, _ := getConstantBytes(rowResult, false, 0)
			require.Equal(t, rowResult.GetType().Scale, filter.GetF().Args[1].Typ.Scale)
			require.Equal(t, rowData, filter.GetF().Args[1].GetFold().Data)
			free()
			nulls.Del(params.GetNulls(), 0)
			require.NoError(t, vector.SetBytesAt(params, 0, []byte("3"), proc.Mp()))
			for _, executor := range executors {
				executor.ResetForNextQuery()
			}
			require.NoError(t, EvalFoldExpr(proc, filter, &executors))
			require.Equal(t, int32(3), filter.GetF().Args[1].Typ.Scale)
		})
	}
}

func TestTemporalHintSurvivesStatsFilterRebuild(t *testing.T) {
	helper := &Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{
		Obj: function.EncodeOverloadID(function.TEMPORAL_DATETIME_BOUND, 0), ObjName: "__mo_temporal_datetime_bound",
	}}}}
	helper.TemporalCandidateFilter = true
	unmarked := DeepCopyExpr(helper)
	unmarked.TemporalCandidateFilter = false
	other := MakePlan2BoolConstExprWithType(true)
	node := &plan.Node{TableDef: &plan.TableDef{}, BindingTags: []int32{1}, BlockFilterList: []*Expr{helper, unmarked, other}}
	require.Equal(t, []*Expr{helper}, existingCompositeBlockFilters(node))
}
