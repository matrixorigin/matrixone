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
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDependentPreparedTypeEdges(t *testing.T) {
	for _, sql := range []string{
		`SELECT (SELECT (SELECT ? + n1.N_NATIONKEY) FROM NATION n2 WHERE n2.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n1`,
		`SELECT (SELECT (SELECT n1.x)) FROM (SELECT ? AS x) n1`,
		`SELECT (SELECT (? + n1.N_NATIONKEY) IN (SELECT n1.N_NATIONKEY FROM NATION n2)) FROM NATION n1`,
	} {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			stmts, err := mysql.Parse(context.Background(), sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			prepared, err := BuildPlan(mock.CurrentContext(), stmts[0], true)
			require.NoError(t, err)
			require.NotNil(t, prepared.GetQuery().Correlation)
			_, params, err := ResetPreparePlan(mock.CurrentContext(), prepared)
			require.NoError(t, err)
			require.Len(t, params, 1)
			original := proto.Clone(prepared).(*plan.Plan)
			for _, typ := range []types.Type{types.T_int64.ToType(), types.T_float64.ToType()} {
				executed, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), prepared, []any{
					ParamValue{Value: "2", RuntimeType: typ, HasRuntimeType: true, IsBinaryProtocol: true},
				})
				require.NoError(t, err)
				require.NoError(t, executed.GetQuery().ValidateCorrelation())
				require.True(t, proto.Equal(original, prepared), "execution mutated the prepared template")
			}
		})
	}
}

func TestDependentPublicPlan(t *testing.T) {
	for _, sql := range []string{
		`SELECT n1.N_NATIONKEY, (SELECT (SELECT n3.N_REGIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY) FROM NATION n2 WHERE n2.N_NATIONKEY = n1.N_NATIONKEY) FROM NATION n1`,
		`SELECT n1.N_NATIONKEY, (SELECT SUM(n2.N_NATIONKEY) OVER (PARTITION BY (SELECT n1.N_NATIONKEY)) FROM NATION n2 LIMIT 1) FROM NATION n1`,
		`SELECT n1.N_NATIONKEY, (SELECT MAX(n2.N_REGIONKEY) FROM NATION n2 WHERE n2.N_REGIONKEY = (SELECT COUNT(*) FROM NATION n3 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY)) FROM NATION n1`,
		`SELECT n1.N_NATIONKEY, (SELECT (SELECT n1.N_NATIONKEY + n2.N_REGIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY = n2.N_NATIONKEY) FROM NATION n2 WHERE n2.N_NATIONKEY = n1.N_NATIONKEY) FROM NATION n1`,
	} {
		p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
		require.NoError(t, err, sql)
		require.NotNil(t, p.GetQuery().Correlation)
		require.NoError(t, p.GetQuery().ValidateCorrelation())
		op := NewBaseOptimizer(NewMockOptimizer(true, newPlanTestProcess(t)).CurrentContext())
		op.qry = DeepCopyQuery(p.GetQuery())
		optimized, err := op.optimize()
		require.NoError(t, err)
		require.NoError(t, optimized.ValidateCorrelation())
		require.Len(t, GetResultColumnsFromPlan(&Plan{Plan: &plan.Plan_Query{Query: optimized}}), 2)
	}
}

func TestDependentODKUAssignmentBinding(t *testing.T) {
	for _, update := range []string{
		`b=concat(b,'x'), b=(SELECT (SELECT concat(t1.b,'y')))`,
		`b=(SELECT (SELECT VALUES(b)))`,
		`b=(SELECT concat(b, VALUES(b)))`,
	} {
		t.Run(update, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			p, err := runOneStmt(mock, t,
				`INSERT INTO constraint_test.t1(a,b) VALUES(1,'incoming') ON DUPLICATE KEY UPDATE `+update)
			require.NoError(t, err)
			q := p.GetQuery()
			require.NotNil(t, q.Correlation)
			require.NoError(t, q.ValidateCorrelation())
			found := false
			for _, n := range q.Nodes {
				if n.DedupJoinCtx == nil || n.OnDuplicateAction != plan.Node_UPDATE {
					continue
				}
				require.True(t, n.DedupJoinCtx.EmitActionRows)
				for _, expr := range n.DedupJoinCtx.UpdateColExprList {
					require.NoError(t, plan.VisitExprTree(expr, func(e *plan.Expr) error {
						if call := e.GetDependent(); call != nil {
							found = true
							stage := q.Correlation.Stages[call.StageId-1]
							require.Equal(t, plan.CorrelationStage_ASSIGNMENT, stage.Kind)
							require.Positive(t, stage.ImageVersion)
							for _, input := range call.Inputs {
								require.NotNil(t, input.GetCol())
								require.Contains(t, []int32{0, 1}, input.GetCol().RelPos)
							}
						}
						return nil
					}))
				}
			}
			require.True(t, found, "assignment must retain its demand instead of moving to INSERT input")
		})
	}
}

func TestDependentConditionalInsertBinding(t *testing.T) {
	for _, sql := range []string{
		`insert first when deptno=1 then into t3(a) values(deptno)
		 when (select (select deptno))=2 then into t2(a,b) values(deptno,deptno)
		 select deptno from dept`,
		`insert first when deptno=1 then into t3(a) values((select(select deptno)))
		 else into t2(a,b) values(deptno,(select(select deptno+1)))
		 select deptno from dept`,
	} {
		p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
		require.NoError(t, err)
		require.NotNil(t, p.GetQuery().Correlation)
		require.NoError(t, p.GetQuery().ValidateCorrelation())
		writes := 0
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType == plan.Node_MULTI_UPDATE {
				writes++
			}
		}
		require.Equal(t, 2, writes)
	}
}

func TestDependentBindingForwardsAncestorSlots(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	typ := plan.Type{Id: int32(types.T_int64)}
	rootCtx := NewBindContext(builder, nil)
	middleCtx := NewBindContext(builder, rootCtx)
	innerCtx := NewBindContext(builder, middleCtx)
	outerRef := &plan.Expr{Typ: typ, Expr: &plan.Expr_Corr{Corr: &plan.CorrColRef{RelPos: 7, ColPos: 0, Depth: 2}}}
	innerCtx.results = []*plan.Expr{outerRef}
	inner := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{outerRef}}, innerCtx)
	innerCall, err := builder.bindDependentSubquery(&plan.SubqueryRef{NodeId: inner, RowSize: 1})
	require.NoError(t, err)
	require.Equal(t, uint32(1), outerRef.GetSlot().ProgramId)
	require.Equal(t, int32(1), innerCall.GetDependent().Inputs[0].GetCorr().Depth)
	middleCtx.results = []*plan.Expr{innerCall}
	middle := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{innerCall}}, middleCtx)
	middleCall, err := builder.bindDependentSubquery(&plan.SubqueryRef{NodeId: middle, RowSize: 1})
	require.NoError(t, err)
	require.Equal(t, uint32(2), innerCall.GetDependent().Inputs[0].GetSlot().ProgramId)
	require.Equal(t, int32(7), middleCall.GetDependent().Inputs[0].GetCol().RelPos)
	main := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{middleCall}}, rootCtx)
	builder.qry.Steps = []int32{main}
	require.NoError(t, builder.finalizeCorrelationBindings())
	require.Equal(t, []uint32{0, 1, 2}, builder.qry.Correlation.StepOwners)
	require.NoError(t, builder.qry.ValidateCorrelation())
	clone := DeepCopyQuery(builder.qry)
	require.NoError(t, clone.ValidateCorrelation())
	require.True(t, proto.Equal(builder.qry, clone))
	clone.Correlation.Programs[0].InputTypes[0].Width = 42
	clone.Nodes[middle].ProjectList[0].GetDependent().Inputs[0].GetSlot().Ordinal = 7
	require.Zero(t, builder.qry.Correlation.Programs[0].InputTypes[0].Width)
	require.Zero(t, innerCall.GetDependent().Inputs[0].GetSlot().Ordinal)
}

func TestDependentBindingDoesNotMergeSemanticOwners(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	ctx := NewBindContext(builder, nil)
	typ := plan.Type{Id: int32(types.T_varchar), Width: 12, PadSpace: true}
	corr := func(tag, depth int32) *plan.Expr {
		return &plan.Expr{Typ: typ, Expr: &plan.Expr_Corr{Corr: &plan.CorrColRef{RelPos: tag, Depth: depth}}}
	}
	refs := []*plan.Expr{corr(1, 1), corr(1, 1), corr(2, 1), corr(1, 2)}
	ctx.results = []*plan.Expr{refs[0]}
	root := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, ProjectList: refs}, ctx)
	call, err := builder.bindDependentSubquery(&plan.SubqueryRef{NodeId: root, RowSize: 1})
	require.NoError(t, err)
	require.Len(t, call.GetDependent().Inputs, 3)
	require.Equal(t, refs[0].GetSlot().Ordinal, refs[1].GetSlot().Ordinal)
	require.NotEqual(t, refs[0].GetSlot().Ordinal, refs[2].GetSlot().Ordinal)
	require.NotEqual(t, refs[0].GetSlot().Ordinal, refs[3].GetSlot().Ordinal)
	require.True(t, builder.qry.Correlation.Programs[0].InputTypes[0].PadSpace)
}

func TestDependentStagesIsolateSharedOwners(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	shared := &plan.Expr{Expr: &plan.Expr_Dependent{Dependent: &plan.DependentExpr{ProgramId: 1}}}
	partition := &plan.Node{NodeId: 0, NodeType: plan.Node_PARTITION, OrderBy: []*plan.OrderBySpec{{Expr: shared}}}
	window := &plan.Node{NodeId: 1, NodeType: plan.Node_WINDOW, Children: []int32{0}, ProjectList: []*plan.Expr{shared}}
	builder.qry.Nodes = []*plan.Node{partition, window}
	builder.qry.Steps = []int32{1}
	builder.qry.Correlation = &plan.CorrelationSpec{}
	for range 2 {
		require.NoError(t, builder.assignCorrelationStages())
		left := partition.OrderBy[0].Expr.GetDependent()
		right := window.ProjectList[0].GetDependent()
		require.NotSame(t, left, right)
		require.NotEqual(t, left.OccurrenceId, right.OccurrenceId)
		require.NotEqual(t, left.StageId, right.StageId)
		require.Equal(t, int32(0), builder.qry.Correlation.Stages[left.StageId-1].OwnerNode)
		require.Equal(t, plan.CorrelationStage_INPUT, builder.qry.Correlation.Stages[left.StageId-1].Kind)
		require.Equal(t, int32(1), builder.qry.Correlation.Stages[right.StageId-1].OwnerNode)
		require.Equal(t, plan.CorrelationStage_WINDOW_ROW, builder.qry.Correlation.Stages[right.StageId-1].Kind)
	}
	require.Zero(t, shared.GetDependent().OccurrenceId)
}

func TestDependentBindingFailureDoesNotPublishPartialSlots(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	ctx := NewBindContext(builder, nil)
	valid := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Corr{Corr: &plan.CorrColRef{Depth: 1}}}
	invalid := DeepCopyExpr(valid)
	invalid.GetCorr().Depth = 0
	ctx.results = []*plan.Expr{valid}
	root := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{valid, invalid}}, ctx)
	_, err := builder.bindDependentSubquery(&plan.SubqueryRef{NodeId: root, RowSize: 1})
	require.ErrorContains(t, err, "invalid correlation depth")
	require.NotNil(t, valid.GetCorr())
	require.Nil(t, builder.qry.Correlation)
}
