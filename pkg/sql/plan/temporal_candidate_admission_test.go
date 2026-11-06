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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalCandidatePredicateAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	col := GetColExpr(Type{Id: int32(types.T_datetime)}, 0, 0)
	atom, err := BindFuncExprImplByPlanExpr(proc.Ctx, "<", []*Expr{col, now})
	require.NoError(t, err)
	require.True(t, temporalCandidatePredicateSafe(atom))
	matched, ok := temporalCandidateAtomForExpr(atom)
	require.True(t, ok)
	require.Equal(t, types.TemporalLess, matched.op)
	reversed, err := BindFuncExprImplByPlanExpr(proc.Ctx, "<", []*Expr{now, col})
	require.NoError(t, err)
	matched, ok = temporalCandidateAtomForExpr(reversed)
	require.True(t, ok)
	require.Equal(t, types.TemporalGreater, matched.op)
	both, err := BindFuncExprImplByPlanExpr(proc.Ctx, "and", []*Expr{atom, MakePlan2BoolConstExprWithType(true)})
	require.NoError(t, err)
	require.True(t, temporalCandidatePredicateSafe(both))

	fsp := func(v int64) *Expr {
		return &Expr{Typ: Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}}}}
	}
	withFSP := DeepCopyExpr(now)
	withFSP.GetF().Args = []*Expr{fsp(3)}
	require.True(t, temporalCandidateBoundSafe(withFSP))
	precisionAtom, err := BindFuncExprImplByPlanExpr(proc.Ctx, "<", []*Expr{col, withFSP})
	require.NoError(t, err)
	_, ok = temporalCandidateAtomForExpr(precisionAtom)
	require.False(t, ok, "runtime NOW precision must agree with planner metadata")
	withFSP.Typ.Scale = 3
	precisionAtom, err = BindFuncExprImplByPlanExpr(proc.Ctx, "<", []*Expr{col, withFSP})
	require.NoError(t, err)
	matched, ok = temporalCandidateAtomForExpr(precisionAtom)
	require.True(t, ok)
	require.Equal(t, int32(3), matched.scale)
	withFSP.GetF().Args[0].GetLit().Src = &Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{}}}
	require.False(t, temporalCandidatePredicateSafe(&Expr{Typ: Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.LESS_THAN, 0)}, Args: []*Expr{col, withFSP}}}}))
	withFSP.GetF().Args = []*Expr{fsp(7)}
	require.False(t, temporalCandidateBoundSafe(withFSP))
	withFSP.GetF().Args = []*Expr{{Typ: Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}}
	require.False(t, temporalCandidateBoundSafe(withFSP))
	volatile := DeepCopyExpr(now)
	volatile.GetF().Func.Obj = function.EncodeOverloadID(function.SYSDATE, 0)
	require.False(t, temporalCandidateBoundSafe(volatile))
	unknown := &Expr{Typ: Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: -1}}}}
	both.GetF().Args[1] = unknown
	require.False(t, temporalCandidatePredicateSafe(both))
	divisor := GetColExpr(Type{Id: int32(types.T_int64)}, 0, 1)
	division, err := BindFuncExprImplByPlanExpr(proc.Ctx, "/", []*Expr{fsp(1), divisor})
	require.NoError(t, err)
	divPredicate, err := BindFuncExprImplByPlanExpr(proc.Ctx, ">", []*Expr{division, fsp(0)})
	require.NoError(t, err)
	both.GetF().Args[1] = divPredicate
	require.False(t, temporalCandidatePredicateSafe(both), "row-scoped division diagnostics must not be pruned")
	scan := &plan.Node{NodeType: plan.Node_TABLE_SCAN, FilterList: []*Expr{atom}, ProjectList: []*Expr{col}}
	require.True(t, temporalCandidateScanExpressionsSafe(scan))
	scan.FilterList = append(scan.FilterList, unknown)
	require.False(t, temporalCandidateScanExpressionsSafe(scan))
	scan.FilterList = []*Expr{atom}
	scan.Limit = fsp(1)
	require.False(t, temporalCandidateScanExpressionsSafe(scan))
	scan.Limit = nil
	project := &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{0}, ProjectList: []*Expr{col}}
	query := &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{1}, Nodes: []*plan.Node{scan, project}}
	require.True(t, temporalCandidateSimplePathSafe(query, 0))
	project.ProjectList = append(project.ProjectList, unknown)
	require.False(t, temporalCandidateSimplePathSafe(query, 0))
	project.ProjectList = []*Expr{col}
	project.NodeType = plan.Node_JOIN
	require.False(t, temporalCandidateSimplePathSafe(query, 0))
	project.NodeType = plan.Node_PROJECT
	query.Steps = []int32{1, 0}
	require.False(t, temporalCandidateSimplePathSafe(query, 0))
}

func TestTemporalOriginalOwnerProofPrecedesRewrite(t *testing.T) {
	builder, _, _ := temporalCBOTraversalFixture(t)
	query := builder.qry
	query.Steps = nil
	scan := query.Nodes[0]
	col := GetColExpr(scan.TableDef.Cols[0].Typ, scan.BindingTags[0], 0)
	project := &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{0}, ProjectList: []*Expr{col}}
	query.Nodes = append(query.Nodes, project)
	builder.collectOriginalTemporalOwner(1)
	require.True(t, builder.temporalOriginalScan.safe)
	require.Equal(t, int32(0), builder.temporalOriginalScan.scanID)
	require.Empty(t, query.Steps, "proof must not mutate the original plan")

	unsafe := &Expr{Typ: Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}
	project.ProjectList = append(project.ProjectList, unsafe)
	builder.collectOriginalTemporalOwner(1)
	project.ProjectList = project.ProjectList[:1] // a later rewrite removes the unsafe expression
	require.False(t, builder.temporalOriginalScan.safe)
	builder.planningInput = nil
	builder.collectOriginalTemporalOwner(1)
	require.False(t, builder.temporalOriginalScan.safe)
}

func TestTemporalOriginalSingleLeafHasIndependentCostEvidence(t *testing.T) {
	builder, _, _ := temporalCBOTraversalFixture(t)
	builder.qry.Steps = nil
	builder.collectOriginalTemporalOwner(0)
	require.True(t, builder.temporalOriginalScan.safe)
	scan := builder.qry.Nodes[0]
	other := *scan
	builder.qry.Nodes = append(builder.qry.Nodes, &other)
	builder.collectOriginalTemporalOwner(1)
	require.True(t, builder.temporalOriginalScan.safe)
	require.Equal(t, int32(1), builder.temporalOriginalScan.scanID)
	builder.collectOriginalTemporalOwner(-1)
	require.False(t, builder.temporalOriginalScan.safe, "an invalid new root must not retain another scan's proof")
	builder.collectOriginalTemporalOwner(0)
	require.True(t, builder.temporalOriginalScan.safe)
	require.Equal(t, int32(0), builder.temporalOriginalScan.scanID)
	scan.FilterList[0].GetF().Args[0].GetCol().RelPos++
	builder.collectOriginalTemporalOwner(0)
	require.False(t, builder.temporalOriginalScan.safe, "a foreign source cannot grant single-leaf cost")
	scan.FilterList[0].GetF().Args[0].GetCol().RelPos--
	scan.FilterList = append(scan.FilterList, DeepCopyExpr(scan.FilterList[0]))
	builder.collectOriginalTemporalOwner(0)
	require.False(t, builder.temporalOriginalScan.safe)
	scan.FilterList = scan.FilterList[:1]
	require.False(t, builder.temporalOriginalScan.safe, "later optimizer removal cannot recover original single-leaf evidence")
}

func TestTemporalOriginalLifetimeAndAdmissionAreIndependent(t *testing.T) {
	for _, tc := range []struct {
		name      string
		replan    bool
		safe      bool
		dependent bool
	}{
		{name: "suffix", safe: true},
		{name: "prepared suffix"},
		{name: "unsafe projection", replan: true},
		{name: "no index", safe: true},
		{name: "already published root", replan: true},
		{name: "cancelled", replan: true},
		{name: "unproved parameter", replan: true},
		{name: "prepare statement"},
		{name: "no resolver", replan: true, safe: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, _, index := temporalCBOTraversalFixture(t)
			builder.qry.Steps = nil
			builder.qry.ExecutionDependentAccess = false
			scan := builder.qry.Nodes[0]
			root := int32(0)
			compiler := builder.compCtx.(*temporalCBOContext).encodedIndexCostTestContext.MockCompilerContext
			switch tc.name {
			case "suffix", "prepared suffix":
				index.Parts = []string{"id", "d"}
				if tc.name == "prepared suffix" {
					compiler.SetContext(withPreparedSourceBindings(builder.GetContext(), nil, []any{"unproved"}))
				}
			case "unsafe projection":
				builder.qry.Nodes = append(builder.qry.Nodes, &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{0},
					ProjectList: []*plan.Expr{{Typ: Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}}})
				root = 1
			case "no index":
				scan.TableDef.Indexes = nil
			case "already published root":
				builder.qry.Steps = []int32{0}
			case "cancelled":
				ctx, cancel := context.WithCancel(builder.GetContext())
				cancel()
				compiler.SetContext(ctx)
			case "unproved parameter":
				scan.FilterList[0].GetF().Args[1] = &plan.Expr{Typ: Type{Id: int32(types.T_any)}, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}
			case "prepare statement":
				builder.isPrepareStatement = true
			case "no resolver":
				builder.planningInput.owner = nil
				builder.borrowResolutionAllowed = false
			}
			builder.collectOriginalTemporalOwner(root)
			require.Equal(t, tc.replan, builder.qry.ExecutionDependentAccess)
			require.Equal(t, tc.safe, builder.temporalOriginalScan.safe)
			state := preparedBindingState(builder.GetContext())
			require.Equal(t, tc.dependent, state != nil && state.valueDependent)
		})
	}
}
