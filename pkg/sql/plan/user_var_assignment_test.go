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
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBindUserVariableAssignmentPlan(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(ctx.GetContext(), "select @rank := 1", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.NotNil(t, p.GetQuery())

	found := false
	for _, node := range p.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			assignment := expr.GetUserVarAssignment()
			if assignment == nil {
				continue
			}
			found = true
			require.Equal(t, "rank", assignment.GetName())
			require.NotNil(t, assignment.GetValue())
		}
	}
	require.True(t, found, "the bound projection must retain the user-variable assignment")
}

func TestBindNestedUserVariableAssignmentPlan(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(ctx.GetContext(), "select @a := @b := 1", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	var outer, inner *Expr
	for _, node := range p.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if expr.GetUserVarAssignment() != nil {
				outer = expr
				inner = expr.GetUserVarAssignment().GetValue()
			}
		}
	}
	require.NotNil(t, outer)
	require.Equal(t, "a", outer.GetUserVarAssignment().GetName())
	require.NotNil(t, inner)
	require.Equal(t, "b", inner.GetUserVarAssignment().GetName())
}

func TestUserVariableAssignmentRejectsUnsafeFilterScope(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(ctx.GetContext(), "select 1 where @rank := 1", 1)
	require.NoError(t, err)
	defer stmt.Free()

	_, err = BuildPlan(ctx, stmt, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "WHERE is not supported")
}

func TestUserVariableAssignmentRejectsAmbiguousOrderAndLimit(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{name: "order", sql: "select @rank := 1 order by 1", want: "ORDER BY with user-variable assignment"},
		{name: "limit", sql: "select @rank := 1 limit 1", want: "LIMIT with user-variable assignment"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(ctx.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = BuildPlan(ctx, stmt, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestUserVariableAssignmentRejectsMultipleProjectionAssignments(t *testing.T) {
	for _, sql := range []string{
		"select @a := 1, @b := 2",
		"select @a := 1, @a := 2",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(ctx.GetContext(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = BuildPlan(ctx, stmt, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "multiple user-variable assignment projections")
		})
	}
}

func TestUserVariableAssignmentScopeValidationBranches(t *testing.T) {
	ctx := context.Background()
	value := tree.NewNumVal(int64(1), "1", false, tree.P_int64)
	assignment := tree.NewUserVarAssignmentExpr("a", value)
	require.False(t, containsUserVarAssignment(nil))
	require.NoError(t, validateUserVarAssignmentScope(ctx, nil, nil, nil))

	for _, tc := range []struct {
		name   string
		clause *tree.SelectClause
		order  tree.OrderBy
		limit  *tree.Limit
		want   string
	}{
		{name: "nested projection", clause: &tree.SelectClause{Exprs: tree.SelectExprs{{Expr: &tree.ParenExpr{Expr: &tree.BinaryExpr{Left: assignment, Op: tree.PLUS, Right: value}}}}}, want: "nested projection"},
		{name: "group by", clause: &tree.SelectClause{GroupBy: &tree.GroupByClause{GroupByExprsList: []tree.Exprs{{assignment}}}}, want: "GROUP BY"},
		{name: "grouping set", clause: &tree.SelectClause{GroupBy: &tree.GroupByClause{GroupingSet: tree.Exprs{assignment}}}, want: "GROUP BY"},
		{name: "having", clause: &tree.SelectClause{Having: &tree.Where{Expr: assignment}}, want: "HAVING"},
		{name: "window partition", clause: &tree.SelectClause{Windows: tree.WindowDefinitions{{Spec: &tree.WindowSpec{PartitionBy: tree.Exprs{assignment}}}}}, want: "WINDOW PARTITION BY"},
		{name: "window order", clause: &tree.SelectClause{Windows: tree.WindowDefinitions{{Spec: &tree.WindowSpec{OrderBy: tree.OrderBy{tree.NewOrder(assignment, tree.DefaultDirection, tree.DefaultNullsPosition, false)}}}}}, want: "WINDOW ORDER BY"},
		{name: "outer order", clause: &tree.SelectClause{}, order: tree.OrderBy{tree.NewOrder(assignment, tree.DefaultDirection, tree.DefaultNullsPosition, false)}, want: "ORDER BY"},
		{name: "distinct", clause: &tree.SelectClause{Distinct: true, Exprs: tree.SelectExprs{{Expr: assignment}}}, want: "DISTINCT"},
		{name: "assignment order", clause: &tree.SelectClause{Exprs: tree.SelectExprs{{Expr: assignment}}}, order: tree.OrderBy{tree.NewOrder(value, tree.DefaultDirection, tree.DefaultNullsPosition, false)}, want: "ORDER BY with user-variable assignment"},
		{name: "assignment limit", clause: &tree.SelectClause{Exprs: tree.SelectExprs{{Expr: assignment}}}, limit: tree.NewLimit(nil, value), want: "LIMIT with user-variable assignment"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateUserVarAssignmentScope(ctx, tc.clause, tc.order, tc.limit)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}

	parenAssignment := &tree.ParenExpr{Expr: assignment}
	require.NoError(t, validateUserVarAssignmentScope(ctx,
		&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: parenAssignment}}}, nil, nil))

	type cyclicNode struct {
		Next *cyclicNode
		Expr tree.Expr
	}
	cycle := &cyclicNode{}
	cycle.Next = cycle
	require.False(t, containsUserVarAssignmentValue(reflect.ValueOf(cycle), make(map[assignmentVisit]struct{})))
}

func TestSelectContainsUserVariableAssignmentAcrossStatements(t *testing.T) {
	value := tree.NewNumVal(int64(1), "1", false, tree.P_int64)
	assignment := tree.NewUserVarAssignmentExpr("a", value)
	plain := &tree.SelectClause{}
	require.False(t, selectContainsUserVarAssignment(nil))
	require.False(t, selectContainsUserVarAssignment(&tree.Select{Select: plain}))
	require.True(t, selectContainsUserVarAssignment(&tree.Select{
		Select:  plain,
		OrderBy: tree.OrderBy{tree.NewOrder(assignment, tree.DefaultDirection, tree.DefaultNullsPosition, false)},
	}))
	require.True(t, containsUserVarAssignmentInSelectStatement(&tree.SelectClause{
		Exprs: tree.SelectExprs{{Expr: assignment}},
	}))
	require.True(t, containsUserVarAssignmentInSelectStatement(tree.NewUnionClause(
		tree.UNION,
		&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: value}}},
		&tree.SelectClause{Having: &tree.Where{Expr: assignment}},
		true,
	)))
	require.True(t, containsUserVarAssignmentInSelectStatement(tree.NewValuesClause([]tree.Exprs{{assignment}})))
	require.False(t, containsUserVarAssignmentInSelectStatement(nil))
}

func makePlanUserVariableAssignment(value *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Typ: value.GetTyp(),
		Expr: &planpb.Expr_UserVarAssignment{UserVarAssignment: &planpb.UserVarAssignment{
			Name:  "rank",
			Value: value,
		}},
	}
}

func TestUserVariableAssignmentPrepareRulesAndStructuralIdentity(t *testing.T) {
	param := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
		}
	}

	assignment := makePlanUserVariableAssignment(param(2))
	get := NewGetParamRule()
	rewritten, err := get.ApplyExpr(assignment)
	require.NoError(t, err)
	require.Same(t, assignment, rewritten)
	require.Equal(t, map[int]int{2: 0}, get.params)

	get.SetParamOrder()
	ordered := makePlanUserVariableAssignment(param(2))
	_, err = NewResetParamOrderRule(get.params).ApplyExpr(ordered)
	require.NoError(t, err)
	require.Equal(t, int32(0), ordered.GetUserVarAssignment().GetValue().GetP().GetPos())

	reset := NewResetParamRefRule(context.Background(), []*planpb.Expr{
		makePlan2Int64ConstExprWithType(7),
		makePlan2Int64ConstExprWithType(8),
		makePlan2Int64ConstExprWithType(9),
	})
	rebound := makePlanUserVariableAssignment(param(2))
	_, err = reset.ApplyExpr(rebound)
	require.NoError(t, err)
	require.Equal(t, int64(9), rebound.GetUserVarAssignment().GetValue().GetLit().GetI64Val())

	preserve := NewResetParamRefRule(context.Background(), []*planpb.Expr{
		makePlan2Int64ConstExprWithType(7),
		makePlan2Int64ConstExprWithType(8),
		makePlan2Int64ConstExprWithType(9),
	})
	preserved := makePlanUserVariableAssignment(param(2))
	_, err = preserve.applyExprPreservingRoot(preserved)
	require.NoError(t, err)
	require.Equal(t, int64(9), preserved.GetUserVarAssignment().GetValue().GetLit().GetI64Val())

	for _, rule := range []VisitPlanRule{
		NewGetParamRule(),
		NewResetParamOrderRule(map[int]int{}),
		NewResetParamRefRule(context.Background(), nil),
	} {
		_, err = rule.ApplyExpr(&planpb.Expr{Expr: &planpb.Expr_UserVarAssignment{}})
		require.NoError(t, err)
	}

	a := makePlanUserVariableAssignment(makePlan2Int64ConstExprWithType(1))
	b := makePlanUserVariableAssignment(makePlan2Int64ConstExprWithType(2))
	c := DeepCopyExpr(a)
	require.True(t, exprStructuralEqual(a, c))
	require.Equal(t, exprStructuralHash(a), exprStructuralHash(c))
	require.False(t, exprStructuralEqual(a, b))
	require.NotEqual(t, exprStructuralHash(a), exprStructuralHash(b))
	nilA := &planpb.Expr{Expr: &planpb.Expr_UserVarAssignment{}}
	nilB := &planpb.Expr{Expr: &planpb.Expr_UserVarAssignment{}}
	require.True(t, exprStructuralEqual(nilA, nilB))
}

func TestUserVariableAssignmentPlanHelpers(t *testing.T) {
	col := &planpb.Expr{
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 4, ColPos: 1}},
	}
	assignment := makePlanUserVariableAssignment(col)
	require.Equal(t, map[int32]bool{4: true}, doGetBindings(assignment))
	require.True(t, HasTag(assignment, 4))
	require.False(t, HasTag(assignment, 9))
	require.False(t, hasCorrCol(assignment))
	require.False(t, hasSubquery(assignment))

	corr := &planpb.Expr{Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{RelPos: 4, ColPos: 1, Depth: 2}}}
	withCorr := makePlanUserVariableAssignment(corr)
	require.True(t, hasCorrCol(withCorr))
	_, correlated := decreaseDepth(withCorr)
	require.True(t, correlated)
	require.Equal(t, int32(1), withCorr.GetUserVarAssignment().GetValue().GetCorr().GetDepth())
	require.Equal(t, int8(JoinSideLeft), getJoinSide(assignment, map[int32]bool{4: true}, nil, -1))

	sub := &planpb.Expr{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{NodeId: 1}}}
	require.True(t, hasSubquery(makePlanUserVariableAssignment(sub)))
}
