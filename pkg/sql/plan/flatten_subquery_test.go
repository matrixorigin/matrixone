// Copyright 2026 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCanPullupDeepCorrelatedPredicates(t *testing.T) {
	for _, tc := range []struct {
		typ  plan.SubqueryRef_Type
		want bool
	}{
		{typ: plan.SubqueryRef_EXISTS, want: true},
		{typ: plan.SubqueryRef_NOT_EXISTS, want: true},
		{typ: plan.SubqueryRef_IN, want: true},
		{typ: plan.SubqueryRef_NOT_IN, want: true},
		{typ: plan.SubqueryRef_ANY, want: true},
		{typ: plan.SubqueryRef_ALL, want: true},
		{typ: plan.SubqueryRef_SCALAR, want: false},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			require.Equal(t, tc.want, canPullupDeepCorrelatedPredicates(tc.typ))
		})
	}
}

func TestHasInnerColumnInDeepCorrelatedFilters(t *testing.T) {
	const (
		subID    int32 = 0
		innerTag int32 = 1
		outerTag int32 = 2
	)

	builder := &QueryBuilder{
		qry: &plan.Query{
			Nodes: []*plan.Node{
				{
					NodeId:      subID,
					NodeType:    plan.Node_TABLE_SCAN,
					BindingTags: []int32{innerTag},
				},
			},
		},
	}

	require.False(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, nil))
	require.False(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, []*plan.Expr{}))
	require.True(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, []*plan.Expr{
		newFlattenSubqueryTestColExpr(innerTag),
	}))
	require.False(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, []*plan.Expr{
		newFlattenSubqueryTestColExpr(outerTag),
	}))
}

func TestInSubqueryJoinShapePreservesThreeValuedSemantics(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		want       plan.Node_JoinType
		forbidMark bool
	}{
		{
			name:       "where in becomes semi join",
			sql:        "select n_name from tpch.nation where n_regionkey in (select r_regionkey from tpch.region)",
			want:       plan.Node_SEMI,
			forbidMark: true,
		},
		{
			name:       "non-null where not in becomes anti join",
			sql:        "select n_name from tpch.nation where n_regionkey not in (select r_regionkey from tpch.region)",
			want:       plan.Node_ANTI,
			forbidMark: true,
		},
		{
			name: "projected in keeps mark result",
			sql:  "select n_regionkey in (select r_regionkey from tpch.region) from tpch.nation",
			want: plan.Node_MARK,
		},
		{
			name: "nullable where not in keeps null-aware mark result",
			sql:  "select n_name from tpch.nation where n_comment not in (select r_comment from tpch.region)",
			want: plan.Node_MARK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tt.sql)
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			require.Truef(t, hasJoinType(query, tt.want), "expected %s join in plan", tt.want)
			if tt.forbidMark {
				require.False(t, hasJoinType(query, plan.Node_MARK))
			}
		})
	}
}

func TestDirectCorrelatedScalarProjectionUsesMatchMarker(t *testing.T) {
	for _, tt := range []struct {
		name         string
		subquery     string
		joinType     plan.Node_JoinType
		wantTruePred bool
	}{
		{
			name:     "direct",
			subquery: "select n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey",
			joinType: plan.Node_SINGLE,
		},
		{
			name:     "order by",
			subquery: "select n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey order by r.r_regionkey",
			joinType: plan.Node_SINGLE,
		},
		{
			name:     "non-one limit fallback",
			subquery: "select n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey limit 2",
			joinType: plan.Node_SINGLE,
		},
		{
			name:     "distinct",
			subquery: "select distinct n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey",
			joinType: plan.Node_MARK,
		},
		{
			name:         "predicate-free distinct",
			subquery:     "select distinct n.n_regionkey from tpch.region r",
			joinType:     plan.Node_MARK,
			wantTruePred: true,
		},
		{
			name:     "order by limit one",
			subquery: "select n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey order by r.r_regionkey limit 1",
			joinType: plan.Node_MARK,
		},
		{
			name:         "predicate-free limit one",
			subquery:     "select n.n_regionkey from tpch.region r limit 1",
			joinType:     plan.Node_MARK,
			wantTruePred: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
				"select n.*, ("+tt.subquery+") as x from tpch.nation n")
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)

			var scalarJoin *plan.Node
			hasCase := false
			for _, node := range query.Nodes {
				for _, expr := range node.ProjectList {
					require.False(t, hasCorrCol(expr), "executable PROJECT contains a correlated expression")
					if f := expr.GetF(); f != nil && f.Func.GetObjName() == "case" {
						hasCase = true
						require.Len(t, f.Args, 3)
					}
				}
				for _, expr := range node.OnList {
					require.False(t, hasCorrCol(expr), "executable JOIN contains a correlated expression")
				}
				for _, expr := range node.FilterList {
					require.False(t, hasCorrCol(expr), "executable FILTER contains a correlated expression")
				}
				for _, orderBy := range node.OrderBy {
					require.False(t, hasCorrCol(orderBy.Expr), "executable SORT contains a correlated expression")
				}
				if node.NodeType == plan.Node_JOIN && node.JoinType == tt.joinType {
					scalarJoin = node
				}
			}

			require.NotNil(t, scalarJoin)
			require.Len(t, scalarJoin.Children, 2)
			if tt.wantTruePred {
				require.NotEmpty(t, scalarJoin.OnList)
				pred := scalarJoin.OnList[0].GetLit()
				require.NotNil(t, pred)
				require.True(t, pred.GetBval())
			}
			rightProject := query.Nodes[scalarJoin.Children[1]]
			require.Equal(t, plan.Node_PROJECT, rightProject.NodeType)
			require.NotEmpty(t, rightProject.ProjectList)
			if tt.joinType == plan.Node_SINGLE {
				marker := rightProject.ProjectList[0].GetLit()
				require.NotNil(t, marker)
				require.True(t, marker.GetBval())
			}
			require.True(t, hasCase)
		})
	}
}

func TestNormalizeDirectCorrelatedScalarProjectionFallsBack(t *testing.T) {
	const (
		projectTag int32 = 10
		outerTag   int32 = 20
	)

	newCorr := func(relPos, colPos, depth int32) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Corr{
				Corr: &plan.CorrColRef{RelPos: relPos, ColPos: colPos, Depth: depth},
			},
		}
	}
	validProject := func() *plan.Node {
		return &plan.Node{
			NodeType:    plan.Node_PROJECT,
			BindingTags: []int32{projectTag},
			ProjectList: []*plan.Expr{newCorr(outerTag, 0, 1)},
		}
	}

	for _, tt := range []struct {
		name     string
		results  []*plan.Expr
		projects []*plan.Expr
		nodes    []*plan.Node
		subID    int32
	}{
		{
			name:     "missing result",
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes:    []*plan.Node{validProject()},
		},
		{
			name:     "nested offset",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				validProject(),
				{NodeType: plan.Node_SORT, Children: []int32{0}, Offset: makePlan2Uint64ConstExprWithType(1)},
				{NodeType: plan.Node_SORT, Children: []int32{1}},
			},
			subID: 2,
		},
		{
			name:     "non-one limit below rim",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, BindingTags: []int32{projectTag}, ProjectList: []*plan.Expr{newCorr(outerTag, 0, 1)}, Limit: makePlan2Uint64ConstExprWithType(2)},
				{NodeType: plan.Node_SORT, Children: []int32{0}},
			},
			subID: 1,
		},
		{
			name:     "empty project list",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, BindingTags: []int32{projectTag}},
			},
		},
		{
			name:     "mismatched projected column",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, BindingTags: []int32{projectTag}, ProjectList: []*plan.Expr{newCorr(outerTag, 1, 1)}},
			},
		},
		{
			name:     "non-unary wrapper",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				{NodeType: plan.Node_SORT, Children: []int32{0, 1}},
			},
		},
		{
			name:     "unsupported wrapper",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				validProject(),
				{NodeType: plan.Node_FILTER, Children: []int32{0}},
			},
			subID: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			builder := &QueryBuilder{qry: &plan.Query{Nodes: tt.nodes}}
			ctx := &BindContext{
				projectTag: projectTag,
				results:    tt.results,
				projects:   tt.projects,
			}

			nodeID, match, outerResult, existential :=
				builder.normalizeDirectCorrelatedScalarProjection(tt.subID, ctx)
			require.Equal(t, tt.subID, nodeID)
			require.Nil(t, match)
			require.Nil(t, outerResult)
			require.False(t, existential)
		})
	}
}

func TestGenerateRowComparisonBuildsBalancedTree(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	subqueryCtx := NewBindContext(builder, nil)
	subqueryCtx.projectTag = 2
	subqueryCtx.results = make([]*plan.Expr, TableColumnCountLimit)

	childItems := make([]*plan.Expr, TableColumnCountLimit)
	for i := range childItems {
		childItems[i] = newRowComparisonTestColumn(1, int32(i))
		subqueryCtx.results[i] = newRowComparisonTestColumn(subqueryCtx.projectTag, int32(i))
	}
	child := &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: childItems},
		},
	}

	for _, tt := range []struct {
		name      string
		op        string
		logicalOp string
	}{
		{name: "tuple in equality", op: "=", logicalOp: "and"},
		{name: "tuple not in inequality", op: "<>", logicalOp: "or"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := builder.generateRowComparison(tt.op, child, subqueryCtx, false)
			require.NoError(t, err)
			require.Equal(t, tt.logicalOp, expr.GetF().Func.GetObjName())

			depth, leaves := planExprDepthAndLeaves(expr)
			require.Equal(t, TableColumnCountLimit*2, leaves)
			require.LessOrEqual(t, depth, 14)
		})
	}
}

func TestGenerateRowComparisonRejectsEmptyTuple(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	subqueryCtx := NewBindContext(builder, nil)

	_, err := builder.generateRowComparison("=", &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{},
		},
	}, subqueryCtx, false)
	require.ErrorContains(t, err, "row comparison requires at least one column")
}

func newRowComparisonTestColumn(relPos, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: relPos,
				ColPos: colPos,
			},
		},
	}
}

func hasJoinType(query *plan.Query, joinType plan.Node_JoinType) bool {
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == joinType {
			return true
		}
	}
	return false
}

func newFlattenSubqueryTestColExpr(tag int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: tag,
			},
		},
	}
}
