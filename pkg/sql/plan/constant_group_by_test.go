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
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestElideStableLiteralGroupByRemapsRegistries(t *testing.T) {
	runtimeLiteral := MakePlan2Int64ConstExprWithType(9)
	runtimeLiteral.GetLit().Src = &pbplan.Expr{
		Typ:  pbplan.Type{Id: int32(types.T_int64)},
		Expr: &pbplan.Expr_P{P: &pbplan.ParamRef{Pos: 0}},
	}
	firstColumn := groupHashKeyTestCol(7, 1)
	secondColumn := groupHashKeyTestCol(7, 2)
	ctx := &BindContext{
		groups: []*pbplan.Expr{
			MakePlan2Int64ConstExprWithType(1),
			firstColumn,
			MakePlan2Int64ConstExprWithType(2),
			runtimeLiteral,
			secondColumn,
		},
		groupingFlag: []bool{true, true, true, true, true},
		groupByAst: map[string]int32{
			"literal-one": 0,
			"first":       1,
			"literal-two": 2,
			"runtime":     3,
			"second":      4,
		},
		groupByCanonicalAst: map[string]int32{
			"canonical-literal": 0,
			"canonical-second":  4,
		},
		groupByParamAst: map[string]int32{"parameter": 3},
	}

	elideStableLiteralGroupBy(ctx)

	require.Equal(t, []*pbplan.Expr{firstColumn, runtimeLiteral, secondColumn}, ctx.groups)
	require.Equal(t, []bool{true, true, true}, ctx.groupingFlag)
	require.Equal(t, map[string]int32{
		"first": 0, "runtime": 1, "second": 2,
	}, ctx.groupByAst)
	require.Equal(t, map[string]int32{"canonical-second": 2}, ctx.groupByCanonicalAst)
	require.Equal(t, map[string]int32{"parameter": 1}, ctx.groupByParamAst)
}

func TestElideStableLiteralGroupByPreservesAllLiteralAndMalformedInputs(t *testing.T) {
	t.Run("all literals preserve empty-input semantics", func(t *testing.T) {
		groups := []*pbplan.Expr{
			MakePlan2Int64ConstExprWithType(1),
			MakePlan2Int64ConstExprWithType(2),
		}
		ctx := &BindContext{
			groups:       groups,
			groupingFlag: []bool{true, true},
			groupByAst:   map[string]int32{"one": 0, "two": 1},
		}

		elideStableLiteralGroupBy(ctx)

		require.Equal(t, groups, ctx.groups)
		require.Equal(t, map[string]int32{"one": 0, "two": 1}, ctx.groupByAst)
	})

	t.Run("mismatched grouping metadata fails closed", func(t *testing.T) {
		groups := []*pbplan.Expr{
			MakePlan2Int64ConstExprWithType(1),
			groupHashKeyTestCol(7, 1),
		}
		ctx := &BindContext{
			groups:       groups,
			groupingFlag: []bool{true},
			groupByAst:   map[string]int32{"one": 0, "column": 1},
		}

		elideStableLiteralGroupBy(ctx)

		require.Equal(t, groups, ctx.groups)
		require.Equal(t, map[string]int32{"one": 0, "column": 1}, ctx.groupByAst)
	})

	t.Run("invalid registry position fails closed", func(t *testing.T) {
		groups := []*pbplan.Expr{
			MakePlan2Int64ConstExprWithType(1),
			groupHashKeyTestCol(7, 1),
		}
		ctx := &BindContext{
			groups:       groups,
			groupingFlag: []bool{true, true},
			groupByAst:   map[string]int32{"one": 0, "invalid": 2},
		}

		elideStableLiteralGroupBy(ctx)

		require.Equal(t, groups, ctx.groups)
		require.Equal(t, map[string]int32{"one": 0, "invalid": 2}, ctx.groupByAst)
	})

	t.Run("nil group expression fails closed", func(t *testing.T) {
		groups := []*pbplan.Expr{
			MakePlan2Int64ConstExprWithType(1),
			nil,
		}
		ctx := &BindContext{
			groups:       groups,
			groupingFlag: []bool{true, true},
			groupByAst:   map[string]int32{"one": 0, "nil": 1},
		}

		elideStableLiteralGroupBy(ctx)

		require.Equal(t, groups, ctx.groups)
		require.Equal(t, map[string]int32{"one": 0, "nil": 1}, ctx.groupByAst)
	})
}

func TestBuildPlanElidesStableLiteralGroupKeys(t *testing.T) {
	tests := []struct {
		name             string
		sql              string
		wantGroupByCount int
		literalOutputs   []int
	}{
		{
			name:             "clickbench q35 shape",
			sql:              "select 1, ename, count(*) from constraint_test.emp group by 1, ename",
			wantGroupByCount: 1,
			literalOutputs:   []int{0},
		},
		{
			name:             "literal after the real key",
			sql:              "select ename, 1, count(*) from constraint_test.emp group by ename, 2",
			wantGroupByCount: 1,
			literalOutputs:   []int{1},
		},
		{
			name:             "multiple literals",
			sql:              "select 1, 'x', ename, count(*) from constraint_test.emp group by 1, 2, ename",
			wantGroupByCount: 1,
			literalOutputs:   []int{0, 1},
		},
		{
			name:             "cast expression fails closed",
			sql:              "select 1.25, ename, count(*) from constraint_test.emp group by 1, ename",
			wantGroupByCount: 2,
		},
		{
			name:             "null literal",
			sql:              "select null, ename, count(*) from constraint_test.emp group by null, ename",
			wantGroupByCount: 1,
			literalOutputs:   []int{0},
		},
		{
			name:             "grouping without aggregate",
			sql:              "select 1, ename from constraint_test.emp group by 1, ename",
			wantGroupByCount: 1,
			literalOutputs:   []int{0},
		},
		{
			name:             "distinct aggregate projection",
			sql:              "select distinct 1, ename, count(*) from constraint_test.emp group by 1, ename",
			wantGroupByCount: 1,
		},
		{
			name: "window consumer",
			sql: "select 1, ename, row_number() over (order by ename) " +
				"from constraint_test.emp group by 1, ename",
			wantGroupByCount: 1,
		},
		{
			name: "alias having and order by",
			sql: "select 1 as constant_key, ename, count(*) from constraint_test.emp " +
				"group by constant_key, ename having constant_key = 1 order by constant_key, ename",
			wantGroupByCount: 1,
			literalOutputs:   []int{0},
		},
		{
			name:             "all literals remain grouping keys",
			sql:              "select 1, 2, count(*) from constraint_test.emp group by 1, 2",
			wantGroupByCount: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)

			var aggregate, projection *pbplan.Node
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType == pbplan.Node_AGG &&
					(aggregate == nil || len(node.AggList) > 0) {
					aggregate = node
				}
				if node.NodeType == pbplan.Node_PROJECT {
					projection = node
				}
			}
			require.NotNil(t, aggregate)
			require.Len(t, aggregate.GroupBy, test.wantGroupByCount)
			require.NotNil(t, projection)
			for _, output := range test.literalOutputs {
				require.NotNil(t, projection.ProjectList[output].GetLit(),
					"output %d must remain the original literal", output)
			}
		})
	}
}

func TestBuildPlanPreservesLiteralGroupKeysForGroupingExtensions(t *testing.T) {
	queries := []string{
		"select 1, ename, count(*) from constraint_test.emp group by grouping sets ((1, ename), (1))",
		"select 1, ename, count(*) from constraint_test.emp group by 1, ename with rollup",
	}
	for _, sql := range queries {
		t.Run(sql, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, sql)
			require.NoError(t, err)

			foundLiteralGroup := false
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != pbplan.Node_AGG {
					continue
				}
				for _, group := range node.GroupBy {
					foundLiteralGroup = foundLiteralGroup || group.GetLit() != nil
				}
			}
			require.True(t, foundLiteralGroup)
		})
	}
}
