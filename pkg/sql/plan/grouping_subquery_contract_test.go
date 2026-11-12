// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestGroupingCompositeOperandProof(t *testing.T) {
	for _, fn := range []string{"+", "coalesce"} {
		t.Run(fn, func(t *testing.T) {
			input := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1}}}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), fn,
				[]*plan.Expr{input, MakePlan2Int64ConstExprWithType(0)})
			require.NoError(t, err)
			require.True(t, expr.Typ.NotNullable)
			branch := &BindContext{groupTag: 1, aggregateTag: 2, windowTag: 3, projectTag: 4,
				groupingStage: &groupingStage{inputs: map[[2]int32]int32{{1, 0}: 0}}}
			mergedType := input.Typ
			mergedType.NotNullable = false
			rebindGroupingExpr(expr, branch, &BindContext{windowTag: 5, projectTag: 6},
				&plan.Node{BindingTags: []int32{7}, ProjectList: []*plan.Expr{{Typ: mergedType}}})
			require.False(t, input.Typ.NotNullable)
			require.Equal(t, fn == "coalesce", expr.Typ.NotNullable,
				"composite operands must reflect merged input proofs before MARK optimization")
		})
	}
}

func TestGroupingDistinctSubqueryAliasOrder(t *testing.T) {
	for _, query := range []string{
		"select distinct (select max(r_regionkey) from region) k from nation group by n_regionkey with rollup order by k+1",
		"select distinct (select max(r_regionkey) from region) k from nation group by n_regionkey with rollup order by abs(k) limit 1",
		"select distinct (select 7) k from nation group by n_regionkey with rollup order by k+1",
		"select distinct coalesce((select max(r_regionkey) from region),0) k from nation group by n_regionkey with rollup order by k+1",
		"select distinct (select count(*) from region) k from nation group by n_regionkey with rollup order by k+1",
	} {
		for _, prefix := range []string{"", "create table ordered_scalar as "} {
			t.Run(prefix+query, func(t *testing.T) {
				_, err := buildSingleStmt(NewMockOptimizer(false), t, prefix+query)
				require.NoError(t, err, "a selected scalar subquery alias is available after DISTINCT")
			})
		}
	}
}

func TestGroupingAliasSubqueryIdentity(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select distinct (select max(r_regionkey) from region) a,(select max(r_regionkey) from region) b from nation group by n_regionkey with rollup order by a+1,b+1", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockOptimizer(false).CurrentContext(), false, true)
	_, err = builder.bindSelect(stmt.(*tree.Select), NewBindContext(builder, nil), true)
	require.NoError(t, err)
	seen := make(map[*groupingStage]bool)
	for _, ctx := range builder.ctxByNode {
		if ctx == nil || ctx.groupingStage == nil || seen[ctx.groupingStage] {
			continue
		}
		stage := ctx.groupingStage
		seen[stage] = true
		require.Len(t, stage.projects, 4)
		ids := make([]int32, 4)
		for i, expr := range stage.projects {
			found := 0
			walkGroupingExpr(expr, func(e *plan.Expr) {
				if sub := e.GetSub(); sub != nil {
					ids[i] = sub.NodeId
					found++
				}
			})
			require.Equal(t, 1, found)
		}
		require.NotEqual(t, ids[0], ids[1], "independent AST subqueries must not be merged by SQL text")
		require.Equal(t, ids[0], ids[2], "alias a reuses only its own bound subquery")
		require.Equal(t, ids[1], ids[3], "alias b reuses only its own bound subquery")
	}
	require.NotEmpty(t, seen)
}

func TestGroupingSubqueryAliasRepeatedCTAS(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table scalar_alias as select distinct (select max(r_regionkey) from region) k from nation group by n_regionkey with rollup order by k+1", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	mock := NewMockOptimizer(false)
	var source string
	for generation := 0; generation < 3; generation++ {
		p, err := BuildPlan(mock.CurrentContext(), stmt, false)
		require.NoError(t, err)
		order := stmt.(*tree.CreateTable).AsSource.OrderBy[0].Expr.(*tree.BinaryExpr)
		name, ok := order.Left.(*tree.UnresolvedName)
		require.True(t, ok, "ORDER BY must retain the alias for CTAS serialization and rebinding")
		require.Equal(t, "k", name.ColName())
		sql := p.GetDdl().GetCreateTable().CreateAsSelectSql
		if generation == 0 {
			source = sql
		} else {
			require.Equal(t, source, sql)
		}
	}
}
