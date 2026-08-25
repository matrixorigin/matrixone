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
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func findAggregateByName(query *planpb.Query, name string) *planpb.Function {
	for _, node := range query.Nodes {
		for _, expr := range node.AggList {
			if fn := expr.GetF(); fn != nil && strings.EqualFold(fn.Func.GetObjName(), name) {
				return fn
			}
		}
	}
	return nil
}

func TestBuildOrderedSetAggregates(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	for _, tc := range []struct {
		name       string
		sql        string
		desc       byte
		wantOrder  bool
		wantMedian bool
	}{
		{name: "continuous", sql: "select percentile_cont(0.5) within group (order by a) from select_test.bind_select"},
		{name: "discrete descending", sql: "select percentile_disc(0.5) within group (order by a desc) from select_test.bind_select", desc: 1},
		{name: "group concat within group", sql: "select group_concat(a) within group (order by b desc) from select_test.bind_select", wantOrder: true},
		{name: "median within group", sql: "select median(a) within group (order by a desc) from select_test.bind_select", wantMedian: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			queryPlan, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)

			name := "percentile_cont"
			if strings.Contains(tc.sql, "percentile_disc") {
				name = "percentile_disc"
			} else if strings.Contains(tc.sql, "group_concat") {
				name = "group_concat"
			} else if strings.Contains(tc.sql, "median") {
				name = "median"
			}
			fn := findAggregateByName(queryPlan.GetQuery(), name)
			require.NotNil(t, fn)
			if tc.wantOrder {
				require.NotEqual(t, planpb.AggregateConfigType_AGG_CONFIG_NONE, fn.AggConfigType)
				require.NotEmpty(t, fn.AggConfig)
				return
			}
			if tc.wantMedian {
				require.Len(t, fn.Args, 1)
				return
			}
			require.Len(t, fn.Args, 2)
			require.Equal(t, tc.desc, fn.AggConfig[0])
		})
	}
}

func TestBuildOrderedSetPercentileRejectsNonConstant(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select percentile_cont(b) within group (order by a) from select_test.bind_select", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.ErrorContains(t, err, "percentile argument of percentile_cont must be a non-null constant")
}

func TestBuildMedianWithinGroupRejectsDifferentOrderExpression(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select median(a) within group (order by b) from select_test.bind_select", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.ErrorContains(t, err, "median requires the WITHIN GROUP ORDER BY expression to match")
}

func TestBuildMedianWithinGroupAcceptsEquivalentScalarSubquery(t *testing.T) {
	for _, sql := range []string{
		"select median((select 1)) within group (order by (select 1)) from select_test.bind_select",
		`select median((select a from select_test.bind_select y limit 1))
  within group (order by (select A from select_test.bind_select y limit 1))
  from select_test.bind_select x`,
		`select median((select a from select_test.bind_select y limit 1))
  within group (order by (select y.a from select_test.bind_select y limit 1))
  from select_test.bind_select x`,
		`select median((select a from select_test.bind_select limit 1))
  within group (order by (select select_test.bind_select.a from select_test.bind_select limit 1))
  from select_test.bind_select x`,
		`select median((select bind_select.a from select_test.bind_select limit 1))
  within group (order by (select select_test.bind_select.a from select_test.bind_select limit 1))
  from select_test.bind_select x`,
		`select median((select abs(y.a) from select_test.bind_select y limit 1))
  within group (order by (select ABS(y.A) from select_test.bind_select y limit 1))
  from select_test.bind_select x`,
		`select median((select a as lhs from select_test.bind_select limit 1))
  within group (order by (select a as rhs from select_test.bind_select limit 1))
  from select_test.bind_select x`,
		`select median((select a from select_test.bind_select limit 1))
  within group (order by (select a as rhs from select_test.bind_select limit 1))
  from select_test.bind_select x`,
		`select median((select lhs.a from select_test.bind_select lhs limit 1))
  within group (order by (select rhs.a from select_test.bind_select rhs limit 1))
  from select_test.bind_select x`,
		`select median((select lhs.a from select_test.bind_select lhs limit 1))
  within group (order by (select a from select_test.bind_select limit 1))
  from select_test.bind_select x`,
		`select median((select a as lhs from select_test.bind_select order by lhs limit 1))
  within group (order by (select a as rhs from select_test.bind_select order by rhs limit 1))
  from select_test.bind_select x`,
		`select median((select lhs.a from (select a from select_test.bind_select) lhs(a) limit 1))
  within group (order by (select rhs.b from (select a from select_test.bind_select) rhs(b) limit 1))
  from select_test.bind_select x`,
		`select median((with lhs as (select a from select_test.bind_select)
    select a from lhs limit 1))
  within group (order by (with rhs as (select a from select_test.bind_select)
    select a from rhs limit 1))
  from select_test.bind_select x`,
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)

			queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)

			fn := findAggregateByName(queryPlan.GetQuery(), "median")
			require.NotNil(t, fn)
			require.Len(t, fn.Args, 1)
		})
	}
}

func TestMedianWithinGroupValidationDoesNotChangeParentAggregateState(t *testing.T) {
	type parentState struct {
		aggregates int
		boundCols  []boundColumn
	}
	capture := func(sql string) parentState {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		selectStmt, ok := stmt.(*tree.Select)
		require.True(t, ok)

		var state parentState
		_, err = bindAndOptimizeSelectQueryWithValidatorAndCapture(
			planpb.Query_SELECT,
			NewMockCompilerContext(true),
			selectStmt,
			false,
			true,
			func(*Query) error { return nil },
			func(ctx *BindContext) {
				state.aggregates = len(ctx.aggregates)
				if provider, ok := ctx.binder.(interface{ medianValidationBaseBinder() *baseBinder }); ok {
					state.boundCols = append([]boundColumn(nil), provider.medianValidationBaseBinder().boundCols...)
				}
			},
			false,
		)
		require.NoError(t, err)
		return state
	}

	ordinary := capture(`select empno, median((select sal))
from constraint_test.emp
group by empno`)
	withinGroup := capture(`select empno,
median((select sal)) within group (order by (select sal))
from constraint_test.emp
group by empno`)
	require.Equal(t, 1, ordinary.aggregates)
	require.Equal(t, ordinary, withinGroup)
}

func TestCloneMedianBindScopeDoesNotShareMutableParentState(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	parent := NewBindContext(builder, nil)
	parent.isCorrelated = false
	parent.times = []*planpb.Expr{{Typ: planpb.Type{Id: 1}}}
	parent.timeByAst["existing"] = 0
	parent.aggregates = []*planpb.Expr{{Typ: planpb.Type{Id: 2}}}
	selectClause := &tree.SelectClause{}
	parent.expandedSelectLists = map[*tree.SelectClause]tree.SelectExprs{
		selectClause: {{Expr: tree.NewNumVal(int64(1), "1", false, tree.P_int64)}},
	}
	cteRef := &CTERef{
		ast: &tree.CTE{Name: &tree.AliasClause{Alias: tree.Identifier("c")}},
		occurrences: []cteOccurrence{{rootID: 1}},
	}
	parent.cteByName = map[string]*CTERef{"c": cteRef}
	havingBinder := NewHavingBinder(builder, parent)
	havingBinder.boundCols = []boundColumn{{name: "existing"}}
	parent.binder = havingBinder

	isolated := cloneMedianBindScope(parent, builder)
	require.NotSame(t, parent, isolated)
	require.NotSame(t, parent.binder, isolated.binder)
	require.NotSame(t, cteRef, isolated.cteByName["c"])
	require.NotSame(t, cteRef.ast, isolated.cteByName["c"].ast)

	isolated.isCorrelated = true
	isolated.times = append(isolated.times, &planpb.Expr{Typ: planpb.Type{Id: 3}})
	isolated.timeByAst["temporary"] = 1
	isolated.aggregates = append(isolated.aggregates, &planpb.Expr{Typ: planpb.Type{Id: 4}})
	isolated.expandedSelectLists[selectClause] = append(
		isolated.expandedSelectLists[selectClause],
		tree.SelectExpr{Expr: tree.NewNumVal(int64(2), "2", false, tree.P_int64)},
	)
	isolated.cteByName["c"].occurrences = append(
		isolated.cteByName["c"].occurrences,
		cteOccurrence{rootID: 2},
	)
	isolated.cteByName["c"].ast.Name.Alias = tree.Identifier("temporary")
	clonedBinder := isolated.binder.(interface{ medianValidationBaseBinder() *baseBinder }).medianValidationBaseBinder()
	clonedBinder.boundCols = append(clonedBinder.boundCols, boundColumn{name: "temporary"})

	require.False(t, parent.isCorrelated)
	require.Len(t, parent.times, 1)
	require.NotContains(t, parent.timeByAst, "temporary")
	require.Len(t, parent.aggregates, 1)
	require.Len(t, parent.expandedSelectLists[selectClause], 1)
	require.Len(t, cteRef.occurrences, 1)
	require.Equal(t, tree.Identifier("c"), cteRef.ast.Name.Alias)
	require.Equal(t, []boundColumn{{name: "existing"}}, havingBinder.boundCols)
}

func TestBuildMedianWithinGroupAcceptsCaseInsensitiveIdentifiers(t *testing.T) {
	for _, sql := range []string{
		"select median(a) within group (order by A) from select_test.bind_select",
		"select median(abs(a)) within group (order by ABS(a)) from select_test.bind_select",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)

			queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)

			fn := findAggregateByName(queryPlan.GetQuery(), "median")
			require.NotNil(t, fn)
			require.Len(t, fn.Args, 1)
		})
	}
}

func TestBuildMedianWithinGroupAcceptsEquivalentColumnQualifications(t *testing.T) {
	for _, sql := range []string{
		"select median(a) within group (order by bind_select.a) from select_test.bind_select",
		"select median(bind_select.a) within group (order by select_test.bind_select.a) from select_test.bind_select",
		"select median(select_test.bind_select.a) within group (order by a) from select_test.bind_select",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)

			queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)

			fn := findAggregateByName(queryPlan.GetQuery(), "median")
			require.NotNil(t, fn)
			require.Len(t, fn.Args, 1)
		})
	}
}

func TestBuildMedianWithinGroupRejectsScopedOrMismatchedQualifications(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "inner versus correlated outer column",
			sql: `select median((select a from select_test.bind_select y limit 1))
  within group (order by (select x.a from select_test.bind_select y limit 1))
  from select_test.bind_select x`,
		},
		{
			name: "wrong database qualifier",
			sql:  "select median(a) within group (order by wrong_database.bind_select.a) from select_test.bind_select",
		},
		{
			name: "both expressions use wrong database qualifier",
			sql:  "select median(wrong_database.bind_select.a) within group (order by wrong_database.bind_select.a) from select_test.bind_select",
		},
		{
			name: "different outer bindings with the same column name",
			sql: `select median((select x.a)) within group (order by (select y.a))
from select_test.bind_select x join select_test.bind_select y on x.a = y.a`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)

			_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.ErrorContains(t, err, "median requires the WITHIN GROUP ORDER BY expression to match")
		})
	}
}

func TestMedianWithinGroupValidationDoesNotLeakCTEConsumers(t *testing.T) {
	sql := `with totals as (
  select a, count(*) as n
  from select_test.bind_select
  group by a
)
select median((select max(n) from totals))
within group (order by (select max(n) from totals))
from totals x`
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	selectStmt, ok := stmt.(*tree.Select)
	require.True(t, ok)

	var bindCtx *BindContext
	_, err = bindAndOptimizeSelectQueryWithValidatorAndCapture(
		planpb.Query_SELECT,
		NewMockCompilerContext(true),
		selectStmt,
		false,
		true,
		func(*Query) error { return nil },
		func(ctx *BindContext) { bindCtx = ctx },
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, bindCtx)
	cteRef := bindCtx.cteByName["totals"]
	require.NotNil(t, cteRef)
	// Only the two scalar subqueries are real consumers.  The equality
	// validation binds cloned subqueries, but must not append those temporary
	// consumers to the shared CTE reference.
	require.Len(t, cteRef.occurrences, 2)
}

func TestBuildMedianWithinGroupRejectsWindowForm(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select median(a) within group (order by a) over () from select_test.bind_select", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.Error(t, err)
}

func TestBindMedianWithinGroupRejectsInvalidShape(t *testing.T) {
	binder := &HavingBinder{baseBinder: baseBinder{sysCtx: context.Background()}}
	for _, tc := range []struct {
		name string
		expr *tree.FuncExpr
		want string
	}{
		{
			name: "multiple value expressions",
			expr: &tree.FuncExpr{Exprs: tree.Exprs{nil, nil}},
			want: "median requires exactly one value expression",
		},
		{
			name: "missing order expression",
			expr: &tree.FuncExpr{Exprs: tree.Exprs{nil}},
			want: "median requires exactly one WITHIN GROUP ORDER BY expression",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := binder.bindMedianWithinGroupAgg(NameMedian, tc.expr, 0, false)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestBuildOrderedSetPercentileRejectsInvalidWithinGroupShape(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "missing within group",
			sql:  "select percentile_cont(0.5) from select_test.bind_select",
			want: "percentile_cont requires WITHIN GROUP",
		},
		{
			name: "multiple order expressions",
			sql:  "select percentile_cont(0.5) within group (order by a, b) from select_test.bind_select",
			want: "percentile_cont requires exactly one WITHIN GROUP ORDER BY expression",
		},
		{
			name: "null percentile",
			sql:  "select percentile_cont(null) within group (order by a) from select_test.bind_select",
			want: "percentile argument of percentile_cont must be a non-null constant",
		},
		{
			name: "non numeric order expression",
			sql:  "select percentile_cont(0.5) within group (order by n_name) from nation",
			want: "",
		},
		{
			name: "maximum width decimal continuous interpolation",
			sql:  "select percentile_cont(0.5) within group (order by cast(a as decimal(38,0))) from select_test.bind_select",
			want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(ctx, stmt, false)
			if tc.want == "" {
				require.Error(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestBuildOrderedSetPercentileRejectsWindowForm(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select percentile_cont(0.5) within group (order by a) over () from select_test.bind_select", 1)
	require.NoError(t, err)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.ErrorContains(t, err, "ordered-set percentile window functions")
}
