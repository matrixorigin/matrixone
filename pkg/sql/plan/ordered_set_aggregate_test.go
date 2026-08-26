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

	"github.com/matrixorigin/matrixone/pkg/container/types"
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

type medianLowerCaseTableNamesContext struct {
	*MockCompilerContext
	lower           int64
	defaultDatabase string
}

func (c *medianLowerCaseTableNamesContext) GetLowerCaseTableNames() int64 {
	return c.lower
}

func (c *medianLowerCaseTableNamesContext) DefaultDatabase() string {
	if c.defaultDatabase != "" {
		return c.defaultDatabase
	}
	return c.MockCompilerContext.DefaultDatabase()
}

func TestMedianSetOrderReferenceMarkerPreservesCaseSensitiveKeys(t *testing.T) {
	upper := medianSetOrderReferenceMarker("A").(*tree.UnresolvedName).ColName()
	lower := medianSetOrderReferenceMarker("a").(*tree.UnresolvedName).ColName()
	require.NotEqual(t, upper, lower)
	require.Equal(t, strings.ToLower(upper), upper)
	require.Equal(t, strings.ToLower(lower), lower)
}

func TestMedianVariableCanonicalizationIsCaseInsensitiveAndScopeAware(t *testing.T) {
	userLower, valid := canonicalMedianWithinGroupAstKey(nil, nil,
		tree.NewVarExpr("review_var", false, false, nil))
	require.True(t, valid)
	userUpper, valid := canonicalMedianWithinGroupAstKey(nil, nil,
		tree.NewVarExpr("REVIEW_VAR", false, false, nil))
	require.True(t, valid)
	session, valid := canonicalMedianWithinGroupAstKey(nil, nil,
		tree.NewVarExpr("REVIEW_VAR", true, false, nil))
	require.True(t, valid)
	global, valid := canonicalMedianWithinGroupAstKey(nil, nil,
		tree.NewVarExpr("review_var", true, true, nil))
	require.True(t, valid)

	require.Equal(t, userLower, userUpper)
	require.NotEqual(t, userLower, session)
	require.NotEqual(t, session, global)
}

func TestBuildMedianWithinGroupCanonicalizesVariableNames(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	fallback := NewMockCompilerContext(true)
	ctx.ResolveVariableFunc = func(name string, system, global bool) (any, error) {
		if !system && strings.EqualFold(name, "review_var") {
			return int64(1), nil
		}
		return fallback.ResolveVariable(name, system, global)
	}
	ctx.ResolveVariableTypeFunc = func(name string, system, global bool) (Type, error) {
		if !system && strings.EqualFold(name, "review_var") {
			return makeSimplePlan2Type(types.T_int64), nil
		}
		return fallback.ResolveVariableType(name, system, global)
	}

	for _, sql := range []string{
		"select median(@review_var) within group (order by @REVIEW_VAR) from select_test.bind_select",
		"select median(cast(@@session.auto_increment_offset as signed)) within group " +
			"(order by cast(@@SESSION.AUTO_INCREMENT_OFFSET as signed)) from select_test.bind_select",
	} {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		_, err = BuildPlan(ctx, stmt, false)
		stmt.Free()
		require.NoError(t, err)
	}

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select median(cast(@@session.auto_increment_offset as signed)) within group "+
			"(order by cast(@@global.auto_increment_offset as signed)) from select_test.bind_select", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, "median requires the WITHIN GROUP ORDER BY expression to match")
}

func TestBuildMedianWithinGroupRespectsLowerCaseTableNames(t *testing.T) {
	const sameCase = `select median((select a from select_test.bind_select limit 1))
  within group (order by (select a from select_test.bind_select limit 1))
  from select_test.bind_select`
	const mixedCase = `select median((select a from select_test.bind_select limit 1))
  within group (order by (select a from SELECT_TEST.BIND_SELECT limit 1))
  from select_test.bind_select`
	const systemSchemaMixedCase = `select median((select version from information_schema.tables limit 1))
  within group (order by (select version from INFORMATION_SCHEMA.TABLES limit 1))
  from select_test.bind_select`
	const defaultInformationSchemaMixedCase = `select median((select version from tables limit 1))
  within group (order by (select version from TABLES limit 1))
  from select_test.bind_select`
	const defaultMySQLMixedCase = `select median((select a from bind_select limit 1))
  within group (order by (select a from BIND_SELECT limit 1))
  from select_test.bind_select`

	for _, test := range []struct {
		name            string
		lower           int64
		defaultDatabase string
		sql             string
		wantErr         bool
	}{
		{name: "mode 0 same spelling", lower: 0, sql: sameCase},
		{name: "mode 0 preserves case", lower: 0, sql: mixedCase, wantErr: true},
		{name: "mode 0 folds compatibility schemas", lower: 0, sql: systemSchemaMixedCase},
		{name: "mode 0 folds unqualified information schema tables", lower: 0, defaultDatabase: "information_schema", sql: defaultInformationSchemaMixedCase},
		{name: "mode 0 folds unqualified mysql tables", lower: 0, defaultDatabase: "mysql", sql: defaultMySQLMixedCase},
		{name: "mode 1 folds case", lower: 1, sql: mixedCase},
		{name: "mode 2 compares case insensitively", lower: 2, sql: mixedCase},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := &medianLowerCaseTableNamesContext{
				MockCompilerContext: NewMockCompilerContext(true),
				lower:               test.lower,
				defaultDatabase:     test.defaultDatabase,
			}
			ctx.dbs["mysql"] = true
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, test.sql, test.lower)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			_, err = BuildPlan(ctx, stmt, false)
			if test.wantErr {
				require.ErrorContains(t, err, "median requires the WITHIN GROUP ORDER BY expression to match")
				return
			}
			require.NoError(t, err)
		})
	}

	for _, test := range []struct {
		name            string
		defaultDatabase string
		queries         []string
	}{
		{
			name:            "information schema ordinary controls",
			defaultDatabase: "information_schema",
			queries: []string{
				"select median((select version from tables limit 1)) from select_test.bind_select",
				"select median((select version from TABLES limit 1)) from select_test.bind_select",
			},
		},
		{
			name:            "mysql ordinary controls",
			defaultDatabase: "mysql",
			queries: []string{
				"select median((select a from bind_select limit 1)) from select_test.bind_select",
				"select median((select a from BIND_SELECT limit 1)) from select_test.bind_select",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := &medianLowerCaseTableNamesContext{
				MockCompilerContext: NewMockCompilerContext(true),
				lower:               0,
				defaultDatabase:     test.defaultDatabase,
			}
			ctx.dbs["mysql"] = true
			for _, sql := range test.queries {
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 0)
				require.NoError(t, err)
				_, err = BuildPlan(ctx, stmt, false)
				stmt.Free()
				require.NoError(t, err)
			}
		})
	}

	ctx := &medianLowerCaseTableNamesContext{
		MockCompilerContext: NewMockCompilerContext(true),
		lower:               2,
	}
	for _, sql := range []string{
		"select median((select a from select_test.bind_select limit 1)) from select_test.bind_select",
		"select median((select a from SELECT_TEST.BIND_SELECT limit 1)) from select_test.bind_select",
	} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 2)
		require.NoError(t, err)
		_, err = BuildPlan(ctx, stmt, false)
		stmt.Free()
		require.NoError(t, err)
	}
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
		`select median((select a as lhs from select_test.bind_select order by lhs limit 1))
  within group (order by (select a from select_test.bind_select order by a limit 1))
  from select_test.bind_select x`,
		`select median((select a as lhs from select_test.bind_select order by lhs limit 1))
  within group (order by (select a from select_test.bind_select order by 1 limit 1))
  from select_test.bind_select x`,
		`select median((select a + 1 as lhs from select_test.bind_select order by lhs limit 1))
  within group (order by (select a + 1 from select_test.bind_select order by a + 1 limit 1))
  from select_test.bind_select x`,
		`select median((select b as a from select_test.bind_select order by a + 1 limit 1))
  within group (order by (select b as rhs from select_test.bind_select order by a + 1 limit 1))
  from select_test.bind_select x`,
		`select median((select a as lhs from select_test.bind_select group by lhs limit 1))
  within group (order by (select a from select_test.bind_select group by a limit 1))
  from select_test.bind_select x`,
		`select median((select a as lhs from select_test.bind_select group by lhs limit 1))
  within group (order by (select a from select_test.bind_select group by 1 limit 1))
  from select_test.bind_select x`,
		`select median((select max(a) as lhs from select_test.bind_select having lhs > 0))
  within group (order by (select max(a) from select_test.bind_select having max(a) > 0))
  from select_test.bind_select x`,
		`select median((select lhs.a from (select a from select_test.bind_select) lhs(a) limit 1))
  within group (order by (select rhs.b from (select a from select_test.bind_select) rhs(b) limit 1))
  from select_test.bind_select x`,
		`select median((with lhs as (select a from select_test.bind_select)
    select a from lhs limit 1))
  within group (order by (with rhs as (select a from select_test.bind_select)
    select a from rhs limit 1))
  from select_test.bind_select x`,
		`select median((with c as (
    select l.a from select_test.bind_select l
  ) select a from c limit 1))
  within group (order by (with d as (
    select r.a from select_test.bind_select r
  ) select a from d limit 1))
  from select_test.bind_select`,
		`select median((
    select d.a from (select l.a from select_test.bind_select l) d limit 1
  )) within group (order by (
    select e.a from (select r.a from select_test.bind_select r) e limit 1
  )) from select_test.bind_select`,
		`select median((with unused_left as (
    select l.a from select_test.bind_select l
  ) select 1))
  within group (order by (with unused_right as (
    select r.a from select_test.bind_select r
  ) select 1))
  from select_test.bind_select`,
		`select median((with unused as (
    select a from select_test.bind_select
  ) select 1))
  within group (order by (select 1))
  from select_test.bind_select`,
		`select median((with recursive unused as (
    select a from select_test.bind_select
  ) select 1))
  within group (order by (select 1))
  from select_test.bind_select`,
		`select median((
    select l.a from select_test.bind_select l
    union all
    select l2.a from select_test.bind_select l2
    limit 1
  )) within group (order by (
    select r.a from select_test.bind_select r
    union all
    select r2.a from select_test.bind_select r2
    limit 1
  )) from select_test.bind_select`,
		`select median((
    select a as lhs from select_test.bind_select
    union all
    select a from select_test.bind_select
    order by lhs limit 1
  )) within group (order by (
    select a as rhs from select_test.bind_select
    union all
    select a from select_test.bind_select
    order by rhs limit 1
  )) from select_test.bind_select`,
		`select median((
    select a as lhs from select_test.bind_select
    union all
    select a from select_test.bind_select
    order by lhs limit 1
  )) within group (order by (
    select a from select_test.bind_select
    union all
    select a from select_test.bind_select
    order by a limit 1
  )) from select_test.bind_select`,
		`select median((
    select a as lhs from select_test.bind_select
    union all
    select a from select_test.bind_select
    order by lhs + 1 limit 1
  )) within group (order by (
    select a from select_test.bind_select
    union all
    select a from select_test.bind_select
    order by a + 1 limit 1
  )) from select_test.bind_select`,
		`select median((
    select a as lhs from select_test.bind_select
    union all
    select a from select_test.bind_select
    order by lhs limit 1
  )) within group (order by (
    select a from select_test.bind_select
    union all
    select a from select_test.bind_select
    order by 1 limit 1
  )) from select_test.bind_select`,
		`select median((with recursive c(n) as (
    select 1 union all select n + 1 from c where n < 2
  ) select max(n) from c))
  within group (order by (with recursive d(m) as (
    select 1 union all select m + 1 from d where m < 2
  ) select max(m) from d))
  from select_test.bind_select`,
		`select median((with recursive unused_left as (
    select a from select_test.bind_select
  ), c(n) as (
    select 1 union all select n + 1 from c where n < 2
  ) select max(n) from c))
  within group (order by (with recursive unused_right as (
    select a from select_test.bind_select
  ), d(m) as (
    select 1 union all select m + 1 from d where m < 2
  ) select max(m) from d))
  from select_test.bind_select`,
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
		nodes, nextBindTag                          int
		groups, aggregates, projects, results       int
		windows, times, whereFilters, boundCTEs     int
		views, expandedSelectLists, binderBoundCols int
		boundCols                                   []boundColumn
	}
	capture := func(sql string) parentState {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		selectStmt, ok := stmt.(*tree.Select)
		require.True(t, ok)
		mock := NewMockCompilerContext(true)
		mock.SetSqlModeOverride("ONLY_FULL_GROUP_BY")

		var state parentState
		_, err = bindAndOptimizeSelectQueryWithValidatorAndCapture(
			planpb.Query_SELECT,
			mock,
			selectStmt,
			false,
			true,
			func(*Query) error { return nil },
			func(ctx *BindContext) {
				state.groups = len(ctx.groups)
				state.aggregates = len(ctx.aggregates)
				state.projects = len(ctx.projects)
				state.results = len(ctx.results)
				state.windows = len(ctx.windows)
				state.times = len(ctx.times)
				state.whereFilters = len(ctx.whereFilters)
				state.boundCTEs = len(ctx.boundCtes)
				state.views = len(ctx.views)
				state.expandedSelectLists = len(ctx.expandedSelectLists)
				if provider, ok := ctx.binder.(interface{ medianValidationBaseBinder() *baseBinder }); ok {
					base := provider.medianValidationBaseBinder()
					state.boundCols = append([]boundColumn(nil), base.boundCols...)
					state.binderBoundCols = len(base.boundCols)
					state.nodes = len(base.builder.qry.Nodes)
					state.nextBindTag = int(base.builder.nextBindTag)
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
	parent.groups = []*planpb.Expr{{Typ: planpb.Type{Id: 5}}}
	parent.timeAsts = []tree.Expr{tree.NewUnresolvedColName("time_source")}
	parent.orderResolution = &orderResolutionMetadata{
		bindAsts: []tree.Expr{tree.NewUnresolvedColName("order_source")},
	}
	selectClause := &tree.SelectClause{}
	parent.expandedSelectLists = map[*tree.SelectClause]tree.SelectExprs{
		selectClause: {{Expr: tree.NewNumVal(int64(1), "1", false, tree.P_int64)}},
	}
	binding := &Binding{
		tag:         7,
		nodeId:      0,
		cols:        []string{"a"},
		types:       []*planpb.Type{{Id: 6}},
		refCnts:     []uint{1},
		colIdByName: map[string]int32{"a": 0},
	}
	parent.bindings = []*Binding{binding}
	parent.bindingByTag[7] = binding
	parent.bindingByTable["t"] = binding
	parent.bindingByCol["a"] = binding
	cteRef := &CTERef{
		ast:         &tree.CTE{Name: &tree.AliasClause{Alias: tree.Identifier("c")}},
		occurrences: []cteOccurrence{{rootID: 1}},
	}
	parent.cteByName = map[string]*CTERef{"c": cteRef}
	havingBinder := NewHavingBinder(builder, parent)
	havingBinder.boundCols = []boundColumn{{name: "existing"}}
	parent.binder = NewProjectionBinder(builder, parent, havingBinder)

	isolated := cloneMedianBindScope(parent, builder)
	require.NotSame(t, parent, isolated)
	require.NotSame(t, parent.binder, isolated.binder)
	clonedProjection := isolated.binder.(*ProjectionBinder)
	require.NotSame(t, havingBinder, clonedProjection.havingBinder)
	require.NotSame(t, binding, isolated.bindingByTag[7])
	require.Same(t, isolated.bindingByTag[7], isolated.bindingByTable["t"])
	require.Same(t, isolated.bindingByTag[7], isolated.bindingByCol["a"])
	require.NotSame(t, cteRef, isolated.cteByName["c"])
	require.NotSame(t, cteRef.ast, isolated.cteByName["c"].ast)
	require.NotSame(t, parent.timeAsts[0], isolated.timeAsts[0])
	require.NotSame(t, parent.orderResolution.bindAsts[0], isolated.orderResolution.bindAsts[0])
	var isolatedSelectClause *tree.SelectClause
	for key := range isolated.expandedSelectLists {
		isolatedSelectClause = key
	}
	require.NotNil(t, isolatedSelectClause)
	require.NotSame(t, selectClause, isolatedSelectClause)

	isolated.isCorrelated = true
	isolated.times = append(isolated.times, &planpb.Expr{Typ: planpb.Type{Id: 3}})
	isolated.groups[0].Typ.Id = 50
	isolated.timeByAst["temporary"] = 1
	isolated.aggregates = append(isolated.aggregates, &planpb.Expr{Typ: planpb.Type{Id: 4}})
	isolated.expandedSelectLists[isolatedSelectClause] = append(
		isolated.expandedSelectLists[isolatedSelectClause],
		tree.SelectExpr{Expr: tree.NewNumVal(int64(2), "2", false, tree.P_int64)},
	)
	isolated.bindingByTag[7].cols[0] = "temporary"
	isolated.bindingByTag[7].types[0].Id = 60
	isolated.bindingByTag[7].refCnts[0] = 2
	isolated.cteByName["c"].occurrences = append(
		isolated.cteByName["c"].occurrences,
		cteOccurrence{rootID: 2},
	)
	isolated.cteByName["c"].ast.Name.Alias = tree.Identifier("temporary")
	clonedBinder := isolated.binder.(interface{ medianValidationBaseBinder() *baseBinder }).medianValidationBaseBinder()
	clonedBinder.boundCols = append(clonedBinder.boundCols, boundColumn{name: "temporary"})
	clonedProjection.havingBinder.boundCols = append(
		clonedProjection.havingBinder.boundCols,
		boundColumn{name: "temporary having"},
	)

	require.False(t, parent.isCorrelated)
	require.Len(t, parent.times, 1)
	require.Equal(t, int32(5), parent.groups[0].Typ.Id)
	require.NotContains(t, parent.timeByAst, "temporary")
	require.Len(t, parent.aggregates, 1)
	require.Len(t, parent.expandedSelectLists[selectClause], 1)
	require.Equal(t, "a", binding.cols[0])
	require.Equal(t, int32(6), binding.types[0].Id)
	require.Equal(t, uint(1), binding.refCnts[0])
	require.Len(t, cteRef.occurrences, 1)
	require.Equal(t, tree.Identifier("c"), cteRef.ast.Name.Alias)
	require.Equal(t, []boundColumn{{name: "existing"}}, havingBinder.boundCols)
}

func TestMedianValidationBuilderPreservesDetachedParentMetadata(t *testing.T) {
	parent := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	parent.qry.Nodes = []*planpb.Node{{
		NodeId:   0,
		NodeType: planpb.Node_TABLE_SCAN,
		TableDef: &planpb.TableDef{
			Name:          "t",
			Name2ColIndex: map[string]int32{"id": 0},
			Pkey: &planpb.PrimaryKeyDef{
				PkeyColName: "id",
				Names:       []string{"id"},
			},
		},
	}}
	parent.ctxByNode = []*BindContext{nil}
	parent.tag2NodeID[4] = 0
	parent.tag2Table[4] = parent.qry.Nodes[0].TableDef
	parentCtx := NewBindContext(parent, nil)
	binding := &Binding{
		tag:         4,
		nodeId:      0,
		cols:        []string{"id", "value"},
		colIdByName: map[string]int32{"id": 0, "value": 1},
	}
	parentCtx.bindings = []*Binding{binding}
	parentCtx.bindingByTag[4] = binding
	parentCtx.bindingByCol["id"] = binding
	parentCtx.bindingByCol["value"] = binding
	parentCtx.groups = []*planpb.Expr{{
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 4, ColPos: 0}},
	}}

	validation := newMedianValidationBuilder(parent)
	isolatedParent := cloneMedianBindScope(parentCtx, validation)
	require.Len(t, validation.qry.Nodes, 1)
	require.NotSame(t, parent.qry.Nodes[0], validation.qry.Nodes[0])
	require.NotSame(t, parent.qry.Nodes[0].TableDef, validation.qry.Nodes[0].TableDef)
	require.NotSame(t, parent.tag2Table[4], validation.tag2Table[4])
	require.Equal(t, int32(0), validation.tag2NodeID[4])
	require.True(t, validation.groupByIncludesPrimaryKey(isolatedParent, isolatedParent.bindingByTag[4]))

	validation.qry.Nodes[0].TableDef.Name = "temporary"
	validation.qry.Nodes[0].TableDef.Name2ColIndex["temporary"] = 1
	validation.tag2Table[4].Name = "temporary"
	validation.tag2Table[4].Name2ColIndex["temporary"] = 1
	require.Equal(t, "t", parent.qry.Nodes[0].TableDef.Name)
	require.NotContains(t, parent.qry.Nodes[0].TableDef.Name2ColIndex, "temporary")
	require.Equal(t, "t", parent.tag2Table[4].Name)
	require.NotContains(t, parent.tag2Table[4].Name2ColIndex, "temporary")
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

func TestBuildMedianWithinGroupAcceptsParenthesizedPredicateExpressions(t *testing.T) {
	for _, sql := range []string{
		"select median(cast((a) is true as signed)) from select_test.bind_select",
		"select median(cast(a is true as signed)) from select_test.bind_select",
		"select median(cast((a) is true as signed)) within group " +
			"(order by cast(a is true as signed)) from select_test.bind_select",
		"select median(cast(a is true as signed)) within group " +
			"(order by cast((a) is true as signed)) from select_test.bind_select",
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
		{
			name: "result alias marker cannot collide with an input column",
			sql: `select median((
  select a as lhs
  from (select a, b as __mo_result_0 from select_test.bind_select) d
  order by lhs limit 1
)) within group (order by (
  select a as rhs
  from (select a, b as __mo_result_0 from select_test.bind_select) d
  order by __mo_result_0 limit 1
)) from select_test.bind_select`,
		},
		{
			name: "top-level result alias takes precedence over source column",
			sql: `select median((
  select b as a from select_test.bind_select order by a limit 1
)) within group (order by (
  select b as rhs from select_test.bind_select order by bind_select.a limit 1
)) from select_test.bind_select`,
		},
		{
			name: "UNION compound result expressions remain semantic",
			sql: `select median((
  select a as lhs from select_test.bind_select
  union all
  select a from select_test.bind_select
  order by lhs + 1 limit 1
)) within group (order by (
  select a from select_test.bind_select
  union all
  select a from select_test.bind_select
  order by a + 2 limit 1
)) from select_test.bind_select`,
		},
		{
			name: "CTE local alias versus correlated outer alias",
			sql: `select median((with c as (
  select l.a from select_test.bind_select l
) select a from c limit 1)) within group (order by (with d as (
  select l.a
) select a from d limit 1))
from select_test.bind_select l`,
		},
		{
			name: "recursive CTE bodies remain semantic",
			sql: `select median((with recursive c(n) as (
  select 1 union all select n + 1 from c where n < 2
) select max(n) from c)) within group (order by (with recursive d(m) as (
  select 1 union all select m + 2 from d where m < 2
) select max(m) from d))
from select_test.bind_select`,
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

func TestBuildMedianWithinGroupPreservesHavingAliasPrecedence(t *testing.T) {
	left := `select median((
  select a + 1 as a from select_test.bind_select
  where a = 1 having a = 0 limit 1
)) from select_test.bind_select`
	right := `select median((
  select a + 1 from select_test.bind_select
  where a = 1 having a = 0 limit 1
)) from select_test.bind_select`
	combined := `select median((
  select a + 1 as a from select_test.bind_select
  where a = 1 having a = 0 limit 1
)) within group (order by (
  select a + 1 from select_test.bind_select
  where a = 1 having a = 0 limit 1
)) from select_test.bind_select`

	for _, sql := range []string{left, right} {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		mock := NewMockCompilerContext(true)
		mock.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
		_, err = BuildPlan(mock, stmt, false)
		stmt.Free()
		require.NoError(t, err)
	}

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, combined, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	mock := NewMockCompilerContext(true)
	mock.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err = BuildPlan(mock, stmt, false)
	require.ErrorContains(t, err, "median requires the WITHIN GROUP ORDER BY expression to match")
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
