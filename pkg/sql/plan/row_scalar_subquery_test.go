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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestScalarAggregateSubqueryRefreshesConsumerNullability(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, `
		select ps.PS_PARTKEY from PARTSUPP ps
		where ps.PS_SUPPLYCOST = (
			select min(inner_ps.PS_SUPPLYCOST) from PARTSUPP inner_ps
			where inner_ps.PS_PARTKEY = ps.PS_PARTKEY)`)
	require.NoError(t, err)

	found := false
	var visit func(*planpb.Expr)
	visit = func(expr *planpb.Expr) {
		if expr == nil {
			return
		}
		if call := expr.GetF(); call != nil {
			if call.Func != nil && call.Func.ObjName == "=" && len(call.Args) == 2 &&
				types.T(call.Args[0].Typ.Id).IsDecimal() && types.T(call.Args[1].Typ.Id).IsDecimal() &&
				(!call.Args[0].Typ.NotNullable || !call.Args[1].Typ.NotNullable) {
				found = true
				require.Equal(t,
					function.DeduceNotNullable(call.Func.Obj, call.Args),
					expr.Typ.NotNullable)
				require.False(t, expr.Typ.NotNullable)
			}
			for _, arg := range call.Args {
				visit(arg)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				visit(item)
			}
		}
	}
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, expressions := range [][]*planpb.Expr{
			node.ProjectList, node.FilterList, node.OnList, node.GroupBy, node.AggList,
		} {
			for _, expr := range expressions {
				visit(expr)
			}
		}
	}
	require.True(t, found, "expected a nullable decimal equality consumer")
}

func TestScalarAggregateSubqueryRefreshPreservesIfNullContract(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, `
		select ifnull((
			select min(r.R_REGIONKEY) from REGION r
			where r.R_REGIONKEY = n.N_REGIONKEY), 0),
			ifnull((
				select min(r.R_REGIONKEY) from REGION r
				where r.R_REGIONKEY = n.N_REGIONKEY), 0) + 1,
			ifnull(cast((
				select min(r.R_REGIONKEY) from REGION r
				where r.R_REGIONKEY = n.N_REGIONKEY) as bigint), 0),
			ifnull(cast(cast((
				select min(r.R_REGIONKEY) from REGION r
				where r.R_REGIONKEY = n.N_REGIONKEY) as bigint) as decimal(20,0)), 0)
		from NATION n`)
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(logicPlan)
	require.Len(t, columns, 4)
	for _, column := range columns {
		require.True(t, column.Typ.NotNullable)
	}

	leftJoins := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_LEFT {
			leftJoins++
		}
	}
	require.Equal(t, 4, leftJoins, "each IFNULL source must be flattened once")
}

func TestRowConstructorScalarSubqueryComparisonBuilds(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "equal", sql: "select (1, 'AFRICA') = (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "not equal", sql: "select (1, 'AFRICA') <> (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "less", sql: "select (1, 'AFRICA') < (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "less equal", sql: "select (1, 'AFRICA') <= (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "greater", sql: "select (1, 'AFRICA') > (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "greater equal", sql: "select (1, 'AFRICA') >= (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "null safe equal", sql: "select (1, 'AFRICA') <=> (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "subquery on left", sql: "select (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0) < (1, 'AFRICA')"},
		{name: "constant subquery", sql: "select (1, 'AFRICA') = (select 1, 'AFRICA')"},
		{name: "nested scalar in left row", sql: "select (1, (select 'AFRICA')) = (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "nested scalar in right row", sql: "select (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0) = ((select 0), 'AFRICA')"},
		{name: "correlated", sql: `select N_NATIONKEY from NATION
			where (N_REGIONKEY, N_NAME) =
				(select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = N_REGIONKEY)`},
		{name: "correlated first result", sql: `select N_NATIONKEY from NATION n
			where (n.N_REGIONKEY, n.N_REGIONKEY) =
				(select n.N_REGIONKEY, r.R_REGIONKEY from REGION r where r.R_REGIONKEY = n.N_REGIONKEY)`},
		{name: "correlated later result", sql: `select N_NATIONKEY from NATION n
			where (n.N_REGIONKEY, n.N_REGIONKEY) =
				(select r.R_REGIONKEY, n.N_REGIONKEY from REGION r where r.R_REGIONKEY = n.N_REGIONKEY)`},
		{name: "correlated result with constant", sql: `select N_NATIONKEY from NATION n
			where (n.N_REGIONKEY, 1) =
				(select n.N_REGIONKEY, 1 from REGION r where r.R_REGIONKEY = n.N_REGIONKEY)`},
		{name: "correlated outer-only result with limit", sql: `select N_NATIONKEY from NATION n
			where (n.N_REGIONKEY, 1) =
				(select n.N_REGIONKEY, 1 from REGION r
					where r.R_REGIONKEY = n.N_REGIONKEY limit 1)`},
		{name: "correlated outer-only result with distinct", sql: `select N_NATIONKEY from NATION n
			where (n.N_REGIONKEY, 1) =
				(select distinct n.N_REGIONKEY, 1 from REGION r
					where r.R_REGIONKEY = n.N_REGIONKEY)`},
		{name: "correlated later result with limit", sql: `select N_NATIONKEY from NATION n
			where (n.N_REGIONKEY, n.N_REGIONKEY) =
				(select r.R_REGIONKEY, n.N_REGIONKEY from REGION r
					where r.R_REGIONKEY = n.N_REGIONKEY limit 1)`},
		{name: "correlated later result with distinct", sql: `select N_NATIONKEY from NATION n
			where (n.N_REGIONKEY, n.N_REGIONKEY) =
				(select distinct r.R_REGIONKEY, n.N_REGIONKEY from REGION r
					where r.R_REGIONKEY = n.N_REGIONKEY)`},
		{name: "correlated aggregate having empty", sql: `select N_NATIONKEY from NATION n
			where (0, null) <=>
				(select count(*), sum(r.R_REGIONKEY) from REGION r
					where r.R_REGIONKEY = n.N_REGIONKEY + 100 having count(*) = 0)`},
		{name: "correlated aggregate first outer result", sql: `select N_NATIONKEY from NATION n
			where (0, 1, 0) =
				(select n.N_REGIONKEY, count(*), sum(r.R_REGIONKEY) from REGION r
					where r.R_REGIONKEY = n.N_REGIONKEY)`},
		{name: "correlated aggregate later outer result", sql: `select N_NATIONKEY from NATION n
			where (1, 0, 0) =
				(select count(*), n.N_REGIONKEY, sum(r.R_REGIONKEY) from REGION r
					where r.R_REGIONKEY = n.N_REGIONKEY)`},
		{name: "correlated aggregate compared with outer row", sql: `select N_NATIONKEY from NATION n
			where (1, n.N_REGIONKEY, n.N_REGIONKEY) =
				(select count(*), sum(r.R_REGIONKEY), n.N_REGIONKEY from REGION r
					where r.R_REGIONKEY = n.N_REGIONKEY)`},
		{name: "scalar aggregate compared with outer", sql: `select N_NATIONKEY from NATION n
			where n.N_REGIONKEY =
				(select sum(r.R_REGIONKEY) from REGION r where r.R_REGIONKEY = n.N_REGIONKEY)`},
		{name: "aggregate row compared with outer", sql: `select N_NATIONKEY from NATION n
			where (1, n.N_REGIONKEY) =
				(select count(*), sum(r.R_REGIONKEY) from REGION r where r.R_REGIONKEY = n.N_REGIONKEY)`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			require.NotNil(t, logicPlan.GetQuery())
			require.False(t, queryContainsSubqueryRef(logicPlan.GetQuery()))
			assertReachablePlanHasNoCorrelatedExpr(t, logicPlan.GetQuery())
		})
	}
}

func TestRowConstructorQuantifiedSubqueryStillRejectsNestedLeftSubquery(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(false), t,
		"select ((select 0), 'AFRICA') in (select R_REGIONKEY, R_NAME from REGION)")
	require.ErrorContains(t, err, "a quantified subquery's left operand can't contain subquery")
}

func TestRowConstructorCorrelatedAggregateUnsupportedWrapperFailsClosed(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(false), t, `select N_NATIONKEY from NATION n
		where (0, null) <=>
			(select count(*), sum(r.R_REGIONKEY) from REGION r
				where r.R_REGIONKEY = n.N_REGIONKEY limit 0)`)
	require.ErrorContains(t, err, "correlated aggregate row result cannot be safely decorrelated")
}

func TestRowConstructorNonEqAggregateMasksDistinctLiteral(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, `select n.N_NATIONKEY
		from NATION n where (1,2) <=>
		(select count(distinct 1),sum(1) from REGION r where r.R_REGIONKEY<n.N_REGIONKEY)`)
	require.NoError(t, err)
	masked := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType != planpb.Node_AGG {
			continue
		}
		for _, agg := range node.AggList {
			f := agg.GetF()
			if f == nil || f.Func == nil || len(f.Args) != 1 {
				continue
			}
			arg := f.Args[0]
			caseFn := arg.GetF()
			if caseFn == nil || caseFn.Func.ObjName != "case" {
				continue
			}
			require.False(t, arg.Typ.NotNullable)
			markerTest := caseFn.Args[0].GetF()
			require.NotNil(t, markerTest)
			require.Equal(t, "isnull", markerTest.Func.ObjName)
			require.False(t, markerTest.Args[0].Typ.NotNullable)
			if f.Func.ObjName == "count" {
				require.NotZero(t, uint64(f.Func.Obj)&function.Distinct)
			}
			masked++
		}
	}
	require.Equal(t, 2, masked, "both unary aggregates must ignore synthetic rows")
}

func TestRowConstructorNonEqAggregateRejectsUnsafeComposition(t *testing.T) {
	for _, test := range []struct{ sql, want string }{
		{`select n.N_NATIONKEY from NATION n where (0,null) <=>
			(select count(*),sum(r.R_REGIONKEY) from REGION r
			 where r.R_REGIONKEY<n.N_REGIONKEY limit 0)`, "pagination in non-equality correlated aggregate"},
		{`select n.N_NATIONKEY from NATION n where (0,null) <=>
			(select sum(coalesce(r.R_REGIONKEY,0)),sum(r.R_REGIONKEY) from REGION r
			 where r.R_REGIONKEY<n.N_REGIONKEY)`, "unsupported non-equality correlated aggregate input"},
		{`select n.N_NATIONKEY, (n.N_REGIONKEY,(select R_REGIONKEY from REGION where R_REGIONKEY=1)) =
			(select count(*),sum(r.R_REGIONKEY) from REGION r where r.R_REGIONKEY<n.N_REGIONKEY)
			from NATION n`, "outer composition cannot be safely decorrelated"},
	} {
		_, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
		require.ErrorContains(t, err, test.want)
	}
}

func TestNonEqAggregateFallbackRejectsBypassedBoundariesBeforeAppend(t *testing.T) {
	intType := planpb.Type{Id: int32(types.T_int32)}
	rowIDType := planpb.Type{Id: int32(types.T_Rowid), Width: 16, NotNullable: true}
	newScan := func(tag int32) *planpb.Node {
		return &planpb.Node{
			NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{tag},
			TableDef: &planpb.TableDef{
				Name2ColIndex: map[string]int32{catalog.Row_ID: 1},
				Cols: []*planpb.ColDef{
					{Name: "k", Typ: intType},
					{Name: catalog.Row_ID, Typ: rowIDType, Hidden: true},
				},
			},
		}
	}
	newBuilder := func() (*QueryBuilder, *BindContext, *BindContext) {
		outerBinding := &Binding{tag: 10, cols: []string{"k", catalog.Row_ID},
			colIsHidden: []bool{false, true}, types: []*planpb.Type{&intType, &rowIDType}}
		innerBinding := &Binding{tag: 20, cols: []string{"k", catalog.Row_ID},
			colIsHidden: []bool{false, true}, types: []*planpb.Type{&intType, &rowIDType}}
		builder := &QueryBuilder{
			compCtx: NewMockCompilerContext(true),
			qry: &planpb.Query{Nodes: []*planpb.Node{
				newScan(10), newScan(20),
				{NodeType: planpb.Node_AGG, Children: []int32{1}, BindingTags: []int32{30, 31},
					AggList: []*planpb.Expr{makePlan2Int64ConstExprWithType(1)}},
				{NodeType: planpb.Node_PROJECT, Children: []int32{2}, BindingTags: []int32{32},
					ProjectList: []*planpb.Expr{GetColExpr(intType, 31, 0)}},
			}},
		}
		return builder, &BindContext{bindings: []*Binding{innerBinding}, aggregateTag: 31},
			&BindContext{bindings: []*Binding{outerBinding}}
	}
	for _, test := range []struct {
		name, node string
		set        func(*planpb.Node)
	}{
		{"project limit", "project", func(n *planpb.Node) { n.Limit = makePlan2Uint64ConstExprWithType(0) }},
		{"project offset", "project", func(n *planpb.Node) { n.Offset = makePlan2Uint64ConstExprWithType(1) }},
		{"project rank", "project", func(n *planpb.Node) { n.RankOption = &planpb.RankOption{Mode: "force"} }},
		{"aggregate limit", "aggregate", func(n *planpb.Node) { n.Limit = makePlan2Uint64ConstExprWithType(0) }},
		{"aggregate offset", "aggregate", func(n *planpb.Node) { n.Offset = makePlan2Uint64ConstExprWithType(1) }},
		{"aggregate rank", "aggregate", func(n *planpb.Node) { n.RankOption = &planpb.RankOption{Mode: "force"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder, subCtx, ctx := newBuilder()
			index := 3
			if test.node == "aggregate" {
				index = 2
			}
			test.set(builder.qry.Nodes[index])
			before := len(builder.qry.Nodes)
			_, _, err := builder.flattenScalarSubqueryWithNonEqAgg(0, 3, subCtx, nil, ctx,
				&planpb.SubqueryRef{Typ: planpb.SubqueryRef_SCALAR})
			require.ErrorContains(t, err, "pagination in non-equality correlated aggregate")
			require.Len(t, builder.qry.Nodes, before)
		})
	}
	for _, test := range []struct {
		name  string
		outer bool
	}{
		{"outer project", true}, {"inner project", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder, subCtx, ctx := newBuilder()
			child := int32(1)
			if test.outer {
				child = 0
			}
			builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
				NodeType: planpb.Node_PROJECT, Children: []int32{child}, BindingTags: []int32{40},
			})
			outerID := int32(0)
			if test.outer {
				outerID = 4
			} else {
				builder.qry.Nodes[2].Children[0] = 4
			}
			before := len(builder.qry.Nodes)
			_, _, err := builder.flattenScalarSubqueryWithNonEqAgg(outerID, 3, subCtx, nil, ctx,
				&planpb.SubqueryRef{Typ: planpb.SubqueryRef_SCALAR})
			if test.outer {
				require.ErrorContains(t, err, "outer composition cannot be safely decorrelated")
			} else {
				require.ErrorContains(t, err, "accessible inner row marker")
			}
			require.Len(t, builder.qry.Nodes, before)
		})
	}
}

func TestRowConstructorCorrelatedVolatileProjectionFailsClosed(t *testing.T) {
	for _, sql := range []string{
		`select N_NATIONKEY from NATION n where (n.N_REGIONKEY, 1) =
			(select n.N_REGIONKEY, nextval('row_scalar_seq') from REGION r
			 where r.R_REGIONKEY = n.N_REGIONKEY)`,
		`select N_NATIONKEY from NATION n where (n.N_REGIONKEY, 1) =
			(select distinct n.N_REGIONKEY, nextval('row_scalar_seq') from REGION r
			 where r.R_REGIONKEY = n.N_REGIONKEY)`,
		`select N_NATIONKEY from NATION n where (n.N_REGIONKEY, 1) =
			(select n.N_REGIONKEY, nextval('row_scalar_seq') from REGION r
			 where r.R_REGIONKEY = n.N_REGIONKEY limit 1)`,
	} {
		_, err := runOneStmt(NewMockOptimizer(false), t, sql)
		require.ErrorContains(t, err, "wrapped correlated scalar projection cannot be safely decorrelated")
	}
}

func TestRowConstructorScalarOrderingDoesNotDuplicateVolatileFields(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{"row on left", "select (nextval('row_cmp_seq'), 0) < (select 1, 1)"},
		{"row on right", "select (select 1, 1) > (nextval('row_cmp_seq'), 0)"},
		{"subquery projection", "select (1, 0) < (select nextval('row_cmp_seq'), 1)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.ErrorContains(t, err, "volatile row ordering comparison")
		})
	}

	_, err := runOneStmt(NewMockOptimizer(false), t,
		"select (1, nextval('row_cmp_seq')) < (select 1, 2)")
	require.NoError(t, err, "the final field appears only once in the comparison")
	for _, sql := range []string{
		"select (nextval('row_cmp_seq'), 0) < any (select 1, 1)",
		"select (nextval('row_cmp_seq'), 0) < all (select R_REGIONKEY, 1 from REGION)",
	} {
		_, err = runOneStmt(NewMockOptimizer(false), t, sql)
		require.NoError(t, err, "quantified row comparisons retain their existing planning path")
	}
}

func TestRowConstructorCorrelatedAggregateVolatileHavingFailsClosed(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(false), t, `select N_NATIONKEY from NATION n
		where (0, null) <=>
			(select count(*), sum(r.R_REGIONKEY) from REGION r
			 where r.R_REGIONKEY = n.N_REGIONKEY + 100
			 having nextval('row_having_seq') = 1)`)
	require.ErrorContains(t, err, "volatile correlated scalar HAVING")

	_, err = runOneStmt(NewMockOptimizer(false), t, `select (select count(*) from REGION r
		where r.R_REGIONKEY = n.N_REGIONKEY having nextval('row_having_seq') = 1)
		from NATION n`)
	require.NoError(t, err, "ordinary one-column scalar HAVING keeps its existing plan")
}

func TestRowConstructorScalarSubqueryComparisonRejectsArityMismatch(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(false), t,
		"select (1, 2) = (select R_REGIONKEY, R_NAME, R_COMMENT from REGION where R_REGIONKEY = 0)")
	require.ErrorContains(t, err, "subquery should return 2 columns")
}

func queryContainsSubqueryRef(query *planpb.Query) bool {
	for _, node := range query.Nodes {
		for _, exprs := range [][]*planpb.Expr{
			node.ProjectList,
			node.FilterList,
			node.OnList,
			node.GroupBy,
			node.AggList,
		} {
			for _, expr := range exprs {
				if hasSubquery(expr) {
					return true
				}
			}
		}
	}
	return false
}
