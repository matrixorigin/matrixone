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
