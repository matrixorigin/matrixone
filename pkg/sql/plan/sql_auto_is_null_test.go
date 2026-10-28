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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// The durable contract of rewriteSqlAutoIsNull, pinned to MySQL's own
// regression results (mysql-test/suite/engines/funcs/r/ai_sql_auto_is_null.result
// and mysql-test/r/odbc.result): only the whole-clause
// `WHERE <AUTO_INCREMENT column> IS NULL` form is rewritten into a comparison
// with last_insert_id(); composite predicates, HAVING, the select list and
// LEFT-JOIN null-extended references keep plain IS NULL semantics.

// newSqlAutoIsNullTestContext overrides only sql_auto_is_null resolution and
// delegates every other variable back to the stock mock behavior.
func newSqlAutoIsNullTestContext(t *testing.T, value interface{}, resolveErr error) *MockCompilerContext {
	t.Helper()
	base := NewMockCompilerContext(false, newPlanTestProcess(t))
	cc := NewMockCompilerContext(false, newPlanTestProcess(t))
	cc.ResolveVariableFunc = func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if name == sqlAutoIsNullVariableName {
			return value, resolveErr
		}
		return base.ResolveVariable(name, isSystemVar, isGlobalVar)
	}
	return cc
}

func buildSqlAutoIsNullQuery(t *testing.T, cc *MockCompilerContext, sql string) *plan.Query {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(cc, stmt, false)
	require.NoError(t, err)
	require.NotNil(t, p.GetQuery())
	return p.GetQuery()
}

// sqlAutoIsNullQueryHasFunction reports whether the built query contains the
// named built-in inside any bound filter, having or projection expression.
func sqlAutoIsNullQueryHasFunction(qry *plan.Query, name string) bool {
	var walk func(e *plan.Expr) bool
	walk = func(e *plan.Expr) bool {
		if e == nil {
			return false
		}
		if fn := e.GetF(); fn != nil {
			if fn.Func != nil && fn.Func.ObjName == name {
				return true
			}
			for _, arg := range fn.Args {
				if walk(arg) {
					return true
				}
			}
		}
		if list := e.GetList(); list != nil {
			for _, item := range list.List {
				if walk(item) {
					return true
				}
			}
		}
		return false
	}
	for _, node := range qry.Nodes {
		for _, e := range node.FilterList {
			if walk(e) {
				return true
			}
		}
		for _, e := range node.ProjectList {
			if walk(e) {
				return true
			}
		}
	}
	return false
}

func sqlAutoIsNullAnyNodeNotCacheable(qry *plan.Query) bool {
	for _, node := range qry.Nodes {
		if node.GetNotCacheable() {
			return true
		}
	}
	return false
}

func TestSqlAutoIsNullEnabledInterpretation(t *testing.T) {
	require.True(t, sqlAutoIsNullEnabled(nil), "nil resolves to the registered default 1")
	require.True(t, sqlAutoIsNullEnabled(int64(1)))
	require.True(t, sqlAutoIsNullEnabled(int8(1)))
	require.True(t, sqlAutoIsNullEnabled(true))
	require.True(t, sqlAutoIsNullEnabled("ON"))
	require.False(t, sqlAutoIsNullEnabled(int64(0)))
	require.False(t, sqlAutoIsNullEnabled(false))
	require.False(t, sqlAutoIsNullEnabled("0"))
	require.False(t, sqlAutoIsNullEnabled("weird"))
	require.False(t, sqlAutoIsNullEnabled(struct{}{}))
}

func TestSqlAutoIsNullRewritesWholeWhereClause(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc, "select seq, val from auto_t where seq is null")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"whole-clause IS NULL on the AUTO_INCREMENT column must compare against last_insert_id()")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"),
		"the IS NULL predicate must not survive the rewrite")
	require.True(t, sqlAutoIsNullAnyNodeNotCacheable(qry),
		"the rewrite decision depends on a session variable, so the statement must be plan-cache rejected")
}

func TestSqlAutoIsNullDisabledByVariableKeepsPlainSemantics(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(0), nil)
	qry := buildSqlAutoIsNullQuery(t, cc, "select seq, val from auto_t where seq is null")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"))
	require.True(t, sqlAutoIsNullAnyNodeNotCacheable(qry),
		"toggling the flag must never serve a plan cached under the other value")
}

func TestSqlAutoIsNullNilResolvesToRegisteredDefault(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, nil, nil)
	qry := buildSqlAutoIsNullQuery(t, cc, "select seq, val from auto_t where seq is null")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"))
}

func TestSqlAutoIsNullResolveErrorKeepsPlainSemanticsUncacheable(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, nil,
		moerr.NewInternalErrorNoCtxf("var not found: %s", sqlAutoIsNullVariableName))
	qry := buildSqlAutoIsNullQuery(t, cc, "select seq, val from auto_t where seq is null")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"))
	require.True(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

func TestSqlAutoIsNullCompositePredicateNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc, "select seq, val from auto_t where seq is null and val <= 1001")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

func TestSqlAutoIsNullNonAutoIncrementColumnNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc, "select seq from auto_t where val is null")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

func TestSqlAutoIsNullNullExtendedJoinColumnNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	// The exact shape of mysql-test r/odbc.result Bug#29171668: the right side
	// of a LEFT JOIN null-extends, and `WHERE <auto col> IS NULL` must keep
	// selecting the null-extended rows instead of last_insert_id().
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select s.val from auto_t a left join auto_t s on a.seq = s.seq where s.seq is null")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"),
		"expected the isnull predicate in the built plan; plan was:\n%s", qry.String())
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"LEFT-JOIN null-extended references never participate in the rewrite")
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

func TestSqlAutoIsNullHavingNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select seq, count(val) from auto_t group by seq having seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"))
}

func TestSqlAutoIsNullParenthesizedWholeClauseRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc, "select seq, val from auto_t where (seq is null)")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"parentheses are surface syntax: MySQL resolves the parenthesized whole-clause form to the same expression and rewrites it too")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.True(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

func TestSqlAutoIsNullSubqueryWholeClauseRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	// MySQL gates the substitution on thd->lex->current_query_block()->where_cond(),
	// so a nested query block's own whole-clause form is substituted as well.
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select val from auto_t where val in (select val from auto_t a where a.seq is null)")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"the inner query block's whole-clause IS NULL must be substituted")
}
