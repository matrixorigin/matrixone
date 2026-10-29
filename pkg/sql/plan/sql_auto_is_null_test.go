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

// TestSqlAutoIsNullEnabledInterpretationMatrix pins the accepted-value
// contract across every Go representation sql_auto_is_null can surface in:
// session-store integers, protocol text, compatibility casts and float-form
// protocol payloads. Every accepted representation enables/disables the
// rewrite symmetrically; anything outside the accepted set disables it.
func TestSqlAutoIsNullEnabledInterpretationMatrix(t *testing.T) {
	enabled := []interface{}{
		int(1), int8(1), int16(1), int32(1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
		float32(1), float64(1),
		true, "1", "on", "true", "TRUE",
	}
	for _, v := range enabled {
		require.True(t, sqlAutoIsNullEnabled(v),
			"representation %T(%v) must enable the rewrite", v, v)
	}
	disabled := []interface{}{
		int(0), int8(0), int16(0), int32(0), int64(0),
		uint(0), uint8(0), uint16(0), uint32(0), uint64(0),
		float32(0), float64(0),
		false, "",
	}
	for _, v := range disabled {
		require.False(t, sqlAutoIsNullEnabled(v),
			"representation %T(%v) must disable the rewrite", v, v)
	}
}

// TestSqlAutoIsNullExpressionOperandNotRewritten pins the operand criterion:
// only a bare AUTO_INCREMENT column reference participates. The whole-clause
// shape on any other expression keeps plain IS NULL semantics and stays
// cacheable, because the statement never matched the MySQL shape.
func TestSqlAutoIsNullExpressionOperandNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc, "select seq, val from auto_t where (seq + 1) is null")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"),
		"an IS NULL whole clause on a non-column operand must keep plain semantics")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry),
		"a non-matching shape must not burn the plan cache")
}

// TestSqlAutoIsNullOuterJoinNullSideMatrix pins the structural exclusion walk
// without depending on parser join support. A LEFT (and ASOF LEFT) join
// null-extends exactly its right subtree, so references through the left side
// keep participating in the rewrite; a FULL OUTER join null-extends both
// sides; and null-side reachability accumulates through nested joins (an
// inner-joined pair on the null side exposes every member) and through
// chained outer joins. Corrupt or out-of-range node references must stay a
// no-op instead of panicking.
func TestSqlAutoIsNullOuterJoinNullSideMatrix(t *testing.T) {
	scan := func(tag int32) *plan.Node {
		// NodeType is irrelevant to the walk under test: it reads only
		// JoinType, Children and BindingTags.
		return &plan.Node{BindingTags: []int32{tag}}
	}
	inner := func(children ...int32) *plan.Node {
		return &plan.Node{JoinType: plan.Node_INNER, Children: children}
	}
	left := func(children ...int32) *plan.Node {
		return &plan.Node{JoinType: plan.Node_LEFT, Children: children}
	}
	build := func(nodes ...*plan.Node) *QueryBuilder {
		return &QueryBuilder{qry: &plan.Query{Nodes: nodes}}
	}

	// LEFT(a, INNER(b, c)): the right subtree is the null side, so tags b and c
	// are excluded while the left-side tag a keeps plain rewrite eligibility.
	nodes := []*plan.Node{
		scan(1), // 0: a
		scan(2), // 1: b
		scan(3), // 2: c
		inner(1, 2),
		left(0, 3),
	}
	builder := build(nodes...)
	require.True(t, builder.colOnOuterJoinNullSide(4, 2),
		"every member of a nested inner pair on the left join's null side is null-extended")
	require.True(t, builder.colOnOuterJoinNullSide(4, 3),
		"every member of a nested inner pair on the left join's null side is null-extended")
	require.False(t, builder.colOnOuterJoinNullSide(4, 1),
		"the preserved side of a LEFT join is not null-extended")

	// FULL OUTER(a, b): both sides are null-extended, so both are excluded.
	nodes = []*plan.Node{
		scan(10), // 0: a
		scan(11), // 1: b
		{NodeType: plan.Node_JOIN, JoinType: plan.Node_OUTER, Children: []int32{0, 1}},
	}
	builder = build(nodes...)
	require.True(t, builder.colOnOuterJoinNullSide(2, 10),
		"a FULL OUTER join conservatively null-extends its left side")
	require.True(t, builder.colOnOuterJoinNullSide(2, 11),
		"a FULL OUTER join conservatively null-extends its right side")

	// Chained LEFT joins accumulate null sides: b (inner null side) and c
	// (outer null side) are both excluded, a stays eligible.
	nodes = []*plan.Node{
		scan(20), // 0: a
		scan(21), // 1: b
		scan(22), // 2: c
		left(0, 1),
		left(3, 2),
	}
	builder = build(nodes...)
	require.True(t, builder.colOnOuterJoinNullSide(4, 21))
	require.True(t, builder.colOnOuterJoinNullSide(4, 22))
	require.False(t, builder.colOnOuterJoinNullSide(4, 20))

	// Out-of-range and negative node references must not panic; they simply
	// report no exclusion.
	builder = build(
		scan(30),
		left(0, 42),
	)
	require.False(t, builder.colOnOuterJoinNullSide(-1, 30))
	require.False(t, builder.colOnOuterJoinNullSide(7, 30))
	require.False(t, builder.colOnOuterJoinNullSide(1, 30),
		"a dangling right child must stay a no-op")
	require.False(t, builder.colOnOuterJoinNullSide(1, 31),
		"an unknown tag is not null-extended")
}

// TestSqlAutoIsNullChainedLeftJoinsNotRewritten pins the SQL-level consumer
// shape that accumulates null sides across chained outer joins: the
// whole-clause form on a column that any join level null-extends keeps
// plain IS NULL semantics, exactly as MySQL's odbc.result requires.
func TestSqlAutoIsNullChainedLeftJoinsNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select b.seq from auto_t a left join auto_t b on a.seq = b.seq "+
			"left join auto_t c on b.val = c.val where b.seq is null")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"),
		"a column null-extended by any outer join level must keep plain IS NULL semantics")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

// The derived-table family pins column lineage: a projected column keeps the
// null-extended reachability of its producer, while a preserved-side
// reference stays rewrite-eligible and a plain projection keeps rewriting.
// MySQL observable semantics: a mergeable derived table collapses into the
// outer block, so the whole-clause form must behave exactly as if written
// against the underlying JOIN.

// TestSqlAutoIsNullDerivedTableNullExtendedColumnNotRewritten is the review
// case: `WHERE d.id IS NULL` where d projects the null-extended side's
// AUTO_INCREMENT column must select the unmatched rows, never compare them to
// last_insert_id().
func TestSqlAutoIsNullDerivedTableNullExtendedColumnNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select s.seq from auto_t m left join auto_t s "+
			"on m.seq = s.seq) d where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a projected null-extended AUTO_INCREMENT column must keep plain IS NULL semantics")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

// TestSqlAutoIsNullDerivedTableMixPreservedColumnNotRewritten pins the
// per-column boundary of the same derived table under the MySQL 8.4
// FIELD_ITEM gate: the outer statement sees d.d1 through the mergeable
// derived table, an Item_view_ref (REF_ITEM) in MySQL terms, which the
// sql_auto_is_null gate in Item_func_isnull::fix_fields rejects; the
// predicate keeps plain IS NULL semantics even for the preserved join side.
func TestSqlAutoIsNullDerivedTableMixPreservedColumnNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.d1 from (select m.seq as d1, s.seq as d2 from auto_t m "+
			"left join auto_t s on m.seq = s.seq) d where d.d1 is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a derived-table column reference is not a FIELD_ITEM and must keep plain IS NULL semantics")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

// TestSqlAutoIsNullNestedDerivedNullSideNotRewritten walks the lineage one
// derived layer deeper: the null extension introduced in the innermost block
// still excludes the outermost reference.
func TestSqlAutoIsNullNestedDerivedNullSideNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select e.d2 from (select d.d2 from (select s.seq as d2 from auto_t m "+
			"left join auto_t s on m.seq = s.seq) d) e where e.d2 is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"the innermost block's null extension must exclude the outer reference")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

// TestSqlAutoIsNullDerivedTablePlainProjectionNotRewritten pins the MySQL
// 8.4 FIELD_ITEM gate for the mergeable plain projection: a reference to a
// derived-table column resolves to Item_view_ref (REF_ITEM), never
// Item_field, so Item_func_isnull::fix_fields skips the sql_auto_is_null
// rewrite and the predicate keeps plain IS NULL semantics.
func TestSqlAutoIsNullDerivedTablePlainProjectionNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select seq from auto_t) d where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a mergeable derived-table column reference is not a FIELD_ITEM and must keep plain IS NULL semantics")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

// The grouped family pins the aggregate boundary (review 5690740934's
// follow-up 5691534789). MySQL cannot merge a derived table that contains
// GROUP BY or aggregate functions, so the outer statement observes a
// materialized column: it no longer resolves to the AUTO_INCREMENT
// definition, and an aggregate over null-extended rows can genuinely yield
// NULL. The whole-clause form therefore keeps plain IS NULL semantics for
// every reference whose lineage crosses an aggregate node.

// TestSqlAutoIsNullGroupedDerivedNullSideNotRewritten is the review case:
// the grouped column of the null-extended side must keep the NULL group row
// observable, never compare it to last_insert_id().
func TestSqlAutoIsNullGroupedDerivedNullSideNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select s.seq from auto_t m left join auto_t s "+
			"on m.seq = s.seq group by s.seq) d where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a grouped null-extended AUTO_INCREMENT column must keep plain IS NULL semantics")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

// TestSqlAutoIsNullGroupedPlainDerivedNotRewritten pins the materialization
// parity of the same boundary without any join: MySQL never rewrites through
// a grouped derived table, and the plain query over a NOT NULL AUTO_INCREMENT
// column therefore observes an empty result instead of the last-inserted row.
func TestSqlAutoIsNullGroupedPlainDerivedNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select seq from auto_t group by seq) d where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a grouped derived table materializes before the outer statement and never carries the rewrite")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

// TestSqlAutoIsNullAggregateOverNullSideNotRewritten pins the aggregate side
// of the boundary: an aggregate over null-extended rows can yield NULL for a
// group, so the whole-clause form on the aggregate output keeps plain
// semantics regardless of which leg of the gate protects it.
func TestSqlAutoIsNullAggregateOverNullSideNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.m from (select max(s.seq) as m from auto_t m left join auto_t s "+
			"on m.seq = s.seq group by m.val) d where d.m is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"an aggregate output over a null-extended side must keep plain IS NULL semantics")
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

// The materialization family pins MySQL's derived-merge boundary (review
// 5692189512): a derived table containing LIMIT/OFFSET, DISTINCT, a window
// function, HAVING or a UNION is not mergeable (mysql/sql_lex.cc
// mergeability rules), so the outer statement observes a materialized
// column whose temporary field loses the AUTO_INCREMENT resolution
// property (mysql/field.cc temporary-field construction). The whole-clause
// form therefore keeps plain IS NULL semantics: over a NOT NULL
// AUTO_INCREMENT column that is an empty result, never a last_insert_id()
// comparison. Plain WHERE-based derived tables stay mergeable and keep the
// rewrite, so the boundary must be the shape of the derived block itself.

func TestSqlAutoIsNullLimitDerivedNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select seq from auto_t limit 1) d where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a materialized (LIMIT/OFFSET) derived column must keep plain IS NULL semantics; plan:\n%s", qry.String())
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

func TestSqlAutoIsNullDistinctDerivedNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select distinct seq from auto_t) d where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a materialized (DISTINCT) derived column must keep plain IS NULL semantics; plan:\n%s", qry.String())
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

func TestSqlAutoIsNullWindowDerivedNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select seq, row_number() over (order by val) as rn "+
			"from auto_t) d where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a materialized (window) derived column must keep plain IS NULL semantics; plan:\n%s", qry.String())
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

func TestSqlAutoIsNullHavingDerivedNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select seq from auto_t having seq >= 0) d where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a materialized (HAVING) derived column must keep plain IS NULL semantics; plan:\n%s", qry.String())
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

func TestSqlAutoIsNullUnionAllDerivedNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select seq from auto_t union all select seq from auto_t) d "+
			"where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a materialized (UNION ALL) derived column must keep plain IS NULL semantics; plan:\n%s", qry.String())
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}

// TestSqlAutoIsNullOrderedLimitDerivedNotRewritten pins the ordered-limit
// shape: ORDER BY with LIMIT materializes the block exactly like a plain
// LIMIT; the limit marker rides on the sort stage between the block's
// projections, and the boundary must catch it there too.
func TestSqlAutoIsNullOrderedLimitDerivedNotRewritten(t *testing.T) {
	cc := newSqlAutoIsNullTestContext(t, int64(1), nil)
	qry := buildSqlAutoIsNullQuery(t, cc,
		"select d.seq from (select seq from auto_t order by seq limit 1) d where d.seq is null")
	require.False(t, sqlAutoIsNullQueryHasFunction(qry, "last_insert_id"),
		"a materialized (ORDER BY ... LIMIT) derived column must keep plain IS NULL semantics; plan:\n%s", qry.String())
	require.True(t, sqlAutoIsNullQueryHasFunction(qry, "isnull"))
	require.False(t, sqlAutoIsNullAnyNodeNotCacheable(qry))
}
