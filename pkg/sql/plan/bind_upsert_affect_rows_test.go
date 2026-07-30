// Copyright 2021 - 2024 Matrix Origin
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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// mainUpdateCtxCountDelete returns the main-table UpdateCtx's
// CountDeleteAffectRows flag of the (single) MULTI_UPDATE node in the plan.
func mainUpdateCtxCountDelete(t *testing.T, p *Plan) bool {
	t.Helper()
	q := p.GetQuery()
	require.NotNil(t, q)
	var found *planpb.Node
	for _, n := range q.Nodes {
		if n.NodeType == planpb.Node_MULTI_UPDATE {
			require.Nil(t, found, "expected a single MULTI_UPDATE node")
			found = n
		}
	}
	require.NotNil(t, found, "no MULTI_UPDATE node in plan")
	require.NotEmpty(t, found.UpdateCtxList)
	return found.UpdateCtxList[0].CountDeleteAffectRows
}

// hasNoopFilter reports whether the plan contains a FILTER node whose predicate
// is the ODKU no-op guard: isnull(old rowid) OR NOT( col <=> col [AND ...] ).
// The isnull(rowid) branch keeps non-conflicting rows (all-NULL old image)
// flowing to the INSERT side instead of being dropped by the equality chain.
func hasNoopFilter(p *Plan) bool {
	q := p.GetQuery()
	if q == nil {
		return false
	}
	for _, n := range q.Nodes {
		if n.NodeType != planpb.Node_FILTER {
			continue
		}
		for _, cond := range n.FilterList {
			if notExpr := noopFilterNotBranch(cond); notExpr != nil {
				if inner := notExpr.Args[0].GetF(); inner != nil && inner.Func != nil {
					switch inner.Func.ObjName {
					case "<=>", "and":
						return true
					}
				}
			}
		}
	}
	return false
}

// noopFilterNotBranch matches the no-op guard shape
// or(isnull(old rowid), not(...)) and returns the not(...) function,
// or nil if expr does not match.
func noopFilterNotBranch(expr *planpb.Expr) *planpb.Function {
	f := expr.GetF()
	if f == nil || f.Func == nil || f.Func.ObjName != "or" || len(f.Args) != 2 {
		return nil
	}
	isNull := f.Args[0].GetF()
	if isNull == nil || isNull.Func == nil || isNull.Func.ObjName != "isnull" ||
		len(isNull.Args) != 1 || isNull.Args[0].GetCol() == nil {
		return nil
	}
	notExpr := f.Args[1].GetF()
	if notExpr == nil || notExpr.Func == nil || notExpr.Func.ObjName != "not" || len(notExpr.Args) != 1 {
		return nil
	}
	return notExpr
}

// TestUpsertAffectRowsPlan verifies the plan-level wiring of the MySQL-compatible
// affected-rows fix: REPLACE and INSERT ... ON DUPLICATE KEY UPDATE flag their
// main-table UpdateCtx so the executor counts the conflicting-row DELETE, ODKU
// additionally inserts a no-op guard FILTER, and plain INSERT / UPDATE keep the
// flag off so they count once.
func TestUpsertAffectRowsPlan(t *testing.T) {
	mock := NewMockOptimizer(true)

	t.Run("ODKU flags main ctx and adds no-op filter", func(t *testing.T) {
		// dept goes through the dedup-join + MULTI_UPDATE path; loc is not part of
		// any key, so it is a legal ON DUPLICATE KEY UPDATE target.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (1, 'A', 'B') on duplicate key update loc = loc")
		require.NoError(t, err)
		require.True(t, mainUpdateCtxCountDelete(t, p),
			"ODKU main UpdateCtx should set CountDeleteAffectRows")
		require.True(t, hasNoopFilter(p),
			"ODKU plan should contain a NOT(<=>) no-op filter")
	})

	t.Run("REPLACE flags main ctx", func(t *testing.T) {
		p, err := runOneStmt(mock, t,
			"replace into constraint_test.emp(empno, ename, job) values (1, 'A', 'B')")
		require.NoError(t, err)
		require.True(t, mainUpdateCtxCountDelete(t, p),
			"REPLACE main UpdateCtx should set CountDeleteAffectRows")
		require.False(t, hasNoopFilter(p),
			"REPLACE always rewrites a conflicting row, so it has no no-op filter")
	})

	t.Run("plain INSERT has no no-op filter", func(t *testing.T) {
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.emp(empno, ename, job) values (1, 'A', 'B')")
		require.NoError(t, err)
		require.False(t, hasNoopFilter(p),
			"plain INSERT must not add a no-op filter")
	})

	t.Run("ODKU no-op filter excludes ON UPDATE columns", func(t *testing.T) {
		// t_on_update has: id (PK), val, updated_at (ON UPDATE CURRENT_TIMESTAMP).
		// ODKU with v=v should skip updated_at in the no-op filter so that the
		// auto-update expression does not defeat the no-op guard.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.t_on_update(id, val) values (1, 10) on duplicate key update val = val")
		require.NoError(t, err)
		require.True(t, mainUpdateCtxCountDelete(t, p),
			"ODKU main UpdateCtx should set CountDeleteAffectRows")
		require.True(t, hasNoopFilter(p),
			"ODKU plan should contain a NOT(<=>) no-op filter")
		// The no-op filter must not reference the auto-update column.
		require.False(t, noopFilterReferencesCol(p, "updated_at"),
			"no-op FILTER must not reference auto-update column updated_at")
	})

	t.Run("ODKU no-op filter keeps non-conflicting rows via rowid guard", func(t *testing.T) {
		// An all-NULL insert row into a nullable unique key never conflicts, so
		// its old image is all-NULL and every <=> in the no-op chain evaluates
		// to true. The isnull(old rowid) OR-branch must be present so such rows
		// are inserted instead of silently dropped. hasNoopFilter only matches
		// the or(isnull(rowid), not(...)) shape, so a true result asserts the
		// guard exists.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (1, 'A', 'B') on duplicate key update loc = loc")
		require.NoError(t, err)
		require.True(t, hasNoopFilter(p),
			"ODKU no-op filter must carry the isnull(old rowid) guard for non-conflicting rows")
	})

	t.Run("ODKU no-op filter compares only written columns on secondary-unique conflict", func(t *testing.T) {
		// dept has PK deptno and a secondary UNIQUE key on dname. An ODKU whose
		// conflict is resolved through dname can carry an incoming deptno that
		// differs from the existing row's deptno. The update (loc = loc) writes
		// only loc, so the row is a genuine no-op and MySQL returns
		// affected-rows = 0. The no-op guard must therefore compare the final
		// written value of loc only — not the immutable PK deptno nor the
		// conflict-key dname against the raw incoming image, which would
		// spuriously turn the no-op into a counted update.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (999, 'Sales', 'NY') on duplicate key update loc = loc")
		require.NoError(t, err)
		require.True(t, hasNoopFilter(p),
			"ODKU plan should contain a no-op filter")
		require.Equal(t, 1, countNoopEqComparisons(p),
			"no-op FILTER must compare only the single written column (loc), not immutable deptno/dname")
		require.True(t, noopEqRightOperandsAreCols(p),
			"no-op FILTER must compare against the materialized new-image column, not re-run the assignment")
	})

	t.Run("ODKU no-op filter references materialized value for computed update", func(t *testing.T) {
		// A computed assignment (loc = substring(loc, 1, 2)) is evaluated once by
		// the dedup-update join and written into the new-image column. The no-op
		// guard must compare against that materialized column, never re-evaluate
		// the expression — otherwise a non-deterministic assignment would be
		// evaluated twice and the no-op decision could disagree with the value
		// actually stored.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (1, 'A', 'B') on duplicate key update loc = substring(loc, 1, 2)")
		require.NoError(t, err)
		require.True(t, hasNoopFilter(p),
			"ODKU plan should contain a no-op filter")
		require.True(t, noopEqRightOperandsAreCols(p),
			"no-op FILTER must reference the materialized new-image column, not the substring() expression")
	})

	t.Run("DeepCopyNode preserves CountDeleteAffectRows on MULTI_UPDATE", func(t *testing.T) {
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (1, 'A', 'B') on duplicate key update loc = loc")
		require.NoError(t, err)
		var mu *planpb.Node
		for _, n := range p.GetQuery().Nodes {
			if n.NodeType == planpb.Node_MULTI_UPDATE {
				mu = n
				break
			}
		}
		require.NotNil(t, mu)
		require.True(t, mu.UpdateCtxList[0].CountDeleteAffectRows)
		copied := DeepCopyNode(mu)
		require.True(t, copied.UpdateCtxList[0].CountDeleteAffectRows,
			"DeepCopyUpdateCtxList must preserve CountDeleteAffectRows")
	})

	t.Run("ODKU no-op filter excludes generated column derived from ON UPDATE", func(t *testing.T) {
		// t_on_update_gen has: id (PK), val, updated_at (ON UPDATE CURRENT_TIMESTAMP),
		// g (stored, g AS (updated_at)). ODKU with val=val must skip both updated_at
		// and its dependent generated column g, otherwise g's recomputed value would
		// defeat the no-op guard.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.t_on_update_gen(id, val) values (1, 10) on duplicate key update val = val")
		require.NoError(t, err)
		require.True(t, mainUpdateCtxCountDelete(t, p),
			"ODKU main UpdateCtx should set CountDeleteAffectRows")
		require.True(t, hasNoopFilter(p),
			"ODKU plan should contain a NOT(<=>) no-op filter")
		require.False(t, noopFilterReferencesCol(p, "updated_at"),
			"no-op FILTER must not reference auto-update column updated_at")
		require.False(t, noopFilterReferencesCol(p, "g"),
			"no-op FILTER must not reference generated column g derived from updated_at")
	})
}

// noopFilterReferencesCol reports whether the ODKU no-op FILTER's AND/<=>
// predicate chain contains a ColRef whose name equals colName.
func noopFilterReferencesCol(p *Plan, colName string) bool {
	q := p.GetQuery()
	if q == nil {
		return false
	}
	for _, n := range q.Nodes {
		if n.NodeType != planpb.Node_FILTER {
			continue
		}
		for _, cond := range n.FilterList {
			exprs := collectNoopColRefs(cond)
			for _, e := range exprs {
				if e.GetCol() != nil && e.GetCol().Name == colName {
					return true
				}
			}
		}
	}
	return false
}

// countNoopEqComparisons returns the number of null-safe-equal (<=>)
// comparisons in the ODKU no-op FILTER's equality chain, or -1 if no such
// filter exists. It counts exactly the columns the update actually writes.
func countNoopEqComparisons(p *Plan) int {
	q := p.GetQuery()
	if q == nil {
		return -1
	}
	for _, n := range q.Nodes {
		if n.NodeType != planpb.Node_FILTER {
			continue
		}
		for _, cond := range n.FilterList {
			if notExpr := noopFilterNotBranch(cond); notExpr != nil {
				return countNullSafeEq(notExpr.Args[0])
			}
		}
	}
	return -1
}

// countNullSafeEq counts <=> nodes in an AND/<=> predicate tree.
func countNullSafeEq(expr *planpb.Expr) int {
	f := expr.GetF()
	if f == nil || f.Func == nil {
		return 0
	}
	switch f.Func.ObjName {
	case "and":
		total := 0
		for _, arg := range f.Args {
			total += countNullSafeEq(arg)
		}
		return total
	case "<=>":
		return 1
	default:
		return 0
	}
}

// noopEqRightOperandsAreCols reports whether every <=> in the ODKU no-op FILTER
// compares against a bare column reference on its right-hand (new-image) side.
// The no-op guard must reference the final written value the dedup-update join
// already materialized into the new image, never re-evaluate the assignment
// expression (which would double-evaluate a non-deterministic update such as
// v = floor(rand()*2)). Returns false if no no-op FILTER exists.
func noopEqRightOperandsAreCols(p *Plan) bool {
	q := p.GetQuery()
	if q == nil {
		return false
	}
	for _, n := range q.Nodes {
		if n.NodeType != planpb.Node_FILTER {
			continue
		}
		for _, cond := range n.FilterList {
			notExpr := noopFilterNotBranch(cond)
			if notExpr == nil {
				continue
			}
			return eqRightOperandsAreCols(notExpr.Args[0])
		}
	}
	return false
}

func eqRightOperandsAreCols(expr *planpb.Expr) bool {
	f := expr.GetF()
	if f == nil || f.Func == nil {
		return false
	}
	switch f.Func.ObjName {
	case "and":
		for _, arg := range f.Args {
			if !eqRightOperandsAreCols(arg) {
				return false
			}
		}
		return true
	case "<=>":
		return len(f.Args) == 2 && f.Args[1].GetCol() != nil
	default:
		return false
	}
}

// collectNoopColRefs collects all ColRef expressions nested inside the
// OR(isnull(rowid), NOT(AND(<=>(...)))) no-op FILTER pattern. Returns nil
// if the pattern does not match. Only the equality chain's ColRefs are
// returned; the rowid guard is not part of the compared columns.
func collectNoopColRefs(expr *planpb.Expr) []*planpb.Expr {
	notExpr := noopFilterNotBranch(expr)
	if notExpr == nil {
		return nil
	}
	return collectAndNullSafeColRefs(notExpr.Args[0])
}

// collectAndNullSafeColRefs recursively collects ColRef expressions from
// an AND/<=> predicate tree.
func collectAndNullSafeColRefs(expr *planpb.Expr) []*planpb.Expr {
	f := expr.GetF()
	if f == nil || f.Func == nil {
		return nil
	}
	switch f.Func.ObjName {
	case "and":
		var res []*planpb.Expr
		for _, arg := range f.Args {
			res = append(res, collectAndNullSafeColRefs(arg)...)
		}
		return res
	case "<=>":
		return f.Args
	default:
		return nil
	}
}
