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
// is the ODKU no-op guard: NOT( col <=> col [AND ...] ).
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
			f := cond.GetF()
			if f == nil || f.Func == nil || f.Func.ObjName != "not" {
				continue
			}
			if inner := f.Args[0].GetF(); inner != nil && inner.Func != nil {
				switch inner.Func.ObjName {
				case "<=>", "and":
					return true
				}
			}
		}
	}
	return false
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

// collectNoopColRefs collects all ColRef expressions nested inside the
// NOT(AND(<=>(...))) no-op FILTER pattern. Returns nil if the pattern
// does not match.
func collectNoopColRefs(expr *planpb.Expr) []*planpb.Expr {
	f := expr.GetF()
	if f == nil || f.Func == nil || f.Func.ObjName != "not" {
		return nil
	}
	if len(f.Args) != 1 {
		return nil
	}
	return collectAndNullSafeColRefs(f.Args[0])
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
