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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// CTE lexical-ownership boundary controls.
//
// The plan-mock catalog contains a persisted VIEW named "c" (its stored SQL
// is its own `WITH c, d ... UNION ALL ...` query) and the mock resolver falls
// back to unqualified compatibility tables when the qualified lookup misses.
// Tests here therefore use unique identifiers and prove catalog absence or
// presence through the real resolver before any physical-sensitive
// expectation, so a withheld pending CTE can never be mistaken for a
// resolved lexical definition and a legal physical fallback can never be
// mistaken for a scope leak.
//
// Contracts covered:
//   - a pending later same-WITH declaration stays invisible through scalar
//     and derived child frames, whether disallowed by an outer CTE body's
//     owner, an inner CTE body's owner, or both acting at once;
//   - earlier same-WITH siblings stay visible through those same frames;
//   - when no physical object exists, a withheld lookup ends in the typed
//     moerr missing-table error; when a physical object exists the fallback
//     is legal and its scan provenance stays physical.

func parseExactlyOneSelect(t *testing.T, sql string) *tree.Select {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	t.Cleanup(stmt.Free)
	sel, ok := stmt.(*tree.Select)
	require.True(t, ok, sql)
	require.NotNil(t, sel.With, sql)
	return sel
}

func TestInheritedMaskAndWrappedRecursiveBodyControls(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))

	// requireNoPhysical proves through the real mock resolver (the same
	// compatibility fallback production uses) that no physical object with
	// this name exists in the loaded fixture catalog.
	requireNoPhysical := func(t *testing.T, name string) {
		t.Helper()
		var borrow TableResolution
		_, tblDef, err := mock.ctxt.Resolve("cte_test", name, nil, &borrow)
		require.NoError(t, err, name)
		require.Nil(t, tblDef, "unexpected physical object %q in mock catalog", name)
	}

	// mustPhysical proves the named fixture object exists through the same
	// resolver and returns its object identity plus definition for
	// provenance comparisons.
	mustPhysical := func(t *testing.T, name string) (*ObjectRef, *TableDef) {
		t.Helper()
		var borrow TableResolution
		objRef, tblDef, err := mock.ctxt.Resolve("cte_test", name, nil, &borrow)
		require.NoError(t, err, name)
		require.NotNil(t, tblDef, "expected physical object %q in mock catalog", name)
		require.NotNil(t, objRef, name)
		return objRef, tblDef
	}

	// requireTypedMissing asserts the withheld lookup ended in the actual
	// moerr missing-table error naming the target relation.
	requireTypedMissing := func(t *testing.T, err error, name string) {
		t.Helper()
		require.Error(t, err)
		require.ErrorContains(t, err, name)
		require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), "%v", err)
	}

	t.Run("g1_scalar_subquery_child_sees_enclosing_definition", func(t *testing.T) {
		sql := `
			WITH seed7 AS (SELECT 7 AS a)
				(WITH childuse AS (SELECT (SELECT a FROM seed7) AS a), seed7 AS (SELECT 9 AS a)
				SELECT a FROM childuse)`
		sel := parseExactlyOneSelect(t, sql)
		require.Len(t, sel.With.CTEs, 1)
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
	})

	t.Run("g1_derived_table_child_sees_enclosing_definition", func(t *testing.T) {
		sql := `
			WITH seed7 AS (SELECT 7 AS a)
				(WITH childuse AS (SELECT a FROM (SELECT a FROM seed7) dsource), seed7 AS (SELECT 9 AS a)
				SELECT a FROM childuse)`
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
	})

	t.Run("c1_prior_sibling_through_child_stays_visible", func(t *testing.T) {
		sql := `
			WITH early7 AS (SELECT 7 AS a), miduse AS (SELECT (SELECT a FROM early7) AS b)
			SELECT b FROM miduse`
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
	})

	t.Run("two_owners_outer_later_sibling_withheld_from_inner_body", func(t *testing.T) {
		// Both CTE body owners are active at the failing lookup: midowner's
		// body is binding (it contains the nested WITH) and innertop's body is
		// binding inside it. The lookup targets OUTERTARGET, a later sibling
		// of midowner declared afterwards in the outer WITH, so it must stay
		// invisible: the enclosing definition set does not contain it yet
		// and no physical object exists to fall back to.
		sql := `
			WITH outerseed AS (SELECT 3 AS a),
				midowner AS (
					(WITH innertop AS (SELECT a AS v FROM outertarget), innerextra AS (SELECT 6 AS w)
					SELECT v FROM innertop)
				),
				outertarget AS (SELECT 4 AS a)
			SELECT v FROM midowner`
		requireNoPhysical(t, "outerseed")
		requireNoPhysical(t, "midowner")
		requireNoPhysical(t, "innertop")
		requireNoPhysical(t, "innerextra")
		requireNoPhysical(t, "outertarget")
		sel := parseExactlyOneSelect(t, sql)
		require.Len(t, sel.With.CTEs, 3)
		require.Equal(t, "midowner", string(sel.With.CTEs[1].Name.Alias))
		require.NotNil(t, sel.With.CTEs[1].Stmt, sql)
		_, err := runOneStmt(mock, t, sql)
		requireTypedMissing(t, err, "outertarget")
	})

	t.Run("two_owners_outer_earlier_sibling_visible_from_inner_body", func(t *testing.T) {
		// The positive twin of the outer-later cell: every token is identical
		// except the lookup now references OUTERSEED, an earlier sibling of
		// midowner in the same outer WITH, which must stay visible through
		// both active bodies.
		sql := `
			WITH outerseed AS (SELECT 3 AS a),
				midowner AS (
					(WITH innertop AS (SELECT a AS v FROM outerseed), innerextra AS (SELECT 6 AS w)
					SELECT v FROM innertop)
				),
				outertarget AS (SELECT 4 AS a)
			SELECT v FROM midowner`
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
	})

	t.Run("two_owners_inner_later_sibling_withheld", func(t *testing.T) {
		// The inner WITH's own later sibling INNERLATER, dissolved inside
		// innertop's body while both body owners are active, must stay
		// invisible and end in the typed missing-table error.
		sql := `
			WITH outerseed AS (SELECT 3 AS a),
				midowner AS (
					(WITH innertop AS (SELECT a AS v FROM innerlater), innerlater AS (SELECT 4 AS a)
					SELECT v FROM innertop)
				),
				outertarget AS (SELECT 5 AS a)
			SELECT v FROM midowner`
		requireNoPhysical(t, "innertop")
		requireNoPhysical(t, "innerlater")
		requireNoPhysical(t, "midowner")
		sel := parseExactlyOneSelect(t, sql)
		require.Len(t, sel.With.CTEs, 3)
		_, err := runOneStmt(mock, t, sql)
		requireTypedMissing(t, err, "innerlater")
	})

	t.Run("two_owners_inner_earlier_sibling_visible", func(t *testing.T) {
		// The positive twin of the inner-later cell: the inner WITH declares
		// innerearly BEFORE innertop, so the lookup through both active body
		// owners must bind normally.
		sql := `
			WITH outerseed AS (SELECT 3 AS a),
				midowner AS (
					(WITH innerearly AS (SELECT 4 AS a), innertop AS (SELECT a AS v FROM innerearly)
					SELECT v FROM innertop)
				)
			SELECT v FROM midowner`
		requireNoPhysical(t, "innerearly")
		requireNoPhysical(t, "innertop")
		requireNoPhysical(t, "midowner")
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
	})

	t.Run("g1_forward_reference_child_withheld_and_typed_missing_no_physical", func(t *testing.T) {
		requireNoPhysical(t, "lateonly")
		sql := `
			WITH fwduse AS (SELECT (SELECT a FROM lateonly) AS a), lateonly AS (SELECT 9 AS a)
			SELECT a FROM fwduse`
		_, err := runOneStmt(mock, t, sql)
		requireTypedMissing(t, err, "lateonly")
	})

	t.Run("legacy_c_view_fallback_after_withholding_stays_valid", func(t *testing.T) {
		// The later-declared CTE "c" stays withheld inside plainuse's scalar
		// child and the binding legally falls back to the persisted VIEW c
		// that the fixture still provides. A stored view keeps its own
		// lexical root; nothing from the caller is injected into it.
		mustPhysical(t, "c")
		sql := `
			WITH plainuse AS (SELECT (SELECT a FROM c) AS a), c AS (SELECT 9 AS a)
			SELECT a FROM plainuse`
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
	})

	t.Run("physical_t1_scan_provenance_beats_invisible_later_cte", func(t *testing.T) {
		// The later-declared CTE t1 stays invisible inside plainuse2's scalar
		// child, and the final plan must prove its t1 scan is the PHYSICAL
		// fixture table rather than the invisible CTE: the reachable scan
		// node's object identity matches the real resolver's t1 (same object
		// name and the same visible column count as the resolver's
		// definition). Value-level oracles for the equivalent embedded
		// physical-vs-CTE contrast live in the sqlintegration suite.
		resolverObjRef, resolverDef := mustPhysical(t, "t1")
		sql := `
			WITH plainuse2 AS (SELECT (SELECT a FROM t1) AS a), t1 AS (SELECT 9 AS a)
			SELECT a FROM plainuse2`
		plan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
		require.NotNil(t, plan)
		qry := plan.GetQuery()
		require.NotNil(t, qry, "expected a query plan from the bound statement")

		// Reachability is the actual execution graph, not the whole node pool:
		// walk the step roots down the child edges only, with visited protection
		// against cycles. Only a node reached this way executes, and the query
		// result passes through it, so the witness scan must be found there. A
		// dead node or a memoized CTE node named identically proves nothing.
		seen := make(map[int32]bool)
		stack := append([]int32(nil), qry.Steps...)
		found := false
		for len(stack) > 0 {
			nodeID := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if nodeID < 0 || seen[nodeID] {
				continue
			}
			seen[nodeID] = true
			node := qry.Nodes[nodeID]
			if node == nil {
				continue
			}
			stack = append(stack, node.Children...)
			if node.NodeType != pbplan.Node_TABLE_SCAN || node.ObjRef == nil {
				continue
			}
			if node.ObjRef.GetObjName() != resolverObjRef.GetObjName() {
				continue
			}
			// Identity of the physical scan: the executed node carries the
			// same object identity the real resolver returned for the fixture
			// table (schema, object name), and its TableDef is named like the
			// catalog object. Widths are not an identity signal.
			require.Equal(t, resolverObjRef.GetSchemaName(), node.ObjRef.GetSchemaName())
			if id := resolverObjRef.GetObj(); id != 0 {
				require.Equal(t, id, node.ObjRef.GetObj(), "physical object id must match the resolver identity")
			}
			if schemaID := resolverObjRef.GetSchema(); schemaID != 0 {
				require.Equal(t, schemaID, node.ObjRef.GetSchema(), "physical schema id must match the resolver identity")
			}
			require.NotNil(t, node.TableDef)
			require.Equal(t, resolverDef.Name, node.TableDef.Name)
			found = true
		}
		require.True(t, found, "expected an executed physical t1 TABLE_SCAN reachable from the plan steps")
	})

	t.Run("f2_witness_s_sibling_is_isolated_from_body_local_x", func(t *testing.T) {
		// s must not read q's body-local x: both names are proven absent from
		// the physical catalog, so the s arm must fail to bind.
		requireNoPhysical(t, "x")
		requireNoPhysical(t, "s")
		_, err := runOneStmt(mock, t, `
			WITH RECURSIVE q(n) AS (
				WITH x AS (SELECT 1 AS a)
				(SELECT a FROM x UNION ALL SELECT n+1 FROM q WHERE n < 2)), s AS (SELECT a FROM x)
			SELECT s.a FROM q JOIN s ON q.n = 1`)
		requireTypedMissing(t, err, "x")
	})

	t.Run("g2_with_wrapped_recursive_union_body", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			WITH RECURSIVE q(n) AS (
				WITH x AS (SELECT 1 AS a)
				(SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3))
			SELECT n FROM q`)
		require.NoError(t, err)
	})

	t.Run("g2_unwrapped_body_control", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			WITH RECURSIVE q(n) AS (
				WITH x AS (SELECT 1 AS a)
				SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3)
			SELECT n FROM q`)
		require.NoError(t, err)
	})
}
