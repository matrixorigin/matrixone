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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// Issue #32123 regression. A set operation whose value-bearing branches all
// project legal BIT columns must keep the BIT domain at the widest declared
// width: the coalesce fallback resolved such a column to the integer domain
// and dropped the declared width, which changed LENGTH bytes and outer
// predicates on the materialized result. The queries below go through the
// real planner build root (parse + BuildPlan with the mock compiler
// context), so the assertions cover the branch casts and set-node output
// types the executor will actually run, not an isolated helper.
//
// The same common-type owner feeds UNION, INTERSECT and MINUS node
// construction (appendSetOperationNode builds every node from the already
// aligned branches), so the INTERSECT/MINUS subtests close those real
// consumers and the typed PhysicalEqualityKeyList built from them.
func TestUnionPureBitBranchesKeepBitDomain(t *testing.T) {
	// Local twin of docs_vec_raw with its three payload columns retyped to
	// BIT(8)/BIT(16)/BIT(64), registered only on this test's private context:
	// the shared mock schema and every original expectation stay untouched.
	// The twin keeps the original column names (id/embedding/payload) so the
	// copied primary-key and name metadata stays consistent; only the types
	// and widths change. Name mapping: id=BIT(8), embedding=BIT(16),
	// payload=BIT(64).
	const twinName = "union_bit_width_local"
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(true, proc)

	baseRef, baseDef, err := ctx.Resolve("constraint_test", "docs_vec_raw", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, baseRef, "docs_vec_raw must resolve with an object reference")
	require.NotNil(t, baseDef, "docs_vec_raw must resolve in the mock schema")
	twin := DeepCopyTableDef(baseDef, true)
	twin.Name = twinName

	retypeWidth := map[string]int32{
		"id":        8,
		"embedding": 16,
		"payload":   64,
	}
	retyped := 0
	for _, col := range twin.Cols {
		want, ok := retypeWidth[col.Name]
		if !ok {
			continue
		}
		col.Typ.Id = int32(types.T_bit)
		col.Typ.Scale = 0
		col.Typ.Width = want
		retyped++
	}
	require.Equal(t, len(retypeWidth), retyped, "the twin must carry all three BIT widths")

	qualified := mockQualifiedTableName("constraint_test", twinName)
	ctx.tablesByQualifiedName[qualified] = twin
	ctx.tables[twinName] = twin
	// Seed the object reference the same way as the vecf64 twin: with a nil
	// objRef, resolve paths that dereference PubInfo would panic.
	twinRef := *baseRef
	twinRef.ObjName = twinName
	ctx.objectsByQualifiedName[qualified] = &twinRef
	ctx.objects[twinName] = &twinRef

	buildQuery := func(t *testing.T, sql string) *plan.Query {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := BuildPlan(ctx, stmt, false, nil)
		require.NoError(t, err)
		return p.GetQuery()
	}
	requireBit := func(t *testing.T, e *plan.Expr, width int32, msg string) {
		t.Helper()
		require.Equal(t, int32(types.T_bit), e.Typ.Id, msg)
		require.EqualValues(t, width, e.Typ.Width, msg)
	}
	requireNotBit := func(t *testing.T, e *plan.Expr, msg string) {
		t.Helper()
		require.NotEqual(t, int32(types.T_bit), e.Typ.Id, msg)
	}
	unionNodes := func(query *plan.Query, nodeType plan.Node_NodeType) []*plan.Node {
		var found []*plan.Node
		for _, node := range query.Nodes {
			if node.NodeType == nodeType {
				found = append(found, node)
			}
		}
		return found
	}
	branches := func(t *testing.T, query *plan.Query, u *plan.Node) []*plan.Node {
		t.Helper()
		require.NotEmpty(t, u.Children)
		out := make([]*plan.Node, 0, len(u.Children))
		for _, id := range u.Children {
			child := query.Nodes[id]
			require.NotEmpty(t, child.ProjectList)
			out = append(out, child)
		}
		return out
	}

	t.Run("mixed-width-picks-widest-and-casts-narrower-branch", func(t *testing.T) {
		query := buildQuery(t, "select id as d from union_bit_width_local union all select embedding from union_bit_width_local")
		unions := unionNodes(query, plan.Node_UNION_ALL)
		require.Len(t, unions, 1)
		u := unions[0]
		requireBit(t, u.ProjectList[0], 16, "the set output must keep the widest BIT domain")
		childs := branches(t, query, u)
		require.Len(t, childs, 2)
		castBranch, refBranch := 0, 0
		for _, child := range childs {
			requireBit(t, child.ProjectList[0], 16, "every branch projection must be widened to the common bit width")
			switch {
			case child.ProjectList[0].GetF() != nil:
				castBranch++
			case child.ProjectList[0].GetCol() != nil:
				refBranch++
			}
		}
		require.Equal(t, 1, castBranch, "only the narrower bit(8) branch gains an in-place bit->bit widening cast")
		require.Equal(t, 1, refBranch, "the already-widest bit(16) branch must stay a column reference")
	})

	t.Run("branch-order-does-not-change-common-width", func(t *testing.T) {
		query := buildQuery(t, "select embedding as d from union_bit_width_local union all select id from union_bit_width_local")
		unions := unionNodes(query, plan.Node_UNION_ALL)
		require.Len(t, unions, 1)
		u := unions[0]
		requireBit(t, u.ProjectList[0], 16, "the widest source width wins regardless of branch order")
		for _, child := range branches(t, query, u) {
			requireBit(t, child.ProjectList[0], 16, "every branch projection must be widened to the common bit width")
		}
	})

	t.Run("same-width-keeps-width-without-redundant-cast", func(t *testing.T) {
		query := buildQuery(t, "select id as d from union_bit_width_local union all select id from union_bit_width_local")
		unions := unionNodes(query, plan.Node_UNION_ALL)
		require.Len(t, unions, 1)
		u := unions[0]
		requireBit(t, u.ProjectList[0], 8, "a same-width pure-BIT union must keep bit(8)")
		for _, child := range branches(t, query, u) {
			requireBit(t, child.ProjectList[0], 8, "same-width branches must not gain a widening cast")
			require.NotNil(t, child.ProjectList[0].GetCol(),
				"an exactly-aligned branch must stay a column reference with no cast")
		}
	})

	t.Run("three-branches-choose-bit64-target", func(t *testing.T) {
		query := buildQuery(t,
			"select id as d from union_bit_width_local union all select embedding from union_bit_width_local union all select payload from union_bit_width_local")
		unions := unionNodes(query, plan.Node_UNION_ALL)
		require.NotEmpty(t, unions)
		for _, u := range unions {
			requireBit(t, u.ProjectList[0], 64,
				"every set node of the pure-BIT chain must expose the widest branch width")
		}
	})

	t.Run("union-distinct-keeps-bit-domain", func(t *testing.T) {
		query := buildQuery(t, "select id as d from union_bit_width_local union select embedding from union_bit_width_local")
		unions := unionNodes(query, plan.Node_UNION)
		require.Len(t, unions, 1)
		u := unions[0]
		requireBit(t, u.ProjectList[0], 16, "UNION DISTINCT keeps the same pure-BIT common type")
		for _, child := range branches(t, query, u) {
			requireBit(t, child.ProjectList[0], 16, "every branch projection must be widened to the common bit width")
		}
	})

	t.Run("intersect-keeps-bit-domain", func(t *testing.T) {
		query := buildQuery(t, "select id as d from union_bit_width_local intersect select embedding from union_bit_width_local")
		intersects := unionNodes(query, plan.Node_INTERSECT)
		require.Len(t, intersects, 1)
		u := intersects[0]
		requireBit(t, u.ProjectList[0], 16, "INTERSECT keeps the pure-BIT common type built by the shared owner")
		// A pure-BIT set has no pad-space-sensitive column, so it needs no
		// separate physical equality key: T_bit already has exactly one
		// physical representation and INTERSECT compares rows through the
		// typed project list above. buildPadSpacePhysicalKeyList deliberately
		// returns nil for that case.
		require.Empty(t, u.PhysicalEqualityKeyList,
			"only pad-space-sensitive columns need a separate physical equality key list")
		for _, child := range branches(t, query, u) {
			requireBit(t, child.ProjectList[0], 16, "every INTERSECT branch projection must be widened to the common bit width")
		}
	})

	t.Run("minus-keeps-bit-domain", func(t *testing.T) {
		query := buildQuery(t, "select id as d from union_bit_width_local minus select embedding from union_bit_width_local")
		minuses := unionNodes(query, plan.Node_MINUS)
		require.Len(t, minuses, 1)
		u := minuses[0]
		requireBit(t, u.ProjectList[0], 16, "MINUS keeps the pure-BIT common type built by the shared owner")
		// Same as INTERSECT: no pad-space-sensitive column means the physical
		// equality key list stays empty and comparison runs on the typed
		// project domain.
		require.Empty(t, u.PhysicalEqualityKeyList,
			"only pad-space-sensitive columns need a separate physical equality key list")
		for _, child := range branches(t, query, u) {
			requireBit(t, child.ProjectList[0], 16, "every MINUS branch projection must be widened to the common bit width")
		}
	})

	t.Run("untyped-null-branch-is-neutral", func(t *testing.T) {
		query := buildQuery(t, "select null as d from union_bit_width_local union all select id from union_bit_width_local")
		unions := unionNodes(query, plan.Node_UNION_ALL)
		require.Len(t, unions, 1)
		u := unions[0]
		requireBit(t, u.ProjectList[0], 8, "an untyped NULL branch must not change the BIT domain or width")
		nullBranch, refBranch := 0, 0
		for _, child := range branches(t, query, u) {
			requireBit(t, child.ProjectList[0], 8, "every branch must expose the typed common width")
			if lit := child.ProjectList[0].GetLit(); lit != nil && lit.GetIsnull() {
				nullBranch++
			}
			if child.ProjectList[0].GetCol() != nil {
				refBranch++
			}
		}
		require.Equal(t, 1, nullBranch, "the untyped NULL branch must carry the set's concrete BIT type")
		require.Equal(t, 1, refBranch, "the typed BIT branch must stay a column reference")
	})

	t.Run("all-null-branches-keep-historical-text-fallback", func(t *testing.T) {
		query := buildQuery(t, "select null as d from union_bit_width_local union all select null as d from union_bit_width_local")
		unions := unionNodes(query, plan.Node_UNION_ALL)
		require.Len(t, unions, 1)
		u := unions[0]
		require.Equal(t, int32(types.T_text), u.ProjectList[0].Typ.Id,
			"an all-NULL set must keep its historical concrete text result")
		for _, child := range branches(t, query, u) {
			require.Equal(t, int32(types.T_text), child.ProjectList[0].Typ.Id,
				"all-NULL branches must not be retyped by the pure-BIT rule")
		}
	})

	t.Run("mixed-bit-and-uint64-stays-out-of-bit-specialization", func(t *testing.T) {
		query := buildQuery(t, "select id as d from union_bit_width_local union all select cast(258 as unsigned)")
		unions := unionNodes(query, plan.Node_UNION_ALL)
		require.Len(t, unions, 1)
		requireNotBit(t, unions[0].ProjectList[0],
			"pure-BIT specialization must not leak into a mixed BIT/uint64 column")
	})

	t.Run("mixed-bit-and-varchar-stays-out-of-bit-specialization", func(t *testing.T) {
		query := buildQuery(t, "select id as d from union_bit_width_local union all select 'x'")
		unions := unionNodes(query, plan.Node_UNION_ALL)
		require.Len(t, unions, 1)
		requireNotBit(t, unions[0].ProjectList[0],
			"pure-BIT specialization must not leak into a mixed BIT/varchar column")
	})

	t.Run("plain-coalesce-keeps-numeric-resolution", func(t *testing.T) {
		query := buildQuery(t, "select coalesce(id, 255) as d from union_bit_width_local")
		require.NotEmpty(t, query.Steps)
		root := query.Nodes[query.Steps[len(query.Steps)-1]]
		requireNotBit(t, root.ProjectList[0],
			"a plain COALESCE expression must keep its existing non-BIT resolution")
	})

	t.Run("single-branch-select-keeps-source-bit-column", func(t *testing.T) {
		// A lone BIT projection (the per-branch precondition of this defect)
		// must keep its own declared width and stay a column reference: the
		// set-operation path never narrows it before the specialization runs.
		query := buildQuery(t, "select embedding as d from union_bit_width_local")
		require.NotEmpty(t, query.Steps)
		root := query.Nodes[query.Steps[len(query.Steps)-1]]
		requireBit(t, root.ProjectList[0], 16, "a lone BIT(16) projection keeps its declared width")
		require.NotNil(t, root.ProjectList[0].GetCol(), "a lone BIT projection stays a column reference")
	})
}
