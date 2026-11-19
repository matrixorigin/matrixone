// Copyright 2026 MatrixOne
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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Named typed planner regression for the #31760 compatibility surface.
//
// Before the set-op collector fix, a plain UNION whose two branches were
// textually identical was silently collapsed to a single operand, so the
// correlated EXISTS/IN corpus rows below never reached the planner as real
// set operations and passed. The fix keeps every operand, which is correct;
// the planner must therefore still plan these previously-active statements
// instead of rejecting them, and the retained UNION must stay a distinct
// (non-ALL) set operation with both operands present.
//
// The queries mirror the failing distributed-corpus shapes
// (subquery/subquery-with-exists.sql row 328, subquery/subquery-with-in.sql
// rows 187/799 view variants) mapped onto the seeded mock-catalog fixture
// table tpch.nation(n_nationkey, n_regionkey, n_name). The fixture only
// mocks the catalog: binding, outer-reference resolution, subquery unnest
// and set-op remap all run through BuildPlan for real. This is a typed
// planner proof, not a SQL runtime pass.

const unionIdenticalCorrelatedExistsSQL = `SELECT 2 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey=t2.n_regionkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey = t2.n_regionkey));`

const unionIdenticalCorrelatedInSQL = `select t1.n_nationkey, t1.n_regionkey, ` +
	`t1.n_nationkey in (select t2.n_nationkey from tpch.nation t2 where t2.n_regionkey = t1.n_regionkey ` +
	`union select t2.n_nationkey from tpch.nation t2 where t2.n_regionkey=t1.n_regionkey) ` +
	`Z from tpch.nation t1;`

const unionCorrelatedNonIdenticalControlSQL = `SELECT 2 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey=t2.n_regionkey)` +
	` UNION (SELECT 2 FROM tpch.nation t2 WHERE t1.n_regionkey=t2.n_regionkey));`

const unionIdenticalUncorrelatedControlSQL = `SELECT 2 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2) UNION (SELECT 1 FROM tpch.nation t2));`

const unionCorrelatedMismatchedKeyControlSQL = `SELECT 2 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey=t2.n_regionkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t1.n_nationkey=t2.n_regionkey));`

const unionCorrelatedMismatchedTypeControlSQL = `SELECT 2 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey=t2.n_regionkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t1.n_name=t2.n_name));`

// setOpInputNodeOfPlanQuery unwraps the pure FILTER/ASSERT/SORT/PARTITION
// wrappers a set operation may interleave over its real input, mirroring
// the planner's own set-operation input rule on a detached final plan.
func setOpInputNodeOfPlanQuery(q *planpb.Query, id int32) *planpb.Node {
	node := q.Nodes[id]
	for len(node.BindingTags) == 0 && len(node.Children) == 1 {
		switch node.NodeType {
		case planpb.Node_FILTER, planpb.Node_ASSERT, planpb.Node_SORT, planpb.Node_PARTITION:
			node = q.Nodes[node.Children[0]]
		default:
			return node
		}
	}
	return node
}

// TestUnionIdenticalCorrelatedUNIONUnderSubqueriesPlan is the regression:
// previously-active correlated identical-branch plain UNION statements
// embedded in EXISTS/IN subqueries must plan successfully, keep a
// distinct-UNION node and retain both identical operands.
func TestUnionIdenticalCorrelatedUNIONUnderSubqueriesPlan(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	for name, sql := range map[string]string{
		"exists subquery": unionIdenticalCorrelatedExistsSQL,
		"in subquery":     unionIdenticalCorrelatedInSQL,
	} {
		t.Run(name, func(t *testing.T) {
			q := buildSetOpChainQuery(t, ctx, sql)
			facts := analyzeSetOpChain(t, q)
			require.GreaterOrEqualf(t, facts.unionFamily, 1,
				"the identical correlated UNION must remain a real UNION-family set-op node")
			require.GreaterOrEqualf(t, facts.operandLeaves, 2*facts.unionFamily,
				"every retained UNION must keep both identical operands as inputs")
			node := firstSetOpNodeOfFamily(q, isUnionFamilyNodeType)
			require.NotNil(t, node)
			require.Equalf(t, planpb.Node_UNION, node.NodeType,
				"deduplicated UNION semantics must not degenerate into UNION_ALL: "+
					"distinct, NULL and multiplicity behavior must be preserved")
			require.Len(t, node.Children, 2,
				"a plain UNION node takes both collected branch plans as children")
			// Both corpus bodies expose one SQL column; the UNION output adds
			// the shared hidden identity slot the consuming join references.
			const wantWidth = 2
			require.Lenf(t, node.ProjectList, wantWidth,
				"the UNION output must carry the shared hidden identity slot")
			for _, childID := range node.Children {
				input := setOpInputNodeOfPlanQuery(q, childID)
				require.Lenf(t, input.ProjectList, wantWidth,
					"a UNION input must expose the union-width output so the "+
						"consuming join references remap through the same slot")
			}
		})
	}
}

// TestUnionIdenticalCorrelatedUNIONAttributionControls pins the guard's
// structural nature: identity, not branch text, decides plannability. The
// uncorrelated identical variant stays plannable; a correlated
// non-identical variant with the same common identity key must also plan;
// arms whose outer equality identity misaligns keep the explicit
// correlated-UNION boundary so they can never plan or execute wrong rows.
func TestUnionIdenticalCorrelatedUNIONAttributionControls(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))

	require.NoError(t,
		buildSetOpChainQueryAllowError(t, ctx, unionIdenticalUncorrelatedControlSQL),
		"an identical-branch UNION inside EXISTS without outer references must stay plannable")

	require.NoError(t,
		buildSetOpChainQueryAllowError(t, ctx, unionCorrelatedNonIdenticalControlSQL),
		"a correlated DISTINCT UNION whose arms share the same depth-1 outer "+
			"equality identity must plan: the guard is structural, not textual")

	for name, sql := range map[string]string{
		"mismatched outer equality key": unionCorrelatedMismatchedKeyControlSQL,
		"mismatched key and slot type":  unionCorrelatedMismatchedTypeControlSQL,
	} {
		t.Run(name, func(t *testing.T) {
			err := buildSetOpChainQueryAllowError(t, ctx, sql)
			require.Error(t, err,
				"misaligned branch identities must not plan or execute")
			require.Containsf(t, err.Error(), "correlated local CTE: UNION",
				"rejection must remain the explicit correlated-UNION identity boundary, got: %v", err)
		})
	}
}
