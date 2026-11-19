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

// Review-regression shapes for the correlated-UNION predicate fold: an EXISTS
// subquery over two identical SELECTs whose WHERE clauses carry two outer
// equalities, or one outer inequality, planned fine on the merge base through
// the then-collapsed single operand and must keep planning now that both
// operands stay: each arm export slot leaves the union and each identical arm
// predicate pair folds onto its own output column for the supported
// correlated join above the union.
const unionIdenticalCorrelatedTwoEqualitiesSQL = `SELECT 3 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey = t2.n_regionkey AND t1.n_nationkey = t2.n_nationkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey = t2.n_regionkey AND t1.n_nationkey = t2.n_nationkey));`

const unionIdenticalCorrelatedInequalitySQL = `SELECT 3 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t2.n_nationkey > t1.n_nationkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t2.n_nationkey > t1.n_nationkey));`

const unionIdenticalCorrelatedMixedComparisonSQL = `SELECT 3 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey = t2.n_regionkey AND t2.n_nationkey > t1.n_nationkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey = t2.n_regionkey AND t2.n_nationkey > t1.n_nationkey));`

// Neighboring broken shapes keep the explicit correlated-UNION boundary: an
// arm exporting a different number of slots than its sibling can never pair
// predicates one-to-one, and a range-exposing fold under a scalar consumer
// would surface duplicate user tuples its semantics can observe.
const unionCorrelatedUnpairedExportsControlSQL = `SELECT 3 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey = t2.n_regionkey AND t1.n_nationkey = t2.n_nationkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey = t2.n_regionkey));`

const unionCorrelatedRangeScalarControlSQL = `SELECT t1.n_nationkey + ` +
	`(SELECT 9 FROM tpch.nation t3 WHERE t3.n_regionkey > t1.n_regionkey ` +
	`UNION SELECT 9 FROM tpch.nation t3 WHERE t3.n_regionkey > t1.n_regionkey) ` +
	`FROM tpch.nation t1;`

// TestUnionIdenticalCorrelatedUNIONFoldsPairedPredicates is the review's
// two-equality and one-inequality regression: the correlated distinct UNION
// must stay a real UNION with both operands while every pulled arm predicate
// pair folds onto its own union output slot.
func TestUnionIdenticalCorrelatedUNIONFoldsPairedPredicates(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	cases := []struct {
		name  string
		sql   string
		width int
	}{
		{"two outer equalities", unionIdenticalCorrelatedTwoEqualitiesSQL, 3},
		{"one outer inequality", unionIdenticalCorrelatedInequalitySQL, 2},
		{"equality and inequality mix", unionIdenticalCorrelatedMixedComparisonSQL, 3},
	}
	for _, want := range cases {
		t.Run(want.name, func(t *testing.T) {
			q := buildSetOpChainQuery(t, ctx, want.sql)
			facts := analyzeSetOpChain(t, q)
			require.GreaterOrEqualf(t, facts.unionFamily, 1,
				"the paired-predicate correlated UNION must remain a real set-op node")
			require.GreaterOrEqualf(t, facts.operandLeaves, 2*facts.unionFamily,
				"every retained UNION must keep both operands as inputs")
			node := firstSetOpNodeOfFamily(q, isUnionFamilyNodeType)
			require.NotNil(t, node)
			require.Equalf(t, planpb.Node_UNION, node.NodeType,
				"the fold must not degrade distinct-UNION semantics")
			require.Len(t, node.Children, 2)
			require.Lenf(t, node.ProjectList, want.width,
				"user column plus every folded arm export slot")
			for _, childID := range node.Children {
				input := setOpInputNodeOfPlanQuery(q, childID)
				require.Lenf(t, input.ProjectList, want.width,
					"each arm must export its own copy of every folded slot")
			}
		})
	}
}

// TestUnionIdenticalCorrelatedUNIONUnfoldableShapesKeepBoundary pins the
// negative space of the fold: unpaired exports never plan through a partial
// fold, and consumers that can observe the duplicates a range export keeps
// never receive one silently. The unpaired shape must keep reporting the
// explicit correlated-UNION boundary; the scalar shape must keep failing
// closed (either explicit boundary is acceptable, wrong rows are not).
func TestUnionIdenticalCorrelatedUNIONUnfoldableShapesKeepBoundary(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))

	err := buildSetOpChainQueryAllowError(t, ctx, unionCorrelatedUnpairedExportsControlSQL)
	require.Error(t, err,
		"arms exporting different numbers of slots must never fold partially")
	require.Containsf(t, err.Error(), "correlated local CTE: UNION",
		"unpaired exports must keep the explicit correlated-UNION boundary, got: %v", err)

	require.Error(t,
		buildSetOpChainQueryAllowError(t, ctx, unionCorrelatedRangeScalarControlSQL),
		"a range-exposing correlated UNION under a scalar consumer must keep failing closed")
}

// Mixed-comparison arms: each arm pulls a comparison on the same inner slot,
// but the two ops differ. The pulled predicates no longer run inside the arms,
// so a fold may never adopt the left arm's condition and drop the right arm's
// own semantics; the pair must keep the explicit correlated-UNION boundary.
// The reversed variant (range arm left, equality arm right) pins that the
// rejection is symmetric, not an artifact of which arm was visited first.
const unionCorrelatedMixedEqGreaterControlSQL = `SELECT 3 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t2.n_regionkey = t1.n_regionkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t2.n_regionkey > t1.n_regionkey));`

const unionCorrelatedMixedEqNeqControlSQL = `SELECT 3 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t2.n_regionkey = t1.n_regionkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t2.n_regionkey <> t1.n_regionkey));`

const unionCorrelatedMixedGreaterEqControlSQL = `SELECT 3 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t2.n_regionkey > t1.n_regionkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t2.n_regionkey = t1.n_regionkey));`

// Commutative equality control: mirrored argument order across the two arms
// is the same equality on the same slot and must keep folding.
const unionCorrelatedCommutativeEqualitySQL = `SELECT 3 FROM tpch.nation t1 WHERE ` +
	`EXISTS ((SELECT 1 FROM tpch.nation t2 WHERE t1.n_regionkey = t2.n_regionkey)` +
	` UNION (SELECT 1 FROM tpch.nation t2 WHERE t2.n_regionkey = t1.n_regionkey));`

// TestUnionIdenticalCorrelatedUNIONMixedArmComparisonsKeepBoundary rejects
// every mixed-op pairing: each arm's own pulled condition would be silently
// lost if the fold adopted the other arm's comparison on the shared slot.
func TestUnionIdenticalCorrelatedUNIONMixedArmComparisonsKeepBoundary(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	for name, sql := range map[string]string{
		"left equality, right greater": unionCorrelatedMixedEqGreaterControlSQL,
		"left equality, right neq":     unionCorrelatedMixedEqNeqControlSQL,
		"left greater, right equality": unionCorrelatedMixedGreaterEqControlSQL,
	} {
		t.Run(name, func(t *testing.T) {
			err := buildSetOpChainQueryAllowError(t, ctx, sql)
			require.Error(t, err,
				"arms with different comparison ops must never fold: the "+
					"dropped arm's predicate has no remaining in-arm evaluation")
			require.Containsf(t, err.Error(), "correlated local CTE: UNION",
				"mixed arm comparisons must keep the explicit correlated-UNION boundary, got: %v", err)
		})
	}
}

// TestUnionIdenticalCorrelatedUNIONCommutativeEqualityStillFolds proves the
// rejection of mixed ops is not a blanket textual rule: mirrored equality
// argument order across arms plans and keeps both operands.
func TestUnionIdenticalCorrelatedUNIONCommutativeEqualityStillFolds(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	q := buildSetOpChainQuery(t, ctx, unionCorrelatedCommutativeEqualitySQL)
	node := firstSetOpNodeOfFamily(q, isUnionFamilyNodeType)
	require.NotNil(t, node)
	require.Equalf(t, planpb.Node_UNION, node.NodeType,
		"mirrored equality must keep a distinct UNION")
	require.Len(t, node.Children, 2)
	require.Lenf(t, node.ProjectList, 2,
		"user column plus the shared hidden identity slot")
}
