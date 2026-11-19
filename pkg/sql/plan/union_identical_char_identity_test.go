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

// Review-regression fixture for review 5968847466's second finding: a
// correlated DISTINCT UNION projecting a CHAR column keeps a pad-space-aware
// PhysicalEqualityKeyList for its user columns, and the hidden-identity fold
// appends its shared slot to ProjectList and must append a same-arity,
// same-normalization key entry for it. Otherwise constructDistinctSetGroup's
// CompactPhysicalGroupKeys call rejects the malformed equality tuple later
// and the compiler panics, which the SQL frontend reports as error 20101.

const unionCharCorrelatedInSQL = `select t1.n_nationkey, ` +
	`t1.n_c in (select t2.n_c from tpch.nation_char t2 where t2.n_regionkey = t1.n_regionkey ` +
	`union select t2.n_c from tpch.nation_char t2 where t2.n_regionkey = t1.n_regionkey) ` +
	`Z from tpch.nation_char t1;`

const unionCharIdentityCorrelatedInSQL = `select t1.n_regionkey, ` +
	`t1.n_c in (select t2.n_c from tpch.nation_char t2 where t2.n_c = t1.n_c ` +
	`union select t2.n_c from tpch.nation_char t2 where t2.n_c = t1.n_c) ` +
	`Z from tpch.nation_char t1;`

// firstCorrelatedCharUnionNode finds the correlated distinct UNION that owns
// pad-space physical equality keys. Uncorrelated or non-CHAR chain unions
// without key material are irrelevant here.
func firstCorrelatedCharUnionNode(q *planpb.Query) *planpb.Node {
	for _, node := range q.Nodes {
		if node.NodeType == planpb.Node_UNION && len(node.Children) == 2 &&
			len(node.PhysicalEqualityKeyList) > 0 {
			return node
		}
	}
	return nil
}

// TestUnionIdenticalCorrelatedCharUnionKeepsPhysicalKeyArity proves review
// 5968847466's CHAR regression: a correlated distinct UNION projecting a CHAR
// column keeps its pad-space physical equality tuple aligned with the
// projected row while the hidden identity slot joins it.
func TestUnionIdenticalCorrelatedCharUnionKeepsPhysicalKeyArity(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	q := buildSetOpChainQuery(t, ctx, unionCharCorrelatedInSQL)
	node := firstCorrelatedCharUnionNode(q)
	require.NotNil(t, node,
		"the CHAR correlated distinct UNION must keep its physical equality keys")
	require.Lenf(t, node.ProjectList, 2,
		"one user column plus the shared hidden identity slot")
	require.Lenf(t, node.PhysicalEqualityKeyList, len(node.ProjectList),
		"the appended hidden identity slot must extend the physical "+
			"equality tuple: CompactPhysicalGroupKeys rejects any arity drift")
	userKey := node.PhysicalEqualityKeyList[0]
	require.NotNilf(t, userKey.GetF(),
		"the CHAR projection must keep its cast normalization key")
	require.Equalf(t, "cast", userKey.GetF().Func.ObjName,
		"existing CHAR normalization must not be weakened by the fold")
	identityKey := node.PhysicalEqualityKeyList[1]
	require.NotNilf(t, identityKey.GetCol(),
		"an integer identity slot needs no normalization cast")
}

// Arms keyed through different inner identities never merge onto one shared
// slot: this control pairs a CHAR identity slot (exported as the pad-space
// cast value) with an integer identity slot under the same CHAR projection.
const unionCharMismatchedIdentityControlSQL = `select t1.n_nationkey, ` +
	`t1.n_c in (select t2.n_c from tpch.nation_char t2 where t2.n_c = t1.n_c ` +
	`union select t2.n_c from tpch.nation_char t2 where t2.n_regionkey = t1.n_regionkey) ` +
	`Z from tpch.nation_char t1;`

// TestUnionIdenticalCorrelatedCharIdentityFolds restores review 5968847466's
// CHAR closure: identical arms tied by a CHAR equality must plan, because the
// merge base executed this statement through its then-collapsed single arm,
// and that supported single-arm path is the audited shape: a correlated CHAR
// equality binds its outer side in the pad-space cast (cast(char as varchar))
// while pullup materializes the wrapped inner value as the export slot. The
// paired-predicate fold accepts exactly that comparison shape and keeps both
// operands.
func TestUnionIdenticalCorrelatedCharIdentityFolds(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	q := buildSetOpChainQuery(t, ctx, unionCharIdentityCorrelatedInSQL)
	node := firstCorrelatedCharUnionNode(q)
	require.NotNil(t, node,
		"the CHAR correlated distinct UNION must keep its physical equality keys")
	require.Equalf(t, planpb.Node_UNION, node.NodeType,
		"the identical CHAR identity fold must keep a distinct UNION")
	require.Len(t, node.Children, 2)
	require.Lenf(t, node.ProjectList, 2,
		"one user column plus the shared hidden identity slot")
	require.Lenf(t, node.PhysicalEqualityKeyList, len(node.ProjectList),
		"the folded CHAR identity slot must keep the physical equality tuple aligned")
	userKey := node.PhysicalEqualityKeyList[0]
	require.NotNilf(t, userKey.GetF(),
		"the CHAR projection must keep its cast normalization key")
	identityKey := node.PhysicalEqualityKeyList[1]
	require.NotNilf(t, identityKey.GetCol(),
		"the identity slot already materializes the pad-space cast value, "+
			"so its physical key needs no second normalization cast")
}

// TestUnionIdenticalCorrelatedCharIdentityMismatchKeepsBoundary keeps the
// fold's negative space on the CHAR fixture: a CHAR identity slot never
// merges with a different arm's integer identity slot.
func TestUnionIdenticalCorrelatedCharIdentityMismatchKeepsBoundary(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	err := buildSetOpChainQueryAllowError(t, ctx, unionCharMismatchedIdentityControlSQL)
	require.Error(t, err,
		"arms keyed through different inner identities must never fold")
	require.Containsf(t, err.Error(), "correlated local CTE: UNION",
		"the mismatched identity must keep the explicit correlated-UNION boundary, got: %v", err)
}
