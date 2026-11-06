// Copyright 2026 MatrixOne
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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Pre-PR review round 4, finding 1: tryHintedCoveringIndexScan fabricated its
// covering index scan without the locked occurrence's ScanReadRole and
// without IndexScanInfo.ParentTableId, although every sibling fabrication
// site propagates both. A covering FORCE INDEX FOR ORDER BY locking query
// therefore received a zero-role index source — a vanished mark: compile's
// D-10.2 covered-index admission rejects it and the locking read silently
// keeps the snapshot. This member drives the REAL constructor through the
// production parser + BuildPlan with a live FORCE INDEX FOR ORDER BY hint on
// a covering locking query and asserts the fabricated scan carries its
// owning base table's id and the source occurrence's role.
func TestHintedCoveringLockingScanCarriesSourceProvenance(t *testing.T) {
	proc := newPlanTestProcess(t)

	const coveringLockingSQL = `
		select id, a, b from index_hint_t force index for order by(idx_ab)
		where a = 1 and b between 10 and 20 order by b, id for update`

	mock := NewMockOptimizer(true, proc)
	addIndexHintChoiceTableForTest(mock)
	baseTableID := mock.ctxt.tables["index_hint_t"].TblId
	require.NotZero(t, baseTableID, "the fixture registers index_hint_t with a nonzero id")

	queryPlan, err := runOneStmt(mock, t, coveringLockingSQL)
	require.NoError(t, err)
	cover := findFirstIndexScanNode(queryPlan)
	require.NotNil(t, cover,
		"the ORDER-BY force requirement routes the locking query through the covering-index constructor")
	require.True(t, cover.IndexScanInfo.IsIndexScan)
	require.Equal(t, "idx_ab", cover.IndexScanInfo.IndexName)
	require.Equal(t, int64(baseTableID), cover.IndexScanInfo.ParentTableId,
		"the fabricated covering scan names its owning locked base table (D-10.2 admission)")
	require.Equal(t, plan.ScanReadRole_ScanOuterLocking, cover.GetScanReadRole(),
		"the covering scan carries the locked occurrence's source role (R13-F3): a zero role is a vanished mark")

	q := queryPlan.GetQuery()
	require.NotNil(t, q)
	require.True(t, PlanHasLockingCurrentRead(q),
		"the hinted covering locking plan still carries its locking-read presence end to end")
	var lockTargets []*plan.LockTarget
	for _, n := range q.Nodes {
		if n.NodeType == plan.Node_LOCK_OP {
			lockTargets = append(lockTargets, n.LockTargets...)
		}
	}
	require.NotEmpty(t, lockTargets,
		"the LOCK_OP of the covering locking query keeps its base-table lock target")
	for _, target := range lockTargets {
		require.Equal(t, baseTableID, target.GetTableId())
	}
}

// The same coherence contract over COMPLETE production builds (review
// round 4, finding 2; rev 15, R15-F1: the below-gate half is removed with
// the capability gate): every outer-locking captured role pairs with a
// presence-marked lock target and every presence target belongs to an
// outer-locking scan — role and presence can never disagree in either
// direction.
func TestFullBuildLockingPresenceMatchesCapturedRoles(t *testing.T) {
	const lockingSQL = "select a from index_hint_t where a = 1 for update"

	requirePresenceMatchesRoles := func(q *plan.Query) {
		require.NotNil(t, q)
		// Set invariants, robust to rewrite shapes: index rewrites may
		// fabricate additional outer-locking source scans (covered-index
		// provenance, D-10.2) and consolidate their marks into the base
		// table's single lock target — but presence and captured
		// outer-locking roles can never disagree in EITHER direction.
		var outerLockingRoles int
		for _, n := range q.Nodes {
			switch n.GetScanReadRole() {
			case plan.ScanReadRole_ScanOuterLocking,
				plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot:
				outerLockingRoles++
			}
		}
		var presenceTargets int
		for _, n := range q.Nodes {
			for _, target := range n.LockTargets {
				if target.GetLockingRead() {
					presenceTargets++
				}
			}
		}
		require.Equal(t, presenceTargets > 0, outerLockingRoles > 0,
			"a captured outer-locking role exists in a plan if and only if the plan carries locking-read presence: one coherent binding decision")
	}

	proc := newPlanTestProcess(t)
	mock := NewMockOptimizer(true, proc)
	addIndexHintChoiceTableForTest(mock)
	queryPlan, err := runOneStmt(mock, t, lockingSQL)
	require.NoError(t, err)
	require.True(t, PlanHasLockingCurrentRead(queryPlan.GetQuery()))
	requirePresenceMatchesRoles(queryPlan.GetQuery())
}
