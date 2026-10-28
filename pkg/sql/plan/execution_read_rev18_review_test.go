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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// setLockingCurrentReadGate pins the service runtime's negotiated protocol
// version, restoring the original value when the test ends. The gate is read
// LIVE at every binding decision (D-7), so flipping this variable between two
// production calls simulates a capability transition exactly at that seam.
func setLockingCurrentReadGate(t *testing.T, proc *process.Process, version int64) {
	t.Helper()
	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)
	orig, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, orig)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
}

// Pre-PR review round 4, finding 1: tryHintedCoveringIndexScan fabricated its
// covering index scan without the locked occurrence's ScanReadRole and
// without IndexScanInfo.ParentTableId, although every sibling fabrication
// site propagates both. A covering FORCE INDEX FOR ORDER BY locking query
// therefore received a zero-role index source — a vanished mark: compile's
// D-10.2 covered-index admission rejects it and the locking read silently
// keeps the snapshot. This member drives the REAL constructor through the
// production parser + BuildPlan with a live FORCE INDEX FOR ORDER BY hint on
// a covering locking query and asserts the fabricated scan carries its
// owning base table's id and the source occurrence's role — above the gate,
// where those fields carry semantics.
func TestHintedCoveringLockingScanCarriesSourceProvenance(t *testing.T) {
	proc := newPlanTestProcess(t)
	setLockingCurrentReadGate(t, proc, defines.MORPCVersion135)

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

	// Below the capability gate the SAME locking hint builds the same hinted
	// covering shape — with typed provenance intact and the uniform legacy
	// roles/presence zero form (D-1's representation closure): a below-gate
	// plan stays wire-identical to today's. Provenance propagation is
	// capability-independent.
	setLockingCurrentReadGate(t, proc, 128)
	mock = NewMockOptimizer(true, proc)
	addIndexHintChoiceTableForTest(mock)
	queryPlan, err = runOneStmt(mock, t, coveringLockingSQL)
	require.NoError(t, err)
	cover = findFirstIndexScanNode(queryPlan)
	require.NotNil(t, cover)
	require.Equal(t, int64(baseTableID), cover.IndexScanInfo.ParentTableId,
		"the owning-table provenance is capability-independent")
	require.Equal(t, plan.ScanReadRole_ScanNestedBlock, cover.GetScanReadRole(),
		"below-gate plans keep the legacy zero role on every scan")
	q = queryPlan.GetQuery()
	require.False(t, PlanHasLockingCurrentRead(q),
		"below-gate builds never carry locking-read presence: uniform legacy execution")
	for _, n := range q.Nodes {
		for _, target := range n.LockTargets {
			require.False(t, target.GetLockingRead())
		}
	}
}

// Pre-PR review round 4, finding 2: lock-target presence used to be derived
// behind a SECOND live capability poll inside collectLockTargets, after the
// scan's role had already been captured under the first poll. A capability
// drop landing exactly between construction and collection left an
// OuterLocking role with LockingRead=false — a mixed execution that bypassed
// capability rejection while compile still admitted the scan from its role.
// The coherent decision derives presence from the CAPTURED role alone, so
// role and presence can never disagree. This member simulates the drop at
// exactly that seam: it captures the role while the gate is active, flips the
// live gate, then runs the production collection.
func TestLockTargetPresenceFollowsCapturedRoleAcrossMidBindGateDrop(t *testing.T) {
	proc := newPlanTestProcess(t)
	setLockingCurrentReadGate(t, proc, defines.MORPCVersion135)
	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)

	mock := NewMockOptimizer(true, proc)
	b := NewQueryBuilder(plan.Query_SELECT, mock.CurrentContext(), false, true)
	intType := plan.Type{Id: int32(types.T_int32)}
	// A scan whose role was captured while the gate was active.
	captured := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{TableType: "r", TblId: 25356, Name: "index_hint_t",
			Cols: []*plan.ColDef{{Name: "id", Typ: intType, Primary: true}, {Name: "a", Typ: intType}},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}},
		BindingTags:  []int32{7},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	}
	captured.NodeId = int32(len(b.qry.Nodes))
	b.qry.Nodes = append(b.qry.Nodes, captured)
	b.ctxByNode = append(b.ctxByNode, NewBindContext(b, nil))

	// The capability drop lands BETWEEN construction and collection.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(128))
	targets := b.collectLockTargets(captured.NodeId, nil)
	require.Len(t, targets, 1, "the primary-key scan contributes exactly one lock target")
	require.True(t, targets[0].GetLockingRead(),
		"presence follows the captured role across the mid-bind capability drop: one coherent binding decision")
	require.Equal(t, plan.ScanReadRole_ScanOuterLocking, captured.GetScanReadRole(),
		"the captured role itself is never mutated by collection")

	// The same captured role keeps its presence while the gate stays high.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	targets = b.collectLockTargets(captured.NodeId, nil)
	require.Len(t, targets, 1)
	require.True(t, targets[0].GetLockingRead())

	// A legacy-role scan never acquires presence in any gate state.
	captured.ScanReadRole = plan.ScanReadRole_ScanNestedBlock
	for _, version := range []int64{128, defines.MORPCVersion135} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		targets = b.collectLockTargets(captured.NodeId, nil)
		require.Len(t, targets, 1)
		require.False(t, targets[0].GetLockingRead(),
			"only an above-gate captured role can ever mark presence")
	}
}

// The same coherence contract over COMPLETE production builds in both gate
// states (review round 4, finding 2): above the gate every outer-locking
// captured role pairs with a presence-marked lock target and every presence
// target belongs to an outer-locking scan; below the gate no plan carries
// either form — roles stay legacy zero and targets stay presence-free, the
// byte-identical legacy shape.
func TestFullBuildLockingPresenceMatchesCapturedRolesBothGateStates(t *testing.T) {
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
	setLockingCurrentReadGate(t, proc, defines.MORPCVersion135)
	mock := NewMockOptimizer(true, proc)
	addIndexHintChoiceTableForTest(mock)
	queryPlan, err := runOneStmt(mock, t, lockingSQL)
	require.NoError(t, err)
	require.True(t, PlanHasLockingCurrentRead(queryPlan.GetQuery()))
	requirePresenceMatchesRoles(queryPlan.GetQuery())

	setLockingCurrentReadGate(t, proc, 128)
	mock = NewMockOptimizer(true, proc)
	addIndexHintChoiceTableForTest(mock)
	queryPlan, err = runOneStmt(mock, t, lockingSQL)
	require.NoError(t, err)
	require.False(t, PlanHasLockingCurrentRead(queryPlan.GetQuery()),
		"a below-gate build is uniformly legacy: no presence anywhere")
	requirePresenceMatchesRoles(queryPlan.GetQuery())
	for _, n := range queryPlan.GetQuery().Nodes {
		require.NotEqual(t, plan.ScanReadRole_ScanOuterLocking, n.GetScanReadRole())
		require.NotEqual(t, plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, n.GetScanReadRole())
	}
}
