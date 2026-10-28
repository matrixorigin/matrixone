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

package compile

// Round-10 (pre-PR review round 10) members for the two lock-marking P2
// findings:
//
// Finding 1 "Honor effective historical eligibility before choosing
// promoted locks" (D-1/R8-F3/R9-F3): bind-time presence
// (plan.LockTarget.LockingRead) records that an occurrence declared a lock
// under an explicit snapshot; the occurrence's EFFECTIVE eligibility — does
// its snapshot ENGAGE the historical cohort for THIS execution — resolves
// per execution at scan admission (executionReadScanAdmitted). A promoted
// locked occurrence that is historical for this execution has its
// current-read machinery withheld exactly as today, so the staged
// pre-pipeline lock options must derive from the EFFECTIVE per-execution
// admission: markedPrePipelineLockTables must answer empty for a statement
// whose only occurrence engages the historical cohort (H != 0 and strictly
// earlier than the executing SnapshotTS), and must retain the marked options
// for every eligible occurrence class (ordinary outer, non-engaging H >= S).
//
// Finding 2 "Preserve lock marks per occurrence instead of expanding by
// table ID" (D-1's per-target representation, R4-F2/R5-F1's same-table
// identity): a LEFT-lowered same-table scalar-subquery yields ONE LockOp
// with TWO occurrences of one table — the outer occurrence marked
// current-read, the nested subquery occurrence snapshot-bound (unmarked).
// Presence travels per occurrence through construction, the attempt stamp
// sweep, Reset retention and the wire serialization — never as a table-ID
// set that would hand the nested occurrence's lock the marked
// suppression/retry contract.

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// The staged pre-pipeline options follow EFFECTIVE per-execution
// occurrence eligibility: an engaging explicit historical snapshot (H nonzero
// and strictly earlier than the executing SnapshotTS — the same predicate
// that refuses the scan at admission and routes the read through the
// historical clone) keeps the LEGACY options, while every eligible occurrence
// class keeps the marked options the round-8/9 contracts promise.
func TestHistoricalStagedLockOptionsKeepLegacySemantics(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	historicalScan := func(h *timestamp.Timestamp) *plan.Node {
		return &plan.Node{
			NodeId:       0,
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     &plan.TableDef{TableType: "r", TblId: 42},
			ScanReadRole: plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot,
			ScanSnapshot: &plan.Snapshot{TS: h},
		}
	}

	// An ENGAGING explicit snapshot (H < SnapshotTS): the occurrence stays
	// historical for this whole execution, so the promoted staged lock
	// keeps today's legacy options — the presence-derived table-ID set is
	// not enough to take the marked acquisition.
	hEngaged := timestamp.Timestamp{PhysicalTime: 50, LogicalTime: 1}
	scan := historicalScan(&hEngaged)
	c.pn = presencePlan(scan)
	require.False(t, c.executionReadScanAdmitted(scan),
		"the engaging historical occurrence is withheld from this execution's current read (D-1/R8-F3)")
	require.Empty(t, c.markedPrePipelineLockTables(),
		"a historical-only locked statement keeps today's LEGACY pre-pipeline lock options")

	// The eligible current-read control: an ordinary outer occurrence
	// keeps the marked staged options.
	ordinary := reviewMarkedScan(0, "t_ordinary")
	c.pn = presencePlan(ordinary)
	require.True(t, c.executionReadScanAdmitted(ordinary),
		"the ordinary outer occurrence is an admitted current read")
	marked := c.markedPrePipelineLockTables()
	require.Contains(t, marked, uint64(42),
		"the eligible occurrence keeps the marked pre-pipeline options (round-8 contract)")

	// A NON-engaging explicit snapshot (H >= SnapshotTS) current-reads for
	// this execution and keeps the marked staged options.
	hLater := timestamp.Timestamp{PhysicalTime: 150, LogicalTime: 1}
	nonEngaging := historicalScan(&hLater)
	c.pn = presencePlan(nonEngaging)
	require.True(t, c.executionReadScanAdmitted(nonEngaging),
		"the non-engaging snapshot (H >= SnapshotTS) stays an admitted current read")
	marked = c.markedPrePipelineLockTables()
	require.Contains(t, marked, uint64(42),
		"the non-engaging occurrence keeps the marked pre-pipeline options")
}

// Presence stays occurrence-scoped through the whole delivered lifecycle:
// construction (the plan target's own LockingRead), the attempt stamp, the
// retained pipeline's Reset, and the wire serialization. The LEFT-lowered
// same-table shape — one marked outer occurrence and one snapshot-bound
// nested occurrence of the SAME table in one LockOp — must never see the
// nested occurrence inherit the outer occurrence's mark (no table-ID
// expansion), matching design D-1's per-target representation.
func TestLockMarksStayPerOccurrenceAcrossStampResetAndWire(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	outer := &plan.LockTarget{TableId: 42, LockingRead: true}
	nested := &plan.LockTarget{TableId: 42}
	arg, err := constructLockOp(&plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{outer, nested},
	}, nil)
	require.NoError(t, err)
	defer arg.Release()

	// Unstamped: marks stay inert without this attempt's frontier.
	targets := arg.CopyToPipelineTarget()
	require.False(t, targets[0].GetLockingRead())
	require.False(t, targets[1].GetLockingRead())

	// This attempt's stamp publishes the frontier: ONLY the marked
	// occurrence carries presence on the wire; the nested same-table
	// occurrence never inherits the outer mark.
	arg.SetLockingReadCurrentFrontier(timestamp.Timestamp{PhysicalTime: 777, LogicalTime: 5}, sessionTS)
	targets = arg.CopyToPipelineTarget()
	require.True(t, targets[0].GetLockingRead(),
		"the marked occurrence carries presence on the wire")
	require.False(t, targets[1].GetLockingRead(),
		"the nested same-table occurrence stays unmarked — presence is per occurrence, never expanded by table ID")
	require.Equal(t, int64(777), targets[0].GetCurrentReadFrontierPhysical())
	require.Equal(t, uint32(5), targets[0].GetCurrentReadFrontierLogical())

	// Reset retires the attempt state with the retained physical
	// generation; per-occurrence provenance survives.
	arg.Reset(c.proc, false, nil)
	targets = arg.CopyToPipelineTarget()
	require.False(t, targets[0].GetLockingRead(), "unstamped-after-reset: presence is inert")
	require.False(t, targets[1].GetLockingRead())

	// The next attempt's stamp sweep serves exactly the marked occurrence:
	// the sweep cannot degrade per-occurrence provenance back into a
	// same-table expansion.
	c.pn = presencePlan(reviewMarkedScan(0, "t_stamp"))
	c.scopes = []*Scope{{RootOp: arg}}
	c.execReadDisabled = false
	c.execReadFrontierSet = true
	c.execReadFrontier = timestamp.Timestamp{PhysicalTime: 333, LogicalTime: 3}
	c.executionReadStampMarkingLockScopes()
	targets = arg.CopyToPipelineTarget()
	require.True(t, targets[0].GetLockingRead(),
		"the stamped attempt serves the marked occurrence with this attempt's frontier")
	require.False(t, targets[1].GetLockingRead(),
		"the attempt stamp keeps occurrence specificity — no same-table expansion")
	require.Equal(t, int64(333), targets[0].GetCurrentReadFrontierPhysical())
	require.Equal(t, uint32(3), targets[0].GetCurrentReadFrontierLogical())
}
