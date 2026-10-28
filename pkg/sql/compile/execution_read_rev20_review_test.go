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

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/stretchr/testify/require"
)

// Pre-PR review round 7, finding 1 (design D-8's bind-time presence
// immutability, R13-F3's covering admission premise): a table-lock
// promotion (applyLockTableFallback over a small MaxLockRowCount budget,
// or the DDL new-table path) moves only the PHYSICAL disposition — the
// promoted target is handed to c.lockTables for the pre-pipeline
// acquisition and stays out of the lock operator's row list. The canonical
// plan's LockTargets keep their bind-time marked provenance, because the
// Run capability validation, lockingTargetMarked's covering parent
// admission and the session/prepared cache marking all read the canonical
// plan. These members drive the REAL compileLock over a hand-promoted
// Shared target (exactly the shape the plan pass emits) plus an ordinary
// marked row target, and pin both sides of the review's contract: the
// physical table-lock disposition happens, and the logical presence
// survives compilation.
func TestCompileLockKeepsPromotedMarkedTargetsThroughPhysicalDisposition(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.anal = &AnalyzeModule{}
	c.lockTables = make(map[uint64]*plan.LockTarget)

	// The shape applyLockTableFallback emits for a shared statement promoted
	// above a small MaxLockRowCount: the SAME target object flips LockTable
	// while keeping its bind-time marked LockingRead.
	promoted := &plan.LockTarget{
		TableId:     7,
		Mode:        lock.LockMode_Shared,
		LockingRead: true,
		LockTable:   true,
	}
	rowTarget := &plan.LockTarget{
		TableId:     8,
		Mode:        lock.LockMode_Exclusive,
		LockingRead: true,
	}
	lockNode := &plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{promoted, rowTarget},
	}
	markedScan := &plan.Node{
		NodeId:       0,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r", TblId: 7},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	}
	covering := scanNode(plan.ScanReadRole_ScanOuterLocking, catalog.SystemIndexRel, nil)
	covering.IndexScanInfo = plan.IndexScanInfo{ParentTableId: 7}
	qry := &plan.Query{Nodes: []*plan.Node{markedScan, covering, lockNode}}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}

	ss, err := c.compileLock(lockNode, []*Scope{{}})
	require.NoError(t, err)

	// The physical table-lock disposition: the promoted Shared target goes
	// to c.lockTables and never reaches the row-lock operator.
	staged, seen := c.lockTables[7]
	require.True(t, seen, "the promoted target's table disposition is staged")
	require.True(t, staged.GetLockTable())
	require.Equal(t, lock.LockMode_Shared, staged.GetMode())
	_, seen = c.lockTables[8]
	require.False(t, seen, "the ordinary row target never becomes a table lock")

	// The logical provenance survives compilation byte-for-byte: the
	// canonical node keeps exactly the marked targets the plan carried in,
	// by pointer, with their marks intact.
	require.Len(t, lockNode.LockTargets, 2,
		"compileLock's physical table-lock disposition never rewrites the canonical LockTargets")
	require.Same(t, promoted, lockNode.LockTargets[0])
	require.Same(t, rowTarget, lockNode.LockTargets[1])
	require.True(t, lockNode.LockTargets[0].GetLockingRead(),
		"the promoted Shared target keeps its bind-time marking")
	require.True(t, lockNode.LockTargets[1].GetLockingRead())

	// The mark-family presence consumers all read the canonical plan, so
	// they keep working for a promoted statement exactly as for a row
	// statement: marked-table lookup, Shared covering-index admission
	// through the owning base table, and foreign-parent refusal.
	require.True(t, c.lockingCurrentReadStatement(),
		"plan-level presence keeps the compiled promoted statement a marked current-read statement")
	require.True(t, c.lockingTargetMarked(7), "the promoted target stays a marked table")
	require.True(t, c.lockingTargetMarked(8), "the row target keeps its marking")
	idx := func(parent int64) *plan.Node {
		n := scanNode(plan.ScanReadRole_ScanOuterLocking, catalog.SystemIndexRel, nil)
		n.IndexScanInfo = plan.IndexScanInfo{ParentTableId: parent}
		return n
	}
	require.True(t, c.executionReadScanAdmitted(idx(7)),
		"a Shared covering-index locking read admits through its promoted owning base table (R13-F3)")
	require.True(t, c.executionReadScanAdmitted(idx(8)))
	require.False(t, c.executionReadScanAdmitted(idx(99)),
		"a fabricated covering scan still never admits under a foreign parent")

	// The lock operator consumes only the physical row list: the promoted
	// table target must not reach the row-lock operator.
	require.Len(t, ss, 1)
	rootOp, ok := ss[0].RootOp.(*lockop.LockOp)
	require.True(t, ok, "the lock operator is attached for the remaining row target")
	wireTargets := rootOp.CopyToPipelineTarget()
	require.Len(t, wireTargets, 1)
	require.Equal(t, uint64(8), wireTargets[0].TableId,
		"the operator's physical row list carries only the unpromoted target")
}

// The capability-drop half of the same finding: with the canonical presence
// intact, the promoted Shared statement stays visible to the Run-time
// capability validation — a single-binary gate drop below MORPCVersion135
// must reject the compiled, promoted statement instead of silently
// admitting the snapshot-read fallback the strip used to leave behind.
func TestPromotedPresencePlanStaysVisibleToCapabilityDropAfterCompile(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.anal = &AnalyzeModule{}
	c.lockTables = make(map[uint64]*plan.LockTarget)

	promoted := &plan.LockTarget{
		TableId:     7,
		Mode:        lock.LockMode_Shared,
		LockingRead: true,
		LockTable:   true,
	}
	lockNode := &plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{promoted},
	}
	markedScan := &plan.Node{
		NodeId:       0,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r", TblId: 7},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	}
	qry := &plan.Query{Nodes: []*plan.Node{markedScan, lockNode}}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}

	rt := moruntime.ServiceRuntime(c.proc.GetService())
	require.NotNil(t, rt)
	orig, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, orig)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	ss, err := c.compileLock(lockNode, []*Scope{{}})
	require.NoError(t, err)
	require.Len(t, ss, 1, "a fully promoted statement keeps its input scope as-is")
	staged, seen := c.lockTables[7]
	require.True(t, seen)
	require.True(t, staged.GetLockTable())
	require.Len(t, lockNode.LockTargets, 1,
		"even a fully promoted statement keeps its canonical marked target")
	require.True(t, lockNode.LockTargets[0].GetLockingRead())

	// After compilation, the capability drop rejects the plan: presence is
	// still visible at the Run gate, so a mixed-version cluster below the
	// allocated gate cannot serve the promoted shared statement as a silent
	// snapshot read.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(128))
	dropErr := validateLockingCurrentReadCapability(c.proc, c.pn)
	require.Error(t, dropErr, "the compiled promoted presence plan stays visible to the capability gate")
	require.True(t, moerr.IsMoErrCode(dropErr, moerr.ErrNotSupported))
	require.Contains(t, dropErr.Error(), "requires rebinding")

	// At or above the allocated gate the same compiled statement passes.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion135))
	require.NoError(t, validateLockingCurrentReadCapability(c.proc, c.pn))
}
