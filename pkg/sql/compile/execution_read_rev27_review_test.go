// Copyright 2026 Matrix One
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

// PR-review P2 round 2 (XuPeng-SH, generation 2, "historical row targets
// covered by an already-staged table lock still lose their validation
// obligations"): promotion is selected independently for each LOCK_OP, and
// compilePlanScope compiles children before the enclosing LOCK_OP, so a
// nested locking block can stage its promoted current-read table lock while
// an enclosing historical occurrence of the same physical table remains a
// row target (H < SnapshotTS) — which compileLock then OMITS from the
// physical row operator (the pre-existing c.lockTables[TableId] filter).
// The staged-legacy resolver skipped LockTable=false targets, so that
// omitted occurrence lost every validation obligation and the marked
// acquisition suppressed the SI wait-resolved conflict the historical
// occurrence requires today. The resolver now resolves the contract of
// every occurrence the staged acquisition COVERS — promoted targets and ROW
// targets of a staged table, in either tree order (a staged-later
// acquisition still grants the whole table before any row wait can resolve
// a conflict) — while an UNSTAGED row occurrence keeps the lock operator's
// per-occurrence resolution exactly as rev 24 round 11 demands. These
// members pin the resolver universe, drive compileLock itself so the
// physical omission and the declination are both observed (as the reviewer
// requested), and prove the acquisition's terminal outcome on the
// lockservice fixture with an observed mid-wait committed conflict.

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	lockservice "github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// nestedLockOpBlocks builds the canonical nested-locking-block shape: two
// separate LOCK_OPs carry their own target lists (a nested locking block
// compiled inside an enclosing one), plus the scan occurrences whose roles
// the admission and closure walks read.
func nestedLockOpBlocks(
	scans []*plan.Node,
	inner, outer *plan.Node,
) *plan.Plan {
	qry := &plan.Query{
		StmtType: plan.Query_SELECT,
		Nodes:    append(slices.Clone(scans), inner, outer),
	}
	return &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}
}

func lockTargetsNode(id int32, targets ...*plan.LockTarget) *plan.Node {
	return &plan.Node{NodeId: id, NodeType: plan.Node_LOCK_OP, LockTargets: targets}
}

// stagedSharedTarget mirrors the promoted non-INSERT shared table-lock
// disposition compileLock installs into c.lockTables.
func stagedSharedTarget(tableID uint64) *plan.LockTarget {
	return &plan.LockTarget{
		TableId:       tableID,
		Mode:          pblock.LockMode_Shared,
		LockingRead:   true,
		LockTable:     true,
		PrimaryColTyp: plan.Type{Id: int32(types.T_int32)},
	}
}

// engagingRowTarget is the enclosing occurrence's target that stays a ROW
// target (LockTable=false) with a stored explicit snapshot engaging the
// historical cohort for this execution.
func engagingRowTarget(tableID uint64, h timestamp.Timestamp) *plan.LockTarget {
	return &plan.LockTarget{
		TableId:                  tableID,
		Mode:                     pblock.LockMode_Shared,
		LockingRead:              true,
		PrimaryColTyp:            plan.Type{Id: int32(types.T_int32)},
		ExplicitSnapshotPhysical: h.PhysicalTime,
		ExplicitSnapshotLogical:  h.LogicalTime,
	}
}

// currentRowTarget is the all-current covered control: an admitted
// locking-current-read occurrence that stays a row target.
func currentRowTarget(tableID uint64) *plan.LockTarget {
	return &plan.LockTarget{
		TableId:       tableID,
		Mode:          pblock.LockMode_Shared,
		LockingRead:   true,
		PrimaryColTyp: plan.Type{Id: int32(types.T_int32)},
	}
}

// TestCoveredRowOccurrenceResolvesStagedContract pins the resolver universe
// (executionReadStagedLegacyObligations): the covered class is the promoted
// target plus the ROW target of a staged table; an unstaged row occurrence
// keeps its lock-operator resolution and never vetoes.
func TestCoveredRowOccurrenceResolvesStagedContract(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	engagingH := timestamp.Timestamp{PhysicalTime: 50, LogicalTime: 1}
	laterH := timestamp.Timestamp{PhysicalTime: 150, LogicalTime: 1}

	currentScan := occurrenceScan(0, 42, plan.ScanReadRole_ScanOuterLocking, nil)
	engagingScan := occurrenceScan(2, 42, plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, &engagingH)
	laterScan := occurrenceScan(3, 42, plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, &laterH)

	staged := map[uint64]*plan.LockTarget{42: stagedSharedTarget(42)}

	// (1) The reviewer's shape, resolver level: a nested promoted current
	// occurrence stages table 42 while the enclosing historical occurrence
	// stays a row target. The covered row occurrence carries the legacy
	// obligation: one acquisition cannot serve the marked current-read
	// contract and the historical occurrence's terminal SI conflict.
	c.pn = nestedLockOpBlocks(
		[]*plan.Node{currentScan, engagingScan},
		lockTargetsNode(10, stagedSharedTarget(42)),
		lockTargetsNode(11, engagingRowTarget(42, engagingH)))
	c.lockTables = staged
	require.Equal(t, map[uint64]struct{}{42: {}},
		c.executionReadStagedLegacyObligations(c.pn.GetQuery()),
		"the row occurrence covered by the staged table lock carries the legacy obligation")
	require.Empty(t, c.markedPrePipelineLockTables(),
		"a nested current promotion must not turn off the covered historical occurrence's exemption")

	// (2) The UNSTAGED control (rev 26 member 7's universe): with no staged
	// entry the same row occurrence keeps its own per-occurrence resolution
	// in the lock operator, so the promoted occurrence's acquisition keeps
	// the marked options.
	c.pn = nestedLockOpBlocks(
		[]*plan.Node{currentScan, engagingScan},
		lockTargetsNode(10, stagedSharedTarget(42)),
		lockTargetsNode(11, engagingRowTarget(42, engagingH)))
	c.lockTables = map[uint64]*plan.LockTarget{}
	require.Empty(t, c.executionReadStagedLegacyObligations(c.pn.GetQuery()))
	require.Contains(t, c.markedPrePipelineLockTables(), uint64(42),
		"an unstaged row occurrence resolves per occurrence in the lock operator")

	// (3) A covered row occurrence WITHOUT locking-current-read presence (a
	// nested or unmarked occurrence whose table the statement stages)
	// keeps the acquisition legacy the same way its promoted variant does.
	c.pn = nestedLockOpBlocks(
		[]*plan.Node{currentScan},
		lockTargetsNode(10, stagedSharedTarget(42)),
		lockTargetsNode(11, &plan.LockTarget{TableId: 42, Mode: pblock.LockMode_Shared,
			LockingRead: false, PrimaryColTyp: plan.Type{Id: int32(types.T_int32)}}))
	c.lockTables = staged
	require.Equal(t, map[uint64]struct{}{42: {}},
		c.executionReadStagedLegacyObligations(c.pn.GetQuery()),
		"a covered presence-less row occurrence keeps today's obligation")
	require.Empty(t, c.markedPrePipelineLockTables())

	// (4) A covered NON-engaging row occurrence (H >= executing SnapshotTS)
	// never vetoes: the occurrence is an admitted current read.
	c.pn = nestedLockOpBlocks(
		[]*plan.Node{currentScan, laterScan},
		lockTargetsNode(10, stagedSharedTarget(42)),
		lockTargetsNode(11, engagingRowTarget(42, laterH)))
	c.lockTables = staged
	require.Empty(t, c.executionReadStagedLegacyObligations(c.pn.GetQuery()))
	require.Contains(t, c.markedPrePipelineLockTables(), uint64(42),
		"the non-engaging covered row occurrence current-reads in this execution")

	// (5) A covered ordinary current row occurrence never vetoes: the fully
	// admitted nested universe keeps the marked options.
	c.pn = nestedLockOpBlocks(
		[]*plan.Node{currentScan},
		lockTargetsNode(10, stagedSharedTarget(42)),
		lockTargetsNode(11, currentRowTarget(42)))
	c.lockTables = staged
	require.Empty(t, c.executionReadStagedLegacyObligations(c.pn.GetQuery()))
	require.Contains(t, c.markedPrePipelineLockTables(), uint64(42),
		"the all-current covered universe keeps the round-8 marked options")

	// (6) The declination stays PER ACQUISITION: table 42's covered all-
	// current universe keeps its marked options while table 43's covered
	// engaging row occurrence declines table 43 alone.
	c.pn = nestedLockOpBlocks(
		[]*plan.Node{
			occurrenceScan(0, 42, plan.ScanReadRole_ScanOuterLocking, nil),
			occurrenceScan(2, 43, plan.ScanReadRole_ScanOuterLocking, nil),
			occurrenceScan(3, 43, plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, &engagingH),
		},
		lockTargetsNode(10, stagedSharedTarget(42), stagedSharedTarget(43)),
		lockTargetsNode(11, currentRowTarget(42), engagingRowTarget(43, engagingH)))
	c.lockTables = map[uint64]*plan.LockTarget{42: stagedSharedTarget(42), 43: stagedSharedTarget(43)}
	marked := c.markedPrePipelineLockTables()
	require.Contains(t, marked, uint64(42),
		"the fully-admitted acquisition on its own physical table keeps its contract")
	require.NotContains(t, marked, uint64(43),
		"the acquisition whose covered row occurrence is historical keeps the legacy options")
}

// TestCompileLockOmitsCoveredRowTargetAndDeclines drives compileLock itself
// with separate promoted and historical-row LOCK_OPs (the reviewer-requested
// regression): the nested promoted block stages the table first, compileLock
// omits the enclosing row target from the physical row operator, the
// resolver declines the marked options through the staged entry, and BOTH
// tree orders decline — the staged-later order keeps the row target in the
// lock operator while the pre-pipeline acquisition still decides its
// wait-resolved conflict.
func TestCompileLockOmitsCoveredRowTargetAndDeclines(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	engagingH := timestamp.Timestamp{PhysicalTime: 50, LogicalTime: 1}
	c.anal = &AnalyzeModule{isFirst: true}
	c.execType = plan2.ExecTypeTP
	// compileLock writes its staged dispositions into lockTables; the shared
	// admission fixture's bare Compile does not install the map.
	c.lockTables = map[uint64]*plan.LockTarget{}

	currentScan := occurrenceScan(0, 42, plan.ScanReadRole_ScanOuterLocking, nil)
	engagingScan := occurrenceScan(2, 42, plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, &engagingH)
	promoted := lockTargetsNode(10, stagedSharedTarget(42))
	histRow := lockTargetsNode(11, engagingRowTarget(42, engagingH))

	// Children compile first (compilePlanScope's LOCK_OP case), so the
	// nested promoted block compiles before the enclosing row operator.
	c.pn = nestedLockOpBlocks(
		[]*plan.Node{currentScan, engagingScan}, promoted, histRow)

	ss := []*Scope{{}}
	ss, err := c.compileLock(promoted, ss)
	require.NoError(t, err)
	require.Contains(t, c.lockTables, uint64(42),
		"the nested promoted block stages the current-read table lock")
	ss, err = c.compileLock(histRow, ss)
	require.NoError(t, err)
	// The physical omission: no lock operator materialized for the enclosing
	// historical row occurrence.
	require.Nil(t, ss[0].RootOp,
		"compileLock omits the historical row target of the staged table")
	// The omitted occurrence resolves through the staged acquisition.
	require.Equal(t, map[uint64]struct{}{42: {}},
		c.executionReadStagedLegacyObligations(c.pn.GetQuery()))
	require.Empty(t, c.markedPrePipelineLockTables(),
		"the shared acquisition cannot carry the marked contract over the covered historical occurrence")

	// The all-current control: the same tree order with a CURRENT row
	// occurrence omits the row target too, but no covered occurrence keeps a
	// legacy obligation, so the marked options survive.
	c.lockTables = map[uint64]*plan.LockTarget{}
	currentRow := lockTargetsNode(11, currentRowTarget(42))
	c.pn = nestedLockOpBlocks(
		[]*plan.Node{currentScan},
		lockTargetsNode(10, stagedSharedTarget(42)), currentRow)
	ss = []*Scope{{}}
	ss, err = c.compileLock(lockTargetsNode(10, stagedSharedTarget(42)), ss)
	require.NoError(t, err)
	ss, err = c.compileLock(currentRow, ss)
	require.NoError(t, err)
	require.Nil(t, ss[0].RootOp,
		"the covered current row occurrence is omitted the same way")
	require.Empty(t, c.executionReadStagedLegacyObligations(c.pn.GetQuery()))
	require.Contains(t, c.markedPrePipelineLockTables(), uint64(42),
		"the all-current covered universe keeps the marked acquisition")

	// The REVERSE tree order: the enclosing row LOCK_OP compiles first and
	// keeps its row target physically, the nested promoted block then stages
	// the same table — the acquisition still decides the covered
	// occurrence's wait-resolved conflict, so the resolver declines.
	c.lockTables = map[uint64]*plan.LockTarget{}
	c.pn = nestedLockOpBlocks(
		[]*plan.Node{currentScan, engagingScan}, histRow, promoted)
	ss = []*Scope{{}}
	ss, err = c.compileLock(histRow, ss)
	require.NoError(t, err)
	require.NotNil(t, ss[0].RootOp,
		"the row-first order keeps the row target in the lock operator")
	kept := ss[0].RootOp.(*lockop.LockOp).CopyToPipelineTarget()
	require.Len(t, kept, 1)
	require.Equal(t, uint64(42), kept[0].GetTableId())
	require.False(t, kept[0].GetLockTable(),
		"the kept occurrence is a row target with per-occurrence resolution")
	_, err = c.compileLock(promoted, ss)
	require.NoError(t, err)
	require.Contains(t, c.lockTables, uint64(42))
	require.Equal(t, map[uint64]struct{}{42: {}},
		c.executionReadStagedLegacyObligations(c.pn.GetQuery()),
		"a staged-later acquisition covers the kept row occurrence's conflict")
	require.Empty(t, c.markedPrePipelineLockTables(),
		"the staged-later acquisition cannot carry the marked contract over the covered historical occurrence")
}

// TestNestedStagedLockKeepsLegacyAcquisitionForCoveredRowOccurrence proves
// the reviewer's shape end to end through the staged acquisition: the
// mid-wait committed writer observed on the shared staged lock must surface
// TODAY'S terminal legacy conflict, and the fully-admitted nested control
// keeps the marked suppression on its own universe.
func TestNestedStagedLockKeepsLegacyAcquisitionForCoveredRowOccurrence(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime("rev27", rt)
			lockservice.RunLockServicesForTest(
				zap.DebugLevel,
				[]string{"rev27"},
				time.Second,
				func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
					rt.SetGlobalVariables(runtime.LockService, services[0])

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					sender, err := rpc.NewSender(rpc.Config{}, rt)
					require.NoError(t, err)
					txnClient := client.NewTxnClient("", sender, client.WithLockService(services[0]))
					txnClient.Resume()
					defer func() { require.NoError(t, txnClient.Close()) }()

					txnOp, err := txnClient.New(ctx, timestamp.Timestamp{},
						client.WithTxnIsolation(pbtxn.TxnIsolation_SI))
					require.NoError(t, err)
					defer func() { require.NoError(t, txnOp.Rollback(ctx)) }()
					sessionSnapshot := txnOp.Txn().SnapshotTS

					proc := process.NewTopProcess(
						ctx,
						mpool.MustNewZero(),
						txnClient,
						txnOp,
						nil,
						services[0],
						nil,
						nil,
						nil,
						nil,
						nil)
					proc.SetPlanSnapshotTS(sessionSnapshot)

					runtime.SetupServiceRuntimeTestingContext(proc.GetService())
					testingContext := runtime.MustGetTestingContext(proc.GetService())
					testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
					higherCommit := sessionSnapshot.Next()
					testingContext.SetAdjustLockResultFunc(
						func(_ []byte, table uint64, result *pblock.Result) {
							if table != 42 {
								return
							}
							// The mid-wait committed writer observed on the
							// shared staged acquisition's waiter wake.
							result.NewLockAdd = true
							result.HasConflict = true
							result.HasPrevCommit = true
							result.Timestamp = higherCommit
						})
					defer testingContext.SetAdjustLockResultFunc(nil)
					defer testingContext.SetBeforeLockFunc(nil)

					engagingH := timestamp.Timestamp{
						PhysicalTime: sessionSnapshot.PhysicalTime - 1,
					}
					require.True(t, !engagingH.IsEmpty() && engagingH.Less(sessionSnapshot),
						"fixture validity: the stored H engages the historical cohort")

					currentScan := occurrenceScan(0, 42, plan.ScanReadRole_ScanOuterLocking, nil)
					engagingScan := occurrenceScan(2, 42,
						plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, &engagingH)

					newCompile := func(pn *plan.Plan) *Compile {
						c := &Compile{proc: proc}
						c.addr = "cn-local"
						c.e = &reviewBarrierEngine{
							database: &reviewFakeDatabase{
								byName: map[string]engine.Relation{"t": &reviewExclusiveRelation{}},
							},
						}
						c.anal = &AnalyzeModule{isFirst: true}
						c.execType = plan2.ExecTypeTP
						// compileLock stages the promoted table lock through
						// this map before lockTable acquires it.
						c.lockTables = map[uint64]*plan.LockTarget{}
						c.pn = pn
						return c
					}

					// (1) The MIXED nested shape: the nested promoted
					// current block stages the table while the enclosing
					// occurrence stays an engaging-historical ROW target the
					// staged acquisition omits. The resolver declines, and
					// the observed mid-wait committed writer surfaces
					// today's terminal conflict instead of the marked
					// suppression riding the shared acquisition.
					mixedInner := lockTargetsNode(10, stagedSharedTarget(42))
					mixedOuter := lockTargetsNode(11, engagingRowTarget(42, engagingH))
					mixed := newCompile(nestedLockOpBlocks(
						[]*plan.Node{currentScan, engagingScan}, mixedInner, mixedOuter))
					ss := []*Scope{{}}
					ss, err = mixed.compileLock(mixedInner, ss)
					require.NoError(t, err)
					ss, err = mixed.compileLock(mixedOuter, ss)
					require.NoError(t, err)
					require.Nil(t, ss[0].RootOp,
						"the covered historical row occurrence never reaches a physical row operator")
					require.Contains(t, mixed.lockTables, uint64(42))
					require.NoError(t, mixed.executionReadResolveRelationSupport(),
						"the declined acquisition skips the marked-route relation probe")
					require.Empty(t, mixed.markedPrePipelineLockTables(),
						"the shared acquisition cannot carry both contracts")
					err = mixed.lockTable()
					require.Error(t, err,
						"the mixed nested shape keeps today's terminal conflict on the mid-wait committed writer")
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)
					require.Equal(t, sessionSnapshot, txnOp.Txn().SnapshotTS,
						"the legacy acquisition never moves the fixed SI snapshot")

					// (2) The CONTROL: the nested promoted block plus a
					// CURRENT row occurrence of the same table — no covered
					// occurrence keeps a legacy obligation — keeps the
					// marked options, and the identical mid-wait committed
					// writer is suppressed exactly as the round-8 contract
					// promises.
					controlInner := lockTargetsNode(10, stagedSharedTarget(42))
					controlOuter := lockTargetsNode(11, currentRowTarget(42))
					currentOnly := newCompile(nestedLockOpBlocks(
						[]*plan.Node{currentScan}, controlInner, controlOuter))
					ss = []*Scope{{}}
					ss, err = currentOnly.compileLock(controlInner, ss)
					require.NoError(t, err)
					ss, err = currentOnly.compileLock(controlOuter, ss)
					require.NoError(t, err)
					require.Nil(t, ss[0].RootOp)
					require.NoError(t, currentOnly.executionReadResolveRelationSupport())
					require.Contains(t, currentOnly.markedPrePipelineLockTables(), uint64(42),
						"the all-current nested universe keeps its marked acquisition")
					require.NoError(t, currentOnly.lockTable(),
						"the admitted marked promoted statement survives the mid-wait committed writer")
					require.True(t, txnOp.HasLockTable(42),
						"the marked staged table lock is really held by the reader transaction")
					require.Equal(t, sessionSnapshot, txnOp.Txn().SnapshotTS,
						"the marked pre-pipeline acquisition never moves the fixed SI snapshot")

					// (3) The REVERSE order on the same fixture: the
					// historical row occurrence compiles first and keeps its
					// row target physically, the nested promoted block then
					// stages the table — the pre-pipeline acquisition still
					// decides the covered occurrence's wait-resolved
					// conflict, so the resolver declines again.
					reversedOuter := lockTargetsNode(11, engagingRowTarget(42, engagingH))
					reversedInner := lockTargetsNode(10, stagedSharedTarget(42))
					reversed := newCompile(nestedLockOpBlocks(
						[]*plan.Node{currentScan, engagingScan}, reversedOuter, reversedInner))
					ss = []*Scope{{}}
					ss, err = reversed.compileLock(reversedOuter, ss)
					require.NoError(t, err)
					kept := ss[0].RootOp.(*lockop.LockOp).CopyToPipelineTarget()
					require.Len(t, kept, 1,
						"the row-first order keeps the historical row occurrence physically")
					_, err = reversed.compileLock(reversedInner, ss)
					require.NoError(t, err)
					require.Contains(t, reversed.lockTables, uint64(42))
					require.Empty(t, reversed.markedPrePipelineLockTables(),
						"the staged-later acquisition cannot carry the marked contract over the covered historical occurrence")
					err = reversed.lockTable()
					require.Error(t, err,
						"the reverse-order nested shape keeps today's terminal conflict too")
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)
				},
				nil,
			)
		},
	)
}
