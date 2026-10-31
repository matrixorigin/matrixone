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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// rev-14 explicit representation: only an outer locking occurrence
// participates in the stage-1 admission set; the explicit-nested and
// covered-strip roles are exempt BY REPRESENTATION and stay snapshot reads,
// and a presence plan's unset role never reaches this predicate (the
// closure walk declines the whole statement first).
func TestExecutionReadScanAdmittedRev14Roles(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	require.False(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanNestedBlockExplicit, "r", nil)),
		"explicit nested provenance stays on snapshot semantics")
	require.False(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanCoveredPlanStrip, "r", nil)),
		"the covered-plan strip is outside the stage-1 admission set")
	require.True(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanOuterLocking, "r", nil)),
		"an outer locking occurrence over an ordinary table is admitted")
}

// Covered/forced secondary-index surfaces (design D-10.2, R13-F3): a
// fabricated index-table scan admits THROUGH its owning locked base table —
// the additive IndexScanInfo.ParentTableId must name a bind-time MARKED lock
// target of this statement. An unset parent, a foreign parent, a nested role
// and the absence of presence all decline so the statement conserves
// today's whole-statement semantics instead of mixing reads.
func TestExecutionReadScanAdmittedThroughIndexParent(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	idxScan := func(role plan.ScanReadRole, parent int64) *plan.Node {
		node := scanNode(role, catalog.SystemIndexRel, nil)
		node.IndexScanInfo = plan.IndexScanInfo{ParentTableId: parent}
		return node
	}

	// Without a bound presence plan no parent link admits anything: the
	// fabricated surface stays on snapshot semantics.
	require.False(t, c.executionReadScanAdmitted(idxScan(plan.ScanReadRole_ScanOuterLocking, 42)),
		"without a presence plan the parent link admits nothing")

	c.pn = presencePlan(&plan.Node{
		NodeId:        0,
		NodeType:      plan.Node_TABLE_SCAN,
		TableDef:      &plan.TableDef{TableType: catalog.SystemIndexRel},
		IndexScanInfo: plan.IndexScanInfo{ParentTableId: 42},
		ScanReadRole:  plan.ScanReadRole_ScanOuterLocking,
	})

	require.True(t, c.executionReadScanAdmitted(idxScan(plan.ScanReadRole_ScanOuterLocking, 42)),
		"the covering index scan admits through the marked owning base table")
	require.False(t, c.executionReadScanAdmitted(idxScan(plan.ScanReadRole_ScanOuterLocking, 77)),
		"a parent link to a table outside the marked targets never admits")
	require.False(t, c.executionReadScanAdmitted(idxScan(plan.ScanReadRole_ScanOuterLocking, 0)),
		"an unset ParentTableId never admits")
	require.False(t, c.executionReadScanAdmitted(idxScan(plan.ScanReadRole_ScanNestedBlock, 42)),
		"a nested-role index surface never admits")

	// An UNMARKED lock target is not an admission link: only bind-time
	// marked occurrences of the owning base table admit the covered surface.
	unmarked := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{
			NodeType:    plan.Node_LOCK_OP,
			LockTargets: []*plan.LockTarget{{TableId: 42}},
		}},
	}}}
	c.pn = unmarked
	require.False(t, c.executionReadScanAdmitted(idxScan(plan.ScanReadRole_ScanOuterLocking, 42)),
		"an unmarked lock target provides no admission link")
}

func presencePlan(nodes ...*plan.Node) *plan.Plan {
	target := &plan.LockTarget{TableId: 42, LockingRead: true}
	lockNode := &plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{target},
	}
	qry := &plan.Query{Nodes: append(nodes, lockNode)}
	return &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}
}

// The R14-F2 closure walk (D-10.3, G4): an unset scan role inside a
// locking-presence plan is a vanished mark — it declines the whole statement
// before source publication; search-served surfaces decline the same way;
// explicit-nested and covered-below-gate clean plans do not.
func TestExecutionReadClosureDeclinesWholeStatement(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	tableType := "r"
	newCompile := func() *Compile {
		c, _, teardown := admissionFixture(t, sessionTS)
		t.Cleanup(teardown)
		return c
	}

	// (a) Lost mark: one unset-role scan beside a valid marked scan.
	marked := &plan.Node{
		NodeId:       0,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: tableType},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	}
	lost := &plan.Node{
		NodeId:       1,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: tableType},
		ScanReadRole: plan.ScanReadRole_ScanNestedBlock,
	}
	c := newCompile()
	c.pn = presencePlan(marked, lost)
	c.execReadEpoch = 1
	declined, op, _, err := c.executionReadAdmitScan(&Scope{})
	require.NoError(t, err)
	require.True(t, declined, "a vanished mark declines the whole statement")
	require.Nil(t, op)
	require.True(t, c.execReadDisabled, "declinature disables the marking execution-wide")
	require.Equal(t, "locking-read provenance lost in a rewritten scan", c.execReadDisableReason)

	// (b) Search-served surface: an INDEX_SEARCH_SCAN in a presence plan.
	c = newCompile()
	c.pn = presencePlan(marked, &plan.Node{
		NodeId:          1,
		NodeType:        plan.Node_INDEX_SEARCH_SCAN,
		IndexSearchScan: &plan.IndexSearchScan{},
	})
	c.execReadEpoch = 1
	declined, _, _, err = c.executionReadAdmitScan(&Scope{})
	require.NoError(t, err)
	require.True(t, declined, "search-served surfaces sit outside the stage-1 admission set")
	require.Equal(t,
		"locking statement mixes a search-served scan outside the stage-1 admission set",
		c.execReadDisableReason)

	// (c) Covered-plan strip value in a presence plan declines too.
	c = newCompile()
	c.pn = presencePlan(marked, &plan.Node{
		NodeId:       1,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: tableType},
		ScanReadRole: plan.ScanReadRole_ScanCoveredPlanStrip,
	})
	c.execReadEpoch = 1
	declined, _, _, err = c.executionReadAdmitScan(&Scope{})
	require.NoError(t, err)
	require.True(t, declined)

	// (d) A clean presence plan passes the walk (the same-table
	// scalar-subquery shape with explicit-nested scans stays decidable).
	c = newCompile()
	c.pn = presencePlan(marked, &plan.Node{
		NodeId:       1,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: tableType},
		ScanReadRole: plan.ScanReadRole_ScanNestedBlockExplicit,
	})
	c.execReadEpoch = 1
	declined, _, _, err = c.executionReadAdmitScan(&Scope{})
	require.False(t, declined, "an explicit-nested occurrence is exempt by representation")
	require.Error(t, err, "the fixture has no logtail barrier: loud rebuild failure, never mixing")

	// (e) The walk runs once per attempt token.
	c = newCompile()
	c.pn = presencePlan(marked, lost)
	c.execReadEpoch = 1
	_, _, _, _ = c.executionReadAdmitScan(&Scope{})
	require.Equal(t, uint64(1), c.execReadClosureToken)
}

// Stage-1 keeps only the node-local surface: a marked scan placed on
// another CN declines (D-9 transport is stage-2), and the decline rolls
// back through the same G4 path.
func TestExecutionReadAdmitScanRemoteDecline(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	c.pn = presencePlan(&plan.Node{
		NodeId:       0,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r"},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	})
	c.addr = "127.0.0.1:6001"
	s := &Scope{}
	s.NodeInfo.Addr = "127.0.0.1:6002"
	declined, op, _, err := c.executionReadAdmitScan(s)
	require.NoError(t, err)
	require.True(t, declined, "remote marked execution declines")
	require.Nil(t, op)
	require.Equal(t, "marked scan would execute remote", c.execReadDisableReason)
	require.True(t, c.execReadDisabled)
}

// The design-review P3-1 invariant: the attempt token is advanced at
// ADMISSION (runOnce entry), independent of any retirement path. A recycled
// Compile whose pool retirement (clear/Reset) was skipped must never
// re-serve the cached carrier, because the attempt token captured at the
// carrier's acquisition can no longer equal the current epoch. The fault
// injection is the skipped retirement: state from statement k survives into
// statement k+1 of the SAME still-open transaction (same operator), and
// admission resets only what runOnce resets.
func TestExecutionReadEpochFaultInjectionSkippedRetirement(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	ownerOp, teardown := admissionFixtureTxn(t, sessionTS)
	defer teardown()

	// Statement k's execution-read state: identical operator, stale carrier.
	frontier := timestamp.Timestamp{PhysicalTime: 300, LogicalTime: 1}
	c, _, teardown2 := admissionFixture(t, sessionTS)
	defer teardown2()
	c.proc.Base.TxnOperator = ownerOp
	c.execReadFrontier = frontier
	c.execReadFrontierSet = true
	c.execReadOp = client.ApplyExecutionReadTS(ownerOp, frontier)
	c.execReadOpOwner = ownerOp
	stale := c.execReadOp
	c.execReadEpoch = 7
	c.execReadCarrierToken = 7

	// Statement k+1's runOnce entry: epoch advances, attempt state resets —
	// runOnce performs exactly these two steps and does NOT retire the
	// carrier cache outside them.
	c.execReadEpoch = 8
	c.resetExecutionReadAttemptState()
	require.False(t, c.execReadFrontierSet)
	require.Nil(t, c.execReadOp,
		"runOnce's attempt reset clears the cached carrier fields")

	// With cleared fields the carrier can never be re-served at all. The
	// stronger defense is the token equality: simulate the most adversarial
	// skipped RESET variant, where only runOnce's epoch step ran and the
	// field clearing was lost — a stale carrier with an old token still
	// cannot match the new epoch.
	c.execReadFrontier = frontier
	c.execReadFrontierSet = true
	c.execReadOp = stale
	c.execReadOpOwner = ownerOp
	c.execReadCarrierToken = 7
	require.NotEqual(t, c.execReadCarrierToken, c.execReadEpoch,
		"a previous attempt's token cannot equal the current epoch")
	op, gotFrontier, err := c.executionReadOperator()
	require.Error(t, err, "without a barrier the rebuild fails loudly; the stale carrier is not served")
	require.Nil(t, op)
	require.True(t, gotFrontier.IsEmpty())

	// The retry floor is execution-chain state: it survives the attempt
	// reset and dies at the execution boundary.
	c.execReadRetryFloor = timestamp.Timestamp{PhysicalTime: 800, LogicalTime: 2}
	c.resetExecutionReadAttemptState()
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 800, LogicalTime: 2}, c.execReadRetryFloor,
		"the floor survives attempt resets (D-5 retry chain)")
	c.resetExecutionReadState()
	require.True(t, c.execReadRetryFloor.IsEmpty(),
		"a fresh execution (Reset/clear boundary) is floorless")
}

// admissionFixtureTxn hands out a bare SI test operator whose transaction
// stays open, so a later "recycled statement" can legitimately point at the
// same still-active operator.
func admissionFixtureTxn(t *testing.T, ts timestamp.Timestamp) (client.TxnOperator, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	op, closeFn := client.NewTestTxnOperator(ctx)
	op.TxnRef().Isolation = pbtxn.TxnIsolation_SI
	op.TxnRef().SnapshotTS = ts
	op.AddWorkspace(&disttae.Transaction{})
	return op, func() {
		cancel()
		closeFn()
	}
}

// The SI retry gating (D-5/D-8, round 7 finding 2): a marked locking
// statement under SI accepts the tokenized locking-read driver and the
// plain shared retry code of the refresh family, but NEVER the
// definition-rebuild code — a rebuilt plan recaptures the SAME fixed SI
// snapshot, so a retained definition fence cannot promise progress and
// keeps today's terminal conflict (non-goal N3/G3, R13-F4's single
// marked-SI retry class). Unmarked statements keep today's terminal
// behavior exactly, and RC keeps the definition-retry rebuild because its
// snapshot advances in place.
func TestIsRetryErrLockingCurrentRead(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.pn = presencePlan(&plan.Node{
		NodeId:       0,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r"},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	})
	lockErr := moerr.NewTxnNeedRetryLockingRead(context.Background(), 900, 3)
	require.True(t, c.lockingCurrentReadStatement(), "presence + SI transaction")
	require.True(t, c.isRetryErr(lockErr), "the marked statement accepts its tokenized driver")
	require.True(t, c.isRetryErr(moerr.NewTxnNeedRetryNoCtx()),
		"marked SI accepts the shared retry code through the refresh family")
	require.False(t, c.isRetryErr(moerr.NewTxnNeedRetryWithDefChangedNoCtx()),
		"a retained definition fence cannot promise progress under the fixed SI snapshot (N3/G3)")

	c2, _, teardown2 := admissionFixture(t, sessionTS)
	defer teardown2()
	c2.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{}}}
	require.False(t, c2.lockingCurrentReadStatement(), "no presence means no marked statement")
	require.False(t, c2.isRetryErr(lockErr), "unmarked SI never accepts the locking driver")
	require.False(t, c2.isRetryErr(moerr.NewTxnNeedRetryNoCtx()),
		"unmarked SI keeps today's terminal retry behavior")
	require.False(t, c2.isRetryErr(moerr.NewTxnNeedRetryWithDefChangedNoCtx()),
		"unmarked SI keeps the terminal DDL-fence conflict")

	// RC keeps every retry class at base semantics: the mutable snapshot
	// advances in place, so the definition rebuild can observe progress.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rcOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	rcOp.TxnRef().Isolation = pbtxn.TxnIsolation_RC
	rcOp.TxnRef().SnapshotTS = sessionTS
	rcProc := testutil.NewProcess(t)
	rcProc.Base.TxnOperator = rcOp
	c3 := &Compile{proc: rcProc}
	c3.pn = presencePlan(&plan.Node{
		NodeId:       0,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r"},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	})
	require.False(t, c3.lockingCurrentReadStatement(), "RC is never a marked SI statement")
	require.True(t, c3.isRetryErr(moerr.NewTxnNeedRetryNoCtx()),
		"RC keeps the shared retry code")
	require.True(t, c3.isRetryErr(moerr.NewTxnNeedRetryWithDefChangedNoCtx()),
		"RC keeps the definition-retry code: its snapshot advances in place")
}

// The wire round-trip of marks + frontier (D-8 layer 1 / D-5): constructLockOp
// derives the marked-table set from the plan's presence, the frontier stamp
// publishes to every marked LockOp, and CopyToPipelineTarget carries presence
// and frontier on the additive fields only for marked, stamped targets.
func TestLockOpMarksAndWireRoundTrip(t *testing.T) {
	locked := &plan.LockTarget{TableId: 42, LockingRead: true}
	other := &plan.LockTarget{TableId: 43}
	arg, err := constructLockOp(&plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{locked, other},
	}, nil)
	require.NoError(t, err)
	defer arg.Release()

	// Unstamped: marks stay inert until the attempt's frontier publishes.
	targets := arg.CopyToPipelineTarget()
	require.False(t, targets[0].GetLockingRead())

	frontier := timestamp.Timestamp{PhysicalTime: 777, LogicalTime: 5}
	arg.SetLockingReadCurrentFrontier(frontier, timestamp.Timestamp{PhysicalTime: 900, LogicalTime: 9})
	targets = arg.CopyToPipelineTarget()
	require.True(t, targets[0].GetLockingRead(), "the marked target carries presence on the wire")
	require.False(t, targets[1].GetLockingRead(), "an unmarked target carries nothing new")
	require.Equal(t, int64(777), targets[0].GetCurrentReadFrontierPhysical())
	require.Equal(t, uint32(5), targets[0].GetCurrentReadFrontierLogical())
}

// The moerr token round-trip (D-5, rev-7 token validity): parsing succeeds
// only for the driver's own messages and tolerates appended diagnostics;
// every other error resolves not-ok and never re-drives a retry.
func TestTxnNeedRetryLockingReadFrontierRoundTrip(t *testing.T) {
	err := moerr.NewTxnNeedRetryLockingRead(context.Background(), 1234567890, 7)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryLockingRead))
	phys, logical, ok := moerr.GetTxnNeedRetryLockingReadFrontier(err)
	require.True(t, ok)
	require.Equal(t, int64(1234567890), phys)
	require.Equal(t, uint32(7), logical)

	_, _, ok = moerr.GetTxnNeedRetryLockingReadFrontier(moerr.NewTxnNeedRetryNoCtx())
	require.False(t, ok, "a different retry code carries no locking frontier")
	_, _, ok = moerr.GetTxnNeedRetryLockingReadFrontier(nil)
	require.False(t, ok)
	_, _, ok = moerr.GetTxnNeedRetryLockingReadFrontier(
		context.Canceled)
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// pre-PR review P1 members: the attempt's admission lifecycle and the
// prepared-pipeline mark retention.

// The P1 ordering invariant through the ACTUAL runPipelineAttempt sequence:
// beginExecutionReadAttempt (epoch advance + attempt-state reset) runs
// inside runPipelineAttempt, BEFORE prePipelineInitializer assembles any
// data source — so everything source assembly installs for the attempt
// (admission, carrier, binding, decline) is owned by the attempt token and
// survives the run body, where the old ordering's reset used to wipe it and
// let mid-execution discovery acquire a second frontier. The assertions
// below observe the attempt state from the run-body position (where the
// old reset ran), across multiple attempts and decline shapes.
func TestExecutionReadAttemptOpensBeforeSourceAssembly(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.disableLock = true

	locked := &plan.LockTarget{TableId: 42, LockingRead: true}
	arg, err := constructLockOp(&plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{locked},
	}, nil)
	require.NoError(t, err)
	defer arg.Release()
	c.scopes = []*Scope{{RootOp: arg}}

	cleanScan := func() *plan.Node {
		return &plan.Node{
			NodeId:       0,
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     &plan.TableDef{TableType: "r"},
			ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
		}
	}
	lostMark := func() *plan.Node {
		return &plan.Node{
			NodeId:       1,
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     &plan.TableDef{TableType: "r"},
			ScanReadRole: plan.ScanReadRole_ScanNestedBlock,
		}
	}

	// A recycled Compile's PREVIOUS statement left its frontier state
	// behind (retirement skipped — the pool reality). A cold Compile in
	// this state used to skip the closure check entirely (epoch ==
	// closureToken == 0) and let init's stamps and discoveries ride
	// whatever was lying around.
	staleFrontier := timestamp.Timestamp{PhysicalTime: 777, LogicalTime: 5}
	c.execReadFrontierSet = true
	c.execReadFrontier = staleFrontier
	c.pn = presencePlan(cleanScan())
	epochBefore := c.execReadEpoch

	var epochAttempt1 uint64
	err = c.runPipelineAttempt(func() error {
		// The run body: the old runOnce head reset executed exactly here.
		require.Greater(t, c.execReadEpoch, epochBefore,
			"the attempt opened before the run body ran")
		epochAttempt1 = c.execReadEpoch
		require.False(t, c.execReadFrontierSet,
			"the previous statement's frontier is retired before this attempt's source assembly; it can neither be served nor stamped")
		targets := arg.CopyToPipelineTarget()
		require.False(t, targets[0].GetLockingRead(),
			"no carrier was acquired for this attempt in the fixture (no barrier): the stale frontier must not stamp anything")

		// Admission inside the attempt body walks the closure once for the
		// attempt token; a second admission (a parallel scope of the same
		// attempt) reuses the same outcome.
		declined, op, _, admitErr := c.executionReadAdmitScan(&Scope{})
		require.False(t, declined)
		require.Nil(t, op)
		require.Error(t, admitErr,
			"the fixture has no logtail barrier: loud rebuild failure, never mixing")
		require.Equal(t, epochAttempt1, c.execReadClosureToken,
			"the closure check is owned by this attempt")
		_, _, _, admitErr = c.executionReadAdmitScan(&Scope{})
		require.Error(t, admitErr)
		require.Equal(t, epochAttempt1, c.execReadClosureToken,
			"a second marked scope reuses the attempt's one closure outcome")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, epochAttempt1, c.execReadEpoch,
		"exactly one attempt opened for the one runPipelineAttempt call")

	// Attempt 2 — a lost-mark statement: the walk outcome is derived DURING
	// THIS ATTEMPT and retained through the run body to the attempt end.
	c.pn = presencePlan(cleanScan(), lostMark())
	epochBeforeAttempt2 := c.execReadEpoch
	sawDecline := false
	err = c.runPipelineAttempt(func() error {
		require.Equal(t, epochBeforeAttempt2+1, c.execReadEpoch,
			"a new pipeline attempt opens a new attempt")
		declined, op2, _, admitErr := c.executionReadAdmitScan(&Scope{})
		require.True(t, declined, "the lost-mark decline is derived in this attempt")
		require.Nil(t, op2)
		require.NoError(t, admitErr, "the G4 decline is a soft rollback, not an error")
		require.True(t, c.execReadDisabled,
			"the decline decision is already retained and visible inside the run body")
		sawDecline = true
		// A parallel scope of the same attempt answers the SAME decline
		// without re-walking and without a second frontier.
		declined, op2, _, admitErr = c.executionReadAdmitScan(&Scope{})
		require.True(t, declined)
		require.Nil(t, op2)
		require.NoError(t, admitErr)
		return nil
	})
	require.NoError(t, err)
	require.True(t, sawDecline)
	require.True(t, c.execReadDisabled,
		"the decline survives beyond the run body")
	require.Equal(t, epochBeforeAttempt2+1, c.execReadClosureToken)

	// Attempt 3 (the retry ladder): the attempt open resets the PREVIOUS
	// attempt's decline; the same lost-mark statement re-derives it inside
	// the new attempt — declines never leak across attempts, and an
	// attempt is never served a stale decline.
	epochBeforeAttempt3 := c.execReadEpoch
	err = c.runPipelineAttempt(func() error {
		require.False(t, c.execReadDisabled,
			"the attempt open resets the previous attempt's decline before source assembly")
		declined, _, _, admitErr := c.executionReadAdmitScan(&Scope{})
		require.True(t, declined)
		require.NoError(t, admitErr)
		require.True(t, c.execReadDisabled)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, epochBeforeAttempt3+1, c.execReadEpoch)
}

// The P1 prepared-pipeline member: Pipeline.Cleanup resets even retained
// physical pipelines, so Reset must keep the bind-time marked-TABLE set —
// a later EXECUTE re-stamps the frontier each attempt and goes back to
// live marked-SI revalidation instead of silently falling to the legacy
// SI conflict path.
func TestLockOpPreparedResetRetainsMarkedProvenance(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	locked := &plan.LockTarget{TableId: 42, LockingRead: true}
	plain := &plan.LockTarget{TableId: 43}
	arg, err := constructLockOp(&plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{locked, plain},
	}, nil)
	require.NoError(t, err)
	defer arg.Release()
	proc := testutil.NewProcess(t)

	// Attempt 1: bind-time marks + this attempt's stamp publish presence.
	arg.SetLockingReadCurrentFrontier(timestamp.Timestamp{PhysicalTime: 111, LogicalTime: 1}, sessionTS)
	targets := arg.CopyToPipelineTarget()
	require.True(t, targets[0].GetLockingRead())

	// The retained pipeline resets between EXECUTEs: the per-attempt stamp
	// dies with the attempt, but the marked-table provenance must survive.
	arg.Reset(proc, false, nil)
	targets = arg.CopyToPipelineTarget()
	require.False(t, targets[0].GetLockingRead(),
		"unstamped-after-reset: presence is inert without this attempt's frontier")

	// Next EXECUTE (a fresh attempt's stamp): the marks re-activate from
	// the retained provenance — this is what markingTarget consumes.
	arg.SetLockingReadCurrentFrontier(timestamp.Timestamp{PhysicalTime: 222, LogicalTime: 2}, sessionTS)
	targets = arg.CopyToPipelineTarget()
	require.True(t, targets[0].GetLockingRead(),
		"the marked-table set survived Reset and re-activates on the next attempt's stamp")
	require.Equal(t, int64(222), targets[0].GetCurrentReadFrontierPhysical())
	require.Equal(t, uint32(2), targets[0].GetCurrentReadFrontierLogical())
	require.False(t, targets[1].GetLockingRead(),
		"an unmarked target stays unmarked across resets")

	// Round 10 (finding 2): presence now travels per occurrence with the
	// targets constructLockOp adds — bind-time provenance Reset cannot
	// lose — so the sweep publishes the frontier only and the per-occurrence
	// marks re-activate from that retained provenance.
	c.pn = presencePlan(&plan.Node{
		NodeId:       0,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r"},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	})
	c.scopes = []*Scope{{RootOp: arg}}
	c.execReadDisabled = false
	c.execReadFrontierSet = true
	c.execReadFrontier = timestamp.Timestamp{PhysicalTime: 333, LogicalTime: 3}
	c.executionReadStampMarkingLockScopes()
	targets = arg.CopyToPipelineTarget()
	require.True(t, targets[0].GetLockingRead(),
		"the swept attempt re-activates the retained per-occurrence marks")
	require.Equal(t, int64(333), targets[0].GetCurrentReadFrontierPhysical(),
		"the sweep stamps this attempt's frontier")
}

// The P2 outer-kind member reaches compile's whole-statement decline: a
// locked occurrence on a relation kind outside the stage-1 admission set
// (the mixed ordinary/cluster shape) declines the ENTIRE statement before
// publication, so no locked peer current-reads while the unsupported
// occurrence keeps the snapshot.
func TestExecutionReadClosureDeclinesUnsupportedOuterKind(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	c.pn = presencePlan(
		&plan.Node{
			NodeId:       0,
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     &plan.TableDef{TableType: "r"},
			ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
		},
		&plan.Node{
			NodeId:       1,
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     &plan.TableDef{TableType: catalog.SystemClusterRel},
			ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
		},
	)
	c.execReadEpoch = 1
	declined, op, _, err := c.executionReadAdmitScan(&Scope{})
	require.NoError(t, err)
	require.True(t, declined,
		"a locked cluster occurrence declines the whole statement")
	require.Nil(t, op)
	require.True(t, c.execReadDisabled)
	require.Equal(t,
		"locking statement locks an outer occurrence on a relation kind outside the stage-1 admission set",
		c.execReadDisableReason)

	// The admissible covered-index member stays green behind the same gate:
	// an outer-locked index surface through its marked owning parent is an
	// admitted stage-1 member, never a decline.
	c2, _, teardown2 := admissionFixture(t, sessionTS)
	defer teardown2()
	idxScan := &plan.Node{
		NodeId:        0,
		NodeType:      plan.Node_TABLE_SCAN,
		TableDef:      &plan.TableDef{TableType: catalog.SystemIndexRel},
		IndexScanInfo: plan.IndexScanInfo{ParentTableId: 42},
		ScanReadRole:  plan.ScanReadRole_ScanOuterLocking,
	}
	c2.pn = presencePlan(idxScan)
	c2.execReadEpoch = 1
	declined, _, _, err = c2.executionReadAdmitScan(&Scope{})
	require.False(t, declined)
	require.Error(t, err, "no barrier in the fixture: loud failure, never mixing")
}
