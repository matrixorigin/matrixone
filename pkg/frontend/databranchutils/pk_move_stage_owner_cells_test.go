// Copyright 2026 Matrix Origin
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

package databranchutils

// D190/D191 cells (H.10)(v), (H.10)(u) with its RED twins, and the
// D191(H)-re-seated (H.10)(t) with its RED twin. Every schedule mirrors
// the production loci the stub harness documents; the round adapters
// below mirror pkg/sql/compile/pk_move_fencing_cs.go's owned geometry
// (bind at grant/F2, BoundOperator at F1, certificate -> terminal entry
// -> decision gate -> commit, fatal-SQL executor arm, fail()'s public
// gate-rejected Rollback).

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Stub-backed round adapters (cs.go owned-geometry mirror).
// ---------------------------------------------------------------------------

type stubRoundOps struct {
	mu      sync.Mutex
	grant   *stageOperatorStub
	f2      *stageOperatorStub
	unowned []*stageOperatorStub
}

type stubScratchLocks struct {
	cli     *stubTxnClient
	journal *stageJournal
	ops     *stubRoundOps
	binding DataBranchFencingBinding
	// blockGrant, when non-nil, parks the grant's body past the bind
	// with a cancel-aware wake (the (o)(ii) grant-in-flight geometry).
	blockGrant chan struct{}
	// onGrantSuccess fires at the end of a successful grant (the (o)(i)
	// seal-mid-round injection point).
	onGrantSuccess func()
}

func (l *stubScratchLocks) hookOp(op *stageOperatorStub) {
	op.AppendEventCallback(client.RollbackEvent, client.TxnEventCallback{
		Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error {
			l.journal.add("rollback-event")
			return nil
		}})
	op.AppendEventCallback(client.ClosedEvent, client.TxnEventCallback{
		Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error {
			l.journal.add("closed-event")
			return nil
		}})
}

func (l *stubScratchLocks) AcquireScratchGrant(
	ctx context.Context, in DataBranchFencingRoundInput,
) (DataBranchScratchGrant, error) {
	owner := DataBranchRoundOwnerFromCtx(ctx)
	op := l.cli.New(1)
	l.hookOp(op)
	l.ops.mu.Lock()
	l.ops.grant = op
	l.ops.mu.Unlock()
	if owner == nil {
		// Legacy (amalgamation-disabled) geometry: the lane retains the
		// operator with no owner — the r75 slot whose retention was
		// unaccounted (D186(A.2)).
		l.ops.mu.Lock()
		l.ops.unowned = append(l.ops.unowned, op)
		l.ops.mu.Unlock()
		return DataBranchScratchGrant{Binding: l.binding, WitnessTS: tsOf(500)}, nil
	}
	if err := owner.BindOperator(op, in.ScratchTableID); err != nil {
		op.closeWithCommitErr(err)
		return DataBranchScratchGrant{}, err
	}
	if l.blockGrant != nil {
		select {
		case <-l.blockGrant:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			// fail() mirror on the cancel wake: the public Rollback rides
			// the grant operator's gate, exactly-once with the bail's own
			// abandonment either way.
			if gerr := op.Rollback(ctx); moerr.IsMoErrCode(gerr, moerr.ErrTxnClosed) {
				l.journal.add("public-rollback-gate-rejected")
			} else {
				l.journal.add("public-rollback-ran")
			}
			return DataBranchScratchGrant{}, ctx.Err()
		}
	}
	if l.onGrantSuccess != nil {
		l.onGrantSuccess()
	}
	return DataBranchScratchGrant{Binding: l.binding, WitnessTS: tsOf(500)}, nil
}

type stubStageExec struct {
	cli     *stubTxnClient
	journal *stageJournal
	ops     *stubRoundOps
	// fatalF2 injects the (H.10)(t)/(u) fatal DELETE error inside T_f2's
	// stage SQL and arms the unlock block on T_f2's operator.
	fatalF2 bool
	// blockF2Stmt, when non-nil, holds T_f2's stage SQL body until the
	// channel closes (the (o.5) blocked-SQL geometry).
	blockF2Stmt chan struct{}
	// blockF1PrePublish, when non-nil, holds F1's lane BEFORE the
	// certificate publication until the channel closes, ignoring the
	// lane's cancel (the (o)(iii)/(o.4) detached late-completion
	// geometry).
	blockF1PrePublish chan struct{}
	// onF1CommitEntry fires inside F1's commit decision point.
	onF1CommitEntry func()
}

func (e *stubStageExec) CommitRoundF1(
	ctx context.Context, in DataBranchFencingRoundInput,
) (DataBranchFencingF1Commit, error) {
	owner := DataBranchRoundOwnerFromCtx(ctx)
	pub := DataBranchLanePublicationFromCtx(ctx)
	cert := DataBranchFencingCertificate{
		CheckAEnvelopeDataObserved: true,
		CheckBEnvelopeDataObserved: true,
	}
	if owner == nil {
		// Legacy geometry: commit the slot operator directly.
		e.ops.mu.Lock()
		op := e.ops.grant
		e.ops.mu.Unlock()
		if op == nil {
			return DataBranchFencingF1Commit{}, moerr.NewInternalErrorNoCtx(
				"data branch fencing round f1: scratch grant missing")
		}
		e.journal.add("commit-entry")
		if err := op.Commit(ctx); err != nil {
			return DataBranchFencingF1Commit{}, err
		}
		return DataBranchFencingF1Commit{
			CommitTS: tsOf(800),
			Shape:    DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1},
			Cert:     cert,
		}, nil
	}
	op := owner.BoundOperator()
	if op == nil {
		return DataBranchFencingF1Commit{}, moerr.NewInternalErrorNoCtx(
			"data branch fencing round f1: scratch grant missing")
	}
	if e.blockF1PrePublish != nil {
		<-e.blockF1PrePublish
		e.journal.add("f1-pre-publish-released")
	}
	if pub != nil {
		e.journal.add("cert-published")
		pub.PublishCertificate(cert)
		pub.PublishTerminalEntry()
		e.journal.add("terminal-entry")
	}
	if e.onF1CommitEntry != nil {
		e.onF1CommitEntry()
	}
	if !owner.SelectCommitting() {
		e.journal.add("f1-select-rejected")
		return DataBranchFencingF1Commit{}, moerr.NewInternalErrorNoCtx(
			"data branch fencing round f1: terminal decision revoked")
	}
	e.journal.add("commit-entry")
	if err := op.Commit(ctx); err != nil {
		// NO in-body abandonment on a commit error (D187(B.3) mirror).
		return DataBranchFencingF1Commit{}, err
	}
	return DataBranchFencingF1Commit{
		CommitTS: tsOf(800),
		Shape:    DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1},
		Cert:     cert,
	}, nil
}

func (e *stubStageExec) CommitRoundF2(
	ctx context.Context, in DataBranchFencingRoundInput,
) (DataBranchFencingF2Commit, error) {
	owner := DataBranchRoundOwnerFromCtx(ctx)
	pub := DataBranchLanePublicationFromCtx(ctx)
	cert := DataBranchFencingCertificate{
		CheckAEnvelopeDataObserved: true,
		CheckBEnvelopeDataObserved: true,
	}
	op := e.cli.New(1)
	// hookOp needs the locks adapter's journal: reuse ours (same family).
	e.ops.mu.Lock()
	e.ops.f2 = op
	e.ops.mu.Unlock()
	op.AppendEventCallback(client.RollbackEvent, client.TxnEventCallback{
		Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error {
			e.journal.add("rollback-event")
			return nil
		}})
	op.AppendEventCallback(client.ClosedEvent, client.TxnEventCallback{
		Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error {
			e.journal.add("closed-event")
			return nil
		}})
	if owner != nil {
		if err := owner.BindOperator(op, in.ScratchTableID); err != nil {
			op.closeWithCommitErr(err)
			return DataBranchFencingF2Commit{}, err
		}
	} else {
		e.ops.mu.Lock()
		e.ops.unowned = append(e.ops.unowned, op)
		e.ops.mu.Unlock()
	}
	err := execStubStageSQL(op, func() error {
		if e.blockF2Stmt != nil {
			<-e.blockF2Stmt
		}
		if e.fatalF2 {
			// Arm the unlock block on THIS stage transaction before the
			// fatal error schedules its deferred rollback.
			e.cli.ls.ArmStallFor(op.id)
			e.journal.add("stage-sql-fatal")
			return stubFatalStageErr()
		}
		e.journal.add("f2-stage-ok")
		return nil
	})
	e.journal.add("f2-lane-return")
	if err != nil {
		// fail() mirror: the public Rollback hits the ALREADY-SEALED
		// terminal gate (D187/D188 recorded rejection).
		if gerr := op.Rollback(ctx); moerr.IsMoErrCode(gerr, moerr.ErrTxnClosed) {
			e.journal.add("public-rollback-gate-rejected")
		} else {
			e.journal.add("public-rollback-ran")
		}
		return DataBranchFencingF2Commit{}, err
	}
	if pub != nil {
		e.journal.add("cert-published")
		pub.PublishCertificate(cert)
		pub.PublishTerminalEntry()
		e.journal.add("terminal-entry")
	}
	if owner != nil && !owner.SelectCommitting() {
		e.journal.add("f2-select-rejected")
		return DataBranchFencingF2Commit{}, moerr.NewInternalErrorNoCtx(
			"data branch fencing round f2: terminal decision revoked")
	}
	e.journal.add("commit-entry")
	if err := op.Commit(ctx); err != nil {
		return DataBranchFencingF2Commit{}, err
	}
	return DataBranchFencingF2Commit{
		ProbeFound: true,
		CommitTS:   tsOf(900),
		Shape:      DataBranchFencingStageShape{TargetLedgerTable: true, PointTombstones: 1},
		Cert:       cert,
	}, nil
}

// ---------------------------------------------------------------------------
// (H.10)(v) ABORT-MARKER CONTENTION cell (D190 F1).
// ---------------------------------------------------------------------------

func TestDataBranchFencingCellVAbortMarkerContention(t *testing.T) {
	run := func(t *testing.T, reverted bool) {
		ctx := context.Background()
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{
			LanePoolCapacity: 2,
			DrainStepTimeout: 100 * time.Millisecond,
			BarrierTimeout:   2 * time.Second,
		})

		// A RETAINED rollback stalled in the injected unlock retry HOLDS
		// the operator mutex: armed deferred rollback (zero tokens) begins
		// cleanup at once and stalls in the service-lifetime unlock retry.
		ls.stallAll.Store(true)
		owner, class := pool.MintStageOwner(77, 1, "cell-v")
		require.Equal(t, DataBranchFencingRoundCommitted, class)
		op := cli.New(1)
		require.NoError(t, owner.BindOperator(op, 77))
		op.AppendEventCallback(client.RollbackEvent, client.TxnEventCallback{
			Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error {
				journal.add("rollback-event")
				return nil
			}})
		op.AppendEventCallback(client.ClosedEvent, client.TxnEventCallback{
			Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error {
				journal.add("closed-event")
				return nil
			}})
		op.RollbackAfterRunningSQL(stubFatalStageErr())
		// Rollback-entry is OBSERVED before any cancellation path
		// (D190(B.2)(ii)).
		waitForCondition(t, 2*time.Second, "rollback-entry observed",
			func() bool { return journal.has("rollback-event") })
		waitForCondition(t, 2*time.Second, "unlock stall entered",
			func() bool { return journal.has("unlock-stall-entered") })
		owner.NoteNoMoreLanes()
		snaps := pool.Dump()
		require.Equal(t, 1, snaps.Owners)
		require.Len(t, snaps.Retained, 1, "owner retained while cleanup stalls")

		// A concurrent ErrCannotCommitOnInvalidCN close walks
		// handleMarkActiveTxnAborted into addFlagIfGeneration on the SAME
		// operator — blocked behind its held mutex.
		wit := cli.New(0)
		wit.closeWithCommitErr(moerr.NewCannotCommitOnInvalidCNNoCtx())
		waitForCondition(t, 2*time.Second, "abort walk begun",
			func() bool { return journal.has("abort-walk-begin") })
		require.False(t, journal.has("abort-walk-done"),
			"the marking walk must be blocked behind the held operator mutex")

		require.NoError(t, pool.CloseStep(ctx))
		journal.add("close-step-done")
		journal.add("store-engine-closed")
		if !reverted {
			stubStopRPCsNormal(ls, cli)
		} else {
			// RED TWIN: the segment order reverted — the txn client's
			// stopper join waits past its budget on the held mutex.
			blocked := stubStopRPCsReverted(ls, cli, 200*time.Millisecond)
			require.True(t, blocked,
				"reverted order must reproduce the deadline witness")
			require.Greater(t, journal.index("txn-stopper-joined"),
				journal.index("unlock-discharged"),
				"the join completed only after the discharged stall freed the mutex")
		}
		// The discharged stall frees the deferred rollback to close; the
		// observed-close consumer performs the single release. Await the
		// release BEFORE the barrier so the residue verify sees the
		// completed registry, not a close consumption still in flight.
		waitForCondition(t, 2*time.Second, "owner released after discharge",
			func() bool { return pool.Dump().Charges == 0 })
		require.NoError(t, pool.DrainBarrier(ctx))
		journal.add("barrier-done")
		journal.requireOrder(t,
			"rollback-event", "unlock-stall-entered", "abort-walk-begin",
			"close-step-done", "ls-closed", "unlock-discharged",
			"abort-walk-done", "txn-stopper-joined", "barrier-done")
		dump := pool.Dump()
		require.Zero(t, dump.Charges)
		require.Zero(t, dump.Owners)
		require.Empty(t, dump.Retained)
		require.Zero(t, gauges.activeTxns.Load())
		require.Zero(t, gauges.heldLocks.Load())
		require.Equal(t, 1, journal.count("closed-event"), "exactly-once close")
	}

	t.Run("bounded-termination-through-reordered-segment", func(t *testing.T) {
		run(t, false)
	})
	t.Run("red-twin-reverted-segment-order", func(t *testing.T) {
		run(t, true)
	})
}

// ---------------------------------------------------------------------------
// (H.10)(u) OUTLAST-TIMEOUT DRAIN-CONTRACT cell (D190 re-seated) + twins.
// ---------------------------------------------------------------------------

func TestDataBranchFencingCellUDrainContract(t *testing.T) {
	run := func(t *testing.T, classificationDisabled bool, revertedWithContender bool) {
		ctx := context.Background()
		var restoreClass func()
		if classificationDisabled {
			restoreClass = DisableDataBranchFencingRetainedClassificationForTest(true)
			defer restoreClass()
		}
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{
			LanePoolCapacity: 2,
			DrainStepTimeout: 100 * time.Millisecond,
			BarrierTimeout:   2 * time.Second,
			RoundTimeout:     2 * time.Second,
		})
		driver := NewDataBranchFencingDriver(pool)

		owner, merr := driver.mintStageOwner(88, "t_f2-fatal")
		require.Nil(t, merr)
		op := cli.New(1)
		require.NoError(t, owner.BindOperator(op, 88))
		op.AppendEventCallback(client.RollbackEvent, client.TxnEventCallback{
			Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error {
				journal.add("rollback-event")
				return nil
			}})
		op.AppendEventCallback(client.ClosedEvent, client.TxnEventCallback{
			Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error {
				journal.add("closed-event")
				return nil
			}})
		ls.ArmStallFor(op.id)

		// (u.1) fatal-SQL schedule arms the deferred rollback with the
		// token held across the in-flight-statement window; (u.2) the
		// independent token-exit edge (execStubStageSQL's defer, mirroring
		// sql_executor.go:385-393) arms the drain.
		stepErr := func() error {
			_, err := driver.runStepOwned(ctx, DataBranchFencingStepRoundF2,
				func(lctx context.Context) (any, error) {
					return nil, execStubStageSQL(op, func() error {
						journal.add("stage-sql-fatal")
						return stubFatalStageErr()
					})
				}, owner, NewDataBranchFencingLanePublication())
			return err
		}()
		require.Error(t, stepErr)

		// (u.3) observed rollback-entry, then (u.4) the injected stall
		// holds the rollback in the service-lifetime retry HOLDING the
		// operator mutex; the lane exits.
		waitForCondition(t, 2*time.Second, "rollback-entry observed",
			func() bool { return journal.has("rollback-event") })
		waitForCondition(t, 2*time.Second, "unlock stall entered",
			func() bool { return journal.has("unlock-stall-entered") })
		require.Zero(t, op.activeTokenCount(), "token exited before lane exit")
		// The round bail abandonment rides the ALREADY-SEALED gate: NO-OP.
		owner.AbandonAsync()
		owner.NoteNoMoreLanes()

		// (u.4) RETAINED accounting: charge counted, zero lanes, zero
		// release, owner in PH-2 (rollback-entry observed, close not).
		waitForCondition(t, 2*time.Second, "lane exited",
			func() bool { return len(pool.Dump().Phases) == 0 })
		dump := pool.Dump()
		require.Equal(t, 1, dump.Charges)
		require.Equal(t, 1, dump.Owners)
		if !classificationDisabled {
			require.Len(t, dump.Retained, 1)
			for _, entry := range dump.Retained {
				require.Contains(t, entry.State, "retain",
					"owner PH-2: retained past lane exit with cleanup running")
			}
		}
		require.False(t, owner.ClosedObserved(), "zero RELEASE observed")
		require.Equal(t, int64(1), gauges.activeTxns.Load())
		require.Equal(t, int64(1), gauges.heldLocks.Load())

		// (u.5) the ACTUAL CN close chain: CloseStep classifies with NO
		// fail-stop, then the reordered stopRPCs segment discharges the
		// stall FIRST.
		require.NoError(t, pool.CloseStep(ctx))
		journal.add("close-step-done")
		if classificationDisabled {
			require.Zero(t, pool.lastCloseStepRetained.Load(),
				"RED TWIN (a): the classification surface reads zero retained")
		} else {
			require.Equal(t, int64(1), pool.lastCloseStepRetained.Load())
		}
		if classificationDisabled {
			// RED TWIN (a): the retained registry is invisible while the
			// owner lives (the silent-retained witness); the barrier's
			// residue verification refuses the unverified zero LOUDLY.
			err := pool.DrainBarrier(ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "unverified residue")
			stubStopRPCsNormal(ls, cli)
			waitForCondition(t, 2*time.Second, "owner released after discharge",
				func() bool { return pool.Dump().Charges == 0 })
			require.NoError(t, pool.DrainBarrier(ctx))
			journal.add("barrier-done")
		} else if revertedWithContender {
			// RED TWIN (b): the (v)-family contender composed in per the
			// approval advisories — an abort-marking walk blocks on the
			// same held mutex, and the reverted segment order reproduces
			// the deadline witness with the barrier unreachable.
			wit := cli.New(0)
			wit.closeWithCommitErr(moerr.NewCannotCommitOnInvalidCNNoCtx())
			waitForCondition(t, 2*time.Second, "abort walk begun",
				func() bool { return journal.has("abort-walk-begin") })
			require.False(t, journal.has("abort-walk-done"))
			blocked := stubStopRPCsReverted(ls, cli, 200*time.Millisecond)
			require.True(t, blocked, "reverted order must reproduce the deadline witness")
			require.Greater(t, journal.index("txn-stopper-joined"),
				journal.index("unlock-discharged"))
			waitForCondition(t, 2*time.Second, "owner released after discharge",
				func() bool { return pool.Dump().Charges == 0 })
			require.NoError(t, pool.DrainBarrier(ctx))
			journal.add("barrier-done")
			journal.requireOrder(t,
				"abort-walk-begin", "ls-closed", "unlock-discharged",
				"abort-walk-done", "txn-stopper-joined", "barrier-done")
		} else {
			stubStopRPCsNormal(ls, cli)
			waitForCondition(t, 2*time.Second, "owner released after discharge",
				func() bool { return pool.Dump().Charges == 0 })
			require.NoError(t, pool.DrainBarrier(ctx))
			journal.add("barrier-done")
			journal.requireOrder(t,
				"rollback-event", "unlock-stall-entered", "close-step-done",
				"ls-closed", "unlock-discharged", "closed-event", "barrier-done")
		}
		final := pool.Dump()
		require.Zero(t, final.Charges)
		require.Zero(t, final.Owners)
		require.Zero(t, gauges.activeTxns.Load())
		require.Zero(t, gauges.heldLocks.Load())
		require.Equal(t, 1, journal.count("rollback-event"),
			"exactly-once cleanup entry")
		require.Equal(t, 1, journal.count("closed-event"),
			"exactly-once release at the observed close")
	}

	t.Run("two-phase-drain-contract", func(t *testing.T) {
		run(t, false, false)
	})
	t.Run("red-twin-a-classification-disabled", func(t *testing.T) {
		run(t, true, false)
	})
	t.Run("red-twin-b-reverted-order-with-contender", func(t *testing.T) {
		run(t, false, true)
	})
}

// ---------------------------------------------------------------------------
// (H.10)(t) RE-SEATED SHARED-OWNERSHIP T_f2 FATAL cell (D191(H)) + twin.
// ---------------------------------------------------------------------------

func TestDataBranchFencingCellTF2FatalSaturation(t *testing.T) {
	run := func(t *testing.T, boundaryDisabled bool) {
		ctx := context.Background()
		var restoreBoundary func()
		if boundaryDisabled {
			restoreBoundary = DisableDataBranchFencingObservedCloseBoundaryForTest(true)
			defer restoreBoundary()
		}
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{
			LanePoolCapacity: 2,
			DrainStepTimeout: 100 * time.Millisecond,
			BarrierTimeout:   2 * time.Second,
			RoundTimeout:     5 * time.Second,
		})
		driver := NewDataBranchFencingDriver(pool)
		ops := &stubRoundOps{}
		locks := &stubScratchLocks{
			cli:     cli,
			journal: journal,
			ops:     ops,
			binding: DataBranchFencingBinding{Table: 1001, ServiceID: "svc-a", Version: 7},
		}
		exec := &stubStageExec{cli: cli, journal: journal, ops: ops, fatalF2: true}

		baseTxns := gauges.activeTxns.Load()
		baseLocks := gauges.heldLocks.Load()

		// (t.1) T_f2's stage SQL fatals; fail()'s public Rollback
		// gate-rejects; the executor stub arms the deferred rollback and
		// releases the token at the independent edge BEFORE lane exit;
		// rollback-entry is observed; the injected unlock-stall holds the
		// genuinely outstanding tail.
		_, err := driver.RunRound(ctx, locks, exec, DataBranchFencingRoundInput{
			ScratchTableID:     99,
			IssueTimeNS:        1_700_000,
			LastWriterCommitTS: tsOf(700),
		})
		require.Error(t, err)
		fe, ok := AsDataBranchFencingError(err)
		require.True(t, ok)
		require.Equal(t, DataBranchFencingRoundStageFailed, fe.Class)
		require.Equal(t, DataBranchFencingStepRoundF2, fe.Step)
		require.True(t, journal.has("public-rollback-gate-rejected"),
			"fail()'s public Rollback gate-rejects (the recorded D187/D188 gate)")

		ops.mu.Lock()
		f2op := ops.f2
		ops.mu.Unlock()
		require.NotNil(t, f2op)
		waitForCondition(t, 2*time.Second, "rollback-entry observed via RollbackEvent",
			func() bool { return journal.has("rollback-event") })
		waitForCondition(t, 2*time.Second, "cleanup held in the unlock tail",
			func() bool { return journal.has("unlock-stall-entered") })
		require.Zero(t, f2op.activeTokenCount(),
			"the SQL token released at the independent token-exit edge BEFORE lane exit")

		// Assert AT lane exit (PH-2 by construction): the T_f2 owner's
		// charge HELD in RETAINING, the scratch transaction LIVE, the
		// held-lock oracle above baseline from the genuinely outstanding
		// unlock tail, zero RELEASE observed.
		if !boundaryDisabled {
			waitForCondition(t, 2*time.Second, "T_f1 owner released, T_f2 retained",
				func() bool {
					d := pool.Dump()
					return d.Charges == 1 && d.Owners == 1
				})
			dump := pool.Dump()
			require.Len(t, dump.Retained, 1, "zero RELEASE observed")
			for _, entry := range dump.Retained {
				require.Equal(t, "round-f2", entry.Stage,
					"the retained entry is the T_f2 owner")
				require.Equal(t, "retaining", entry.State)
			}
			require.Equal(t, baseTxns+1, gauges.activeTxns.Load(),
				"scratch transaction LIVE above baseline")
			require.Equal(t, baseLocks+1, gauges.heldLocks.Load(),
				"held locks above baseline from the outstanding unlock tail")
		} else {
			// RED TWIN: the accounting mechanically reverted to lane-exit
			// release — the charge frees while cleanup is genuinely live.
			waitForCondition(t, 2*time.Second, "charge released at lane exit",
				func() bool { return pool.Dump().Charges == 0 })
			require.Equal(t, baseTxns+1, gauges.activeTxns.Load(),
				"F1's released-while-cleanup-live witness: transaction still live")
			require.Equal(t, baseLocks+1, gauges.heldLocks.Load())
		}

		// (t.2) SATURATION: with the budget filled by one live lane plus
		// the retained PH-2 owner, the next reserve REFUSES with the
		// recorded class — NO uncharged admission.
		parked := make(chan struct{})
		parkedLane, class := pool.spawn(DataBranchFencingStepGrant,
			func(lctx context.Context) (any, error) {
				<-parked
				return nil, nil
			}, ctx)
		require.NotNil(t, parkedLane)
		require.Equal(t, DataBranchFencingRoundCommitted, class)
		wantCharges := 2
		if boundaryDisabled {
			wantCharges = 1
		}
		waitForCondition(t, 2*time.Second, "parked lane charged",
			func() bool { return pool.Dump().Charges == wantCharges })
		extra, mintErr := driver.mintStageOwner(1000, "saturation-probe")
		if !boundaryDisabled {
			require.Nil(t, extra)
			require.NotNil(t, mintErr)
			require.Equal(t, DataBranchFencingRoundSaturated, mintErr.Class,
				"the recorded saturation class refuses the next reserve")
		} else {
			// RED TWIN: with the charge freed at lane exit, the reserve
			// ADMITS uncharged work while cleanup is live.
			require.NotNil(t, extra)
			require.Nil(t, mintErr)
			extra.AbandonAsync()
			extra.NoteNoMoreLanes()
		}
		close(parked)
		waitForCondition(t, 2*time.Second, "parked lane drained",
			func() bool { return pool.Dump().Charges <= wantCharges-1 })

		// (t.3) RELEASE CLEANUP: discharge the injected stall; the
		// rollback runs to closeLocked, the ClosedEvent publishes, the
		// owner releases EXACTLY ONCE, and every baseline returns exactly.
		ls.Close()
		// Discharge BEFORE the exactly-once/baseline assertions (D191(H)
		// (t.3)): the rollback runs to closeLocked and the observed-close
		// consumer performs the single release; the barrier then verifies
		// an already-completed registry.
		waitForCondition(t, 2*time.Second, "T_f2 owner released after discharge",
			func() bool {
				d := pool.Dump()
				return d.Charges == 0 && d.Owners == 0 && len(d.Retained) == 0
			})
		// The RED twin released the OWNER at lane exit while cleanup was
		// still live, so its release wait cannot witness the close:
		// await T_f1's and T_f2's ClosedEvent explicitly in both arms.
		waitForCondition(t, 2*time.Second, "both closes observed",
			func() bool { return journal.count("closed-event") == 2 })
		require.NoError(t, pool.DrainBarrier(ctx))
		journal.add("barrier-done")
		require.Equal(t, baseTxns, gauges.activeTxns.Load(), "baseline returns exactly")
		require.Equal(t, baseLocks, gauges.heldLocks.Load(), "baseline returns exactly")
		require.Equal(t, 1, journal.count("rollback-event"), "exactly-once cleanup")
		require.Equal(t, 2, journal.count("closed-event"),
			"T_f1 and T_f2 closed exactly once each")
	}

	t.Run("retained-charging-saturation-and-exactly-once-release", func(t *testing.T) {
		run(t, false)
	})
	t.Run("red-twin-lane-exit-release", func(t *testing.T) {
		run(t, true)
	})
}
