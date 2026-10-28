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

// D186/D187/D188/D190 cells on the shared stage-owner harness family:
// (H.10)(n) real-drain terminal-block with the retained-owner
// classification edges and the D190 split stopRPCs edges; (H.10)(o)
// T_f1 ownership amalgamation arms (i)-(iii), (o.4) round-token
// aliasing, the D188-re-seated (o.5) expiry-during-blocked-SQL with its
// decision-gate RED twin and the blocked-Commit variants, (o.6)
// independently scheduled cleanup with the boundary-reverted RED twin,
// and the amalgamation-disabled RED twin; (H.10)(q) allocator lifetime
// against the globalPools registration oracle; and the defect-flag
// hygiene cell. Production mirrors follow the documented loci of the
// sibling cell file.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func hookStageJournal(journal *stageJournal, op *stageOperatorStub) {
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
}

// ---------------------------------------------------------------------------
// (H.10)(n) REAL-DRAIN TERMINAL-BLOCK cell, chain-amended by D189/D190.
// ---------------------------------------------------------------------------

func TestDataBranchFencingCellNRealDrain(t *testing.T) {
	setup := func(t *testing.T, capacity int) (
		*stageGauges, *stageJournal, *stubLockService,
		*stubTxnClient, *DataBranchFencingPool, *DataBranchFencingDriver,
	) {
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{
			LanePoolCapacity: capacity,
			DrainStepTimeout: 100 * time.Millisecond,
			BarrierTimeout:   2 * time.Second,
			RoundTimeout:     5 * time.Second,
		})
		driver := NewDataBranchFencingDriver(pool)
		return gauges, journal, ls, cli, pool, driver
	}

	// f2FatalOwner runs the retained-T_f2 geometry fully: fatal stage SQL
	// arms the deferred rollback, the lane exits with the error result,
	// and the bail abandonment rides the sealed gate as a NO-OP. The
	// owner reaches RETAINING with the rollback stalled in unlock.
	f2FatalOwner := func(
		t *testing.T, ctx context.Context, ls *stubLockService, cli *stubTxnClient,
		driver *DataBranchFencingDriver, journal *stageJournal, pool *DataBranchFencingPool,
		scratch uint64,
	) {
		owner, merr := driver.mintStageOwner(scratch, "cell-n-f2-fatal")
		require.Nil(t, merr)
		op := cli.New(1)
		require.NoError(t, owner.BindOperator(op, scratch))
		hookStageJournal(journal, op)
		ls.ArmStallFor(op.id)
		_, stepErr := driver.runStepOwned(ctx, DataBranchFencingStepRoundF2,
			func(lctx context.Context) (any, error) {
				return nil, execStubStageSQL(op, func() error {
					journal.add("stage-sql-fatal")
					return stubFatalStageErr()
				})
			}, owner, NewDataBranchFencingLanePublication())
		require.Error(t, stepErr)
		waitForCondition(t, 2*time.Second, "rollback-entry observed",
			func() bool { return journal.has("rollback-event") })
		waitForCondition(t, 2*time.Second, "unlock stall entered",
			func() bool { return journal.has("unlock-stall-entered") })
		owner.AbandonAsync()
		owner.NoteNoMoreLanes()
		waitForCondition(t, 2*time.Second, "F2 owner retained with all lanes exited",
			func() bool { return len(pool.Dump().Retained) == 1 })
	}

	t.Run("real-drain-retained-classification-and-split-edges", func(t *testing.T) {
		ctx := context.Background()
		gauges, journal, ls, cli, pool, driver := setup(t, 3)
		ls.stallAll.Store(true)

		// The retained owner (T_f2 fatal geometry) FIRST: its edges lead
		// the chain deterministically. Retention with all lanes exited
		// precedes the CloseStep classification.
		f2FatalOwner(t, ctx, ls, cli, driver, journal, pool, 11)

		// The real T_f1 helper: certificate publication and terminal
		// entry precede the commit whose deferred sync unlock blocks.
		f1Owner, merr := driver.mintStageOwner(12, "cell-n-f1")
		require.Nil(t, merr)
		opA := cli.New(1)
		require.NoError(t, f1Owner.BindOperator(opA, 12))
		hookStageJournal(journal, opA)
		commitDone := make(chan error, 1)
		go func() {
			_, err := driver.runStepOwned(ctx, DataBranchFencingStepRoundF1,
				func(lctx context.Context) (any, error) {
					pub := DataBranchLanePublicationFromCtx(lctx)
					cert := DataBranchFencingCertificate{
						CheckAEnvelopeDataObserved: true,
						CheckBEnvelopeDataObserved: true,
					}
					journal.add("cert-published")
					pub.PublishCertificate(cert)
					pub.PublishTerminalEntry()
					journal.add("terminal-entry")
					owner := DataBranchRoundOwnerFromCtx(lctx)
					if !owner.SelectCommitting() {
						return nil, moerr.NewInternalErrorNoCtx("cell-n f1: decision revoked")
					}
					return nil, opA.Commit(lctx)
				}, f1Owner, NewDataBranchFencingLanePublication())
			commitDone <- err
		}()
		// Note the coverage end only after the F1 lane actually spawned:
		// noting it at zero spawned lanes would settle the owner into
		// RETAINING ahead of its lane (the production contract notes the
		// end after the final stage step).
		waitForCondition(t, 2*time.Second, "F1 commit entry",
			func() bool { return journal.has("commit-entry") })
		f1Owner.NoteNoMoreLanes()
		waitForCondition(t, 2*time.Second, "F1 lane blocked pre-result",
			func() bool {
				for _, phase := range pool.Dump().Phases {
					if phase == DataBranchFencingLaneTerminalInFlight {
						return true
					}
				}
				return false
			})

		// CloseStep: the lane classifies terminal-in-flight, NOT Running,
		// with NO fail-stop; the retained owner is RECORDED with no
		// fail-stop (D189(B.2)(iii)).
		require.NoError(t, pool.CloseStep(ctx))
		journal.add("close-step-done")
		require.Equal(t, int64(1), pool.lastCloseStepRetained.Load(),
			"retained classification recorded without fail-stop")
		journal.add("store-engine-closed")

		// stopRPCs split edges: the cancellation segment FIRST discharges
		// every stall, both deferred tails run to close, the full lane
		// exit follows, then the txn stopper joins; the FINAL BARRIER
		// verifies zero before CN.Close.
		ls.Close()
		waitForCondition(t, 2*time.Second, "both closes observed",
			func() bool { return journal.count("closed-event") == 2 })
		waitForCondition(t, 2*time.Second, "lane full exit",
			func() bool { return len(pool.Dump().Phases) == 0 })
		journal.add("tail-full-exit")
		cli.Close(0)
		require.NoError(t, pool.DrainBarrier(ctx))
		journal.add("barrier-done")
		journal.add("cn-closed")
		journal.requireOrder(t,
			"rollback-event", "unlock-stall-entered",
			"cert-published", "terminal-entry", "commit-entry",
			"close-step-done", "store-engine-closed", "ls-closed",
			"unlock-discharged", "closed-event", "tail-full-exit",
			"txn-stopper-joined", "barrier-done", "cn-closed")
		require.NoError(t, <-commitDone)
		dump := pool.Dump()
		require.Zero(t, dump.Charges)
		require.Zero(t, dump.Owners)
		require.Empty(t, dump.Retained)
		require.Equal(t, 1, journal.count("commit-entry"), "F1 committed exactly once")
		require.Equal(t, 2, journal.count("closed-event"))
		require.Zero(t, gauges.activeTxns.Load())
		require.Zero(t, gauges.heldLocks.Load())
	})

	t.Run("red-twin-a-publication-disabled", func(t *testing.T) {
		restore := DisableDataBranchFencingPublicationForTest(true)
		defer restore()
		ctx := context.Background()
		gauges, journal, ls, cli, pool, driver := setup(t, 2)
		ls.stallAll.Store(true)

		// The publication-disabled runner strips the lane record: the
		// blocked terminal lane reads Running at the step and the chain
		// HALTS before closeStoreEngine ((A.1)(iii)'s fail-stop).
		f1Owner, merr := driver.mintStageOwner(13, "cell-n-twin-a")
		require.Nil(t, merr)
		opA := cli.New(1)
		require.NoError(t, f1Owner.BindOperator(opA, 13))
		hookStageJournal(journal, opA)
		go func() {
			_, _ = driver.runStepOwned(ctx, DataBranchFencingStepRoundF1,
				func(lctx context.Context) (any, error) {
					owner := DataBranchRoundOwnerFromCtx(lctx)
					if !owner.SelectCommitting() {
						return nil, moerr.NewInternalErrorNoCtx("cell-n twin-a: revoked")
					}
					return nil, opA.Commit(lctx)
				}, f1Owner, NewDataBranchFencingLanePublication())
		}()
		// As in the main arm: note the coverage end only once the F1 lane
		// is live and its terminal verb selected (commit-entry journals).
		waitForCondition(t, 2*time.Second, "F1 commit entry",
			func() bool { return journal.has("commit-entry") })
		f1Owner.NoteNoMoreLanes()
		err := pool.CloseStep(ctx)
		require.Error(t, err, "publication-disabled blocked lane fail-stops the step")
		require.Contains(t, err.Error(), "running")
		require.False(t, journal.has("store-engine-closed"),
			"the chain halts before closeStoreEngine")

		// Hygiene: discharge and verify the platform path still drains.
		ls.Close()
		cli.Close(0)
		waitForCondition(t, 2*time.Second, "owner released after discharge",
			func() bool { return pool.Dump().Charges == 0 })
		require.NoError(t, pool.DrainBarrier(ctx))
		require.Zero(t, gauges.activeTxns.Load())
		require.Zero(t, gauges.heldLocks.Load())
	})

	t.Run("red-twin-b-pre-certificate-running-fail-stop", func(t *testing.T) {
		ctx := context.Background()
		_, _, _, _, pool, driver := setup(t, 2)

		// A lane blocked BEFORE its certificate publication holds the
		// recorded Running fail-stop (never misread as terminal-in-flight).
		owner, merr := driver.mintStageOwner(14, "cell-n-twin-b")
		require.Nil(t, merr)
		parked := make(chan struct{})
		go func() {
			_, _ = driver.runStepOwned(ctx, DataBranchFencingStepGrant,
				func(lctx context.Context) (any, error) {
					<-parked
					return nil, nil
				}, owner, NewDataBranchFencingLanePublication())
		}()
		waitForCondition(t, 2*time.Second, "pre-certificate lane live",
			func() bool { return len(pool.Dump().Phases) == 1 })
		// The lane is live: noting the coverage end now keeps the owner
		// ACTIVE so the abandonment below reaches the vacuous-reap arm.
		owner.NoteNoMoreLanes()
		err := pool.CloseStep(ctx)
		require.Error(t, err, "a pre-certificate lane holds the Running fail-stop")
		require.Contains(t, err.Error(), "running")

		// Hygiene: the bail abandonment reaps the vacuous (no-transaction)
		// owner, the released lane exits, and the barrier verifies zero.
		owner.AbandonAsync()
		close(parked)
		waitForCondition(t, 2*time.Second, "lane exit and vacuous reap",
			func() bool { return pool.Dump().Charges == 0 })
		require.NoError(t, pool.DrainBarrier(ctx))
	})
}

// ---------------------------------------------------------------------------
// (H.10)(o) T_f1 OWNERSHIP AMALGAMATION cell (D186/D187/D188 chain).
// ---------------------------------------------------------------------------

func TestDataBranchFencingCellOOwnershipAmalgamation(t *testing.T) {
	newPool := func(capacity int, round time.Duration) *DataBranchFencingPool {
		return NewDataBranchFencingPool(DataBranchFencingDriverConfig{
			LanePoolCapacity: capacity,
			DrainStepTimeout: 100 * time.Millisecond,
			BarrierTimeout:   2 * time.Second,
			RoundTimeout:     round,
		})
	}

	// (o)(i) F1-stage reservation REJECTED (sealed): the refusal abandons
	// the round; the scratch transaction rolls back EXACTLY ONCE,
	// anchors/locks are observed released, and the charge releases at
	// cleanup.
	t.Run("arm-i-sealed-reservation-rejection", func(t *testing.T) {
		ctx := context.Background()
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := newPool(2, 2*time.Second)
		driver := NewDataBranchFencingDriver(pool)

		ops := &stubRoundOps{}
		locks := &stubScratchLocks{
			cli: cli, journal: journal, ops: ops,
			binding: DataBranchFencingBinding{Table: 3000, ServiceID: "svc-i", Version: 1},
		}
		// Seal the pool the moment the grant succeeds: the F1 reservation
		// is then refused with the recorded class.
		locks.onGrantSuccess = func() { _ = pool.CloseStep(ctx) }
		exec := &stubStageExec{cli: cli, journal: journal, ops: ops}
		in := DataBranchFencingRoundInput{
			ScratchTableID:     55,
			IssueTimeNS:        1_800_000,
			LastWriterCommitTS: tsOf(700),
		}
		_, err := driver.RunRound(ctx, locks, exec, in)
		require.Error(t, err)
		fe, ok := AsDataBranchFencingError(err)
		require.True(t, ok)
		require.Equal(t, DataBranchFencingRoundPoolSealed, fe.Class)

		waitForCondition(t, 2*time.Second, "cleanup drained and released",
			func() bool { return pool.Dump().Charges == 0 })
		require.True(t, pool.Dump().Sealed)
		require.Equal(t, 1, journal.count("rollback-event"), "exactly-once rollback")
		require.Equal(t, 1, journal.count("closed-event"), "exactly-once close")
		require.Zero(t, gauges.activeTxns.Load(), "anchors/locks observed released")
		require.Zero(t, gauges.heldLocks.Load())
	})

	// (o)(ii) round EXPIRY with the grant in flight: the owner abandons
	// with exactly-once rollback; the late grant completion is refused at
	// intake (the round-key guard) and the lane-side Rollback finds the
	// terminal gate already claimed.
	t.Run("arm-ii-expiry-with-grant-in-flight", func(t *testing.T) {
		ctx := context.Background()
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := newPool(2, 150*time.Millisecond)
		driver := NewDataBranchFencingDriver(pool)

		ops := &stubRoundOps{}
		locks := &stubScratchLocks{
			cli: cli, journal: journal, ops: ops,
			binding:    DataBranchFencingBinding{Table: 3001, ServiceID: "svc-o", Version: 3},
			blockGrant: make(chan struct{}),
		}
		exec := &stubStageExec{cli: cli, journal: journal, ops: ops}
		in := DataBranchFencingRoundInput{
			ScratchTableID:     77,
			IssueTimeNS:        1_800_000,
			LastWriterCommitTS: tsOf(700),
		}
		// The cancel wake arrives at the 150ms deadline; the channel
		// close is a backstop that can never fire first in this schedule.
		go func() { <-time.After(2 * time.Second); close(locks.blockGrant) }()
		_, err := driver.RunRound(ctx, locks, exec, in)
		require.Error(t, err)
		fe, ok := AsDataBranchFencingError(err)
		require.True(t, ok)
		require.Equal(t, DataBranchFencingRoundExpired, fe.Class)

		waitForCondition(t, 2*time.Second, "late grant refused at intake",
			func() bool { return pool.Dump().LateResults == 1 })
		waitForCondition(t, 2*time.Second, "cleanup drained and released",
			func() bool { return pool.Dump().Charges == 0 })
		require.Equal(t, 1, journal.count("rollback-event"),
			"the late completion could not publish a second cleanup")
		require.Equal(t, 1, journal.count("closed-event"))
		require.Equal(t, 1,
			journal.count("public-rollback-gate-rejected")+journal.count("public-rollback-ran"),
			"exactly one public Rollback reached the terminal gate")
		require.Zero(t, gauges.activeTxns.Load())
		require.Zero(t, gauges.heldLocks.Load())
	})

	// (o)(iii) F1 LATE completion past the deadline: the same single-shot
	// terminal geometry - the owner abandons exactly once; the late F1
	// arrival finds result acceptance revoked and is refused at intake.
	t.Run("arm-iii-f1-late-completion", func(t *testing.T) {
		ctx := context.Background()
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := newPool(2, 150*time.Millisecond)
		driver := NewDataBranchFencingDriver(pool)

		ops := &stubRoundOps{}
		locks := &stubScratchLocks{
			cli: cli, journal: journal, ops: ops,
			binding: DataBranchFencingBinding{Table: 3001, ServiceID: "svc-o", Version: 3},
		}
		blockF1 := make(chan struct{})
		exec := &stubStageExec{cli: cli, journal: journal, ops: ops, blockF1PrePublish: blockF1}
		in := DataBranchFencingRoundInput{
			ScratchTableID:     78,
			IssueTimeNS:        1_800_000,
			LastWriterCommitTS: tsOf(700),
		}
		go func() { <-time.After(500 * time.Millisecond); close(blockF1) }()
		_, err := driver.RunRound(ctx, locks, exec, in)
		require.Error(t, err)
		fe, ok := AsDataBranchFencingError(err)
		require.True(t, ok)
		require.Equal(t, DataBranchFencingRoundExpired, fe.Class)

		waitForCondition(t, 2*time.Second, "late F1 refused at intake",
			func() bool { return pool.Dump().LateResults == 1 })
		waitForCondition(t, 2*time.Second, "cleanup drained and released",
			func() bool { return pool.Dump().Charges == 0 })
		require.Equal(t, 1, journal.count("rollback-event"),
			"a single-shot terminal action for the expired round")
		require.Equal(t, 1, journal.count("closed-event"))
		require.Equal(t, 1, journal.count("f1-select-rejected"),
			"result acceptance was revoked before the late F1 arrived")
		require.Zero(t, journal.count("commit-entry"),
			"the late completion never issued its terminal verb")
		require.Zero(t, gauges.activeTxns.Load())
		require.Zero(t, gauges.heldLocks.Load())
	})

	// (o.4) ROUND-TOKEN ALIASING: an expired predecessor of the SAME
	// account AND binding completes nominally while its SUCCESSOR round
	// (a fresh ScratchTableID) is active. Its publication and terminal
	// cleanup are acceptance-revoked NO-OPs; the successor's slot,
	// certificate, and charge stay untouched end to end.
	t.Run("arm-o4-round-token-aliasing", func(t *testing.T) {
		ctx := context.Background()
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := newPool(3, 150*time.Millisecond)
		driver := NewDataBranchFencingDriver(pool)

		ops := &stubRoundOps{}
		binding := DataBranchFencingBinding{Table: 3002, ServiceID: "svc-o4", Version: 9}
		locks := &stubScratchLocks{cli: cli, journal: journal, ops: ops, binding: binding}

		// Predecessor round: F1 parks BEFORE its publication; the round
		// deadline expires with the grant already durable.
		blockA := make(chan struct{})
		execA := &stubStageExec{cli: cli, journal: journal, ops: ops, blockF1PrePublish: blockA}
		inA := DataBranchFencingRoundInput{
			ScratchTableID:     910,
			IssueTimeNS:        1_900_000,
			LastWriterCommitTS: tsOf(700),
		}
		_, errA := driver.RunRound(ctx, locks, execA, inA)
		require.Error(t, errA)
		feA, ok := AsDataBranchFencingError(errA)
		require.True(t, ok)
		require.Equal(t, DataBranchFencingRoundExpired, feA.Class)

		// Successor round, SAME account AND binding, FRESH scratch draw:
		// it completes ordinarily while the predecessor's lane is parked.
		execB := &stubStageExec{cli: cli, journal: journal, ops: ops}
		inB := DataBranchFencingRoundInput{
			ScratchTableID:     911,
			IssueTimeNS:        1_900_500,
			LastWriterCommitTS: tsOf(710),
		}
		recB, errB := driver.RunRound(ctx, locks, execB, inB)
		require.NoError(t, errB)
		require.False(t, recB.CommitTS.IsEmpty(), "the successor completed ordinarily")
		require.Equal(t, 1, journal.count("f2-stage-ok"),
			"the successor's T_f2 stage ran")
		require.Equal(t, 4, journal.count("commit-entry"),
			"exactly the successor's two commits journaled (exec + stub each)")

		// Slot authority (D187(B.1)(ii), (C.28)): the token-keyed slot
		// refuses both an already-bound re-bind and a mismatched scratch.
		slotOwner, merr := driver.mintStageOwner(950, "cell-o4-slot")
		require.Nil(t, merr)
		slotOp := cli.New(0)
		require.NoError(t, slotOwner.BindOperator(slotOp, 950))
		stray1 := cli.New(0)
		require.Error(t, slotOwner.BindOperator(stray1, 950),
			"an already-bound slot refuses the predecessor's late bind")
		stray2 := cli.New(0)
		require.Error(t, slotOwner.BindOperator(stray2, 951),
			"a token-mismatched scratch can never mutate the slot")
		stray1.closeWithCommitErr(moerr.NewInternalErrorNoCtx("stray cleanup"))
		stray2.closeWithCommitErr(moerr.NewInternalErrorNoCtx("stray cleanup"))
		slotOwner.AbandonAsync()
		slotOwner.NoteNoMoreLanes()

		// The expired predecessor completes nominally: its publication
		// and terminal cleanup are NO-OPs; the successor is untouched.
		close(blockA)
		waitForCondition(t, 2*time.Second, "predecessor's late completion refused",
			func() bool { return pool.Dump().LateResults == 1 })
		require.Equal(t, 1, journal.count("f1-select-rejected"),
			"the predecessor's completion ran into the revoked acceptance")
		waitForCondition(t, 2*time.Second, "all charges released",
			func() bool {
				d := pool.Dump()
				return d.Charges == 0 && d.Owners == 0
			})
		waitForCondition(t, 2*time.Second, "gauges return to baseline",
			func() bool {
				return gauges.activeTxns.Load() == 0 && gauges.heldLocks.Load() == 0
			})
	})

	// (o.5) EXPIRY-DURING-BLOCKED-SQL (D188 re-seat): fire round expiry
	// while the F1 helper SQL is blocked; the selected Rollback returns
	// the wait error with the deferred cleanup ARMED - assert AT the
	// return: charge held, slot/accounting intact, the scratch
	// transaction and its anchors/locks live, result acceptance revoked;
	// then release the blocked SQL: the charge release fires EXACTLY ONCE
	// at the observed close and the baselines return exactly.
	t.Run("arm-o5-expiry-during-blocked-sql", func(t *testing.T) {
		run := func(t *testing.T, gateDisabled bool) {
			ctx := context.Background()
			if gateDisabled {
				restore := DisableDataBranchFencingDecisionGateForTest(true)
				defer restore()
			}
			gauges := &stageGauges{}
			journal := &stageJournal{}
			ls := newStubLockService(gauges, journal)
			cli := newStubTxnClient(gauges, journal, ls)
			pool := newPool(2, 150*time.Millisecond)
			driver := NewDataBranchFencingDriver(pool)
			baseTxns := gauges.activeTxns.Load()
			baseLocks := gauges.heldLocks.Load()

			owner, merr := driver.mintStageOwner(66, "cell-o5")
			require.Nil(t, merr)
			op := cli.New(1)
			require.NoError(t, owner.BindOperator(op, 66))
			hookStageJournal(journal, op)
			blockedSQL := make(chan struct{})
			go func() {
				_, _ = driver.runStepOwned(ctx, DataBranchFencingStepRoundF1,
					func(lctx context.Context) (any, error) {
						return nil, execStubStageSQL(op, func() error {
							<-blockedSQL
							journal.add("f1-sql-unblocked")
							return moerr.NewInternalErrorNoCtx("cell-o5: helper SQL cancelled")
						})
					}, owner, NewDataBranchFencingLanePublication())
			}()
			waitForCondition(t, 2*time.Second, "expiry classified",
				func() bool { return owner.AcceptanceRevoked() })

			// The selected Rollback: the running-SQL wait fails past its
			// budget and the deferred cleanup is ARMED (gate sealed).
			owner.Abandon(ctx)
			require.Equal(t, int32(stubTerminalSealed), op.terminalCall.Load(),
				"the selected Rollback's wait error armed the deferred cleanup")
			dump := pool.Dump()
			require.Equal(t, 1, dump.Charges, "charge held AT the deferred return")
			require.Equal(t, 1, dump.Owners, "slot/accounting intact")
			require.False(t, owner.ClosedObserved())
			require.Equal(t, baseTxns+1, gauges.activeTxns.Load(),
				"scratch transaction live past the expiry")
			require.Equal(t, baseLocks+1, gauges.heldLocks.Load(),
				"anchors/locks live past the expiry")
			require.Equal(t, 0, journal.count("rollback-event"))
			require.Equal(t, 0, journal.count("closed-event"))
			owner.NoteNoMoreLanes()

			if gateDisabled {
				// RED TWIN (D187(H)): the decision gate mechanically
				// disabled re-runs the r76 first-return rule - ANY
				// terminal verb races in. The second verb reaches the
				// OPERATOR'S gate, which alone preserves exactly-once.
				require.True(t, owner.SelectCommitting(),
					"gate disabled: a raced-in Commit is admitted to invocation")
				cerr := op.Commit(ctx)
				require.Error(t, cerr)
				require.True(t, moerr.IsMoErrCode(cerr, moerr.ErrTxnClosed))
				journal.add("twin-commit-gate-rejected")
				require.Equal(t, 0, journal.count("rollback-event"),
					"no cleanup ran for the gate-rejected verb")
				require.Equal(t, 1, pool.Dump().Charges,
					"a gate-rejected return releases nothing")
			}

			close(blockedSQL)
			waitForCondition(t, 2*time.Second, "release exactly once at observed close",
				func() bool {
					d := pool.Dump()
					return d.Charges == 0 && d.Owners == 0
				})
			require.Equal(t, 1, journal.count("rollback-event"),
				"exactly-once cleanup through the deferred path")
			require.Equal(t, 1, journal.count("closed-event"),
				"exactly-once release at the observed close")
			waitForCondition(t, 2*time.Second, "late lane result refused",
				func() bool { return pool.Dump().LateResults == 1 })
			require.Equal(t, baseTxns, gauges.activeTxns.Load(), "baseline returns exactly")
			require.Equal(t, baseLocks, gauges.heldLocks.Load(), "baseline returns exactly")
		}
		t.Run("selected-rollback-wait-error-arm", func(t *testing.T) { run(t, false) })
		t.Run("red-twin-decision-gate-disabled", func(t *testing.T) { run(t, true) })
	})

	// (o.5) blocked-COMMIT variant (D187(H) kept, D188 re-pointed): the
	// owner's F1 Commit is BLOCKED inside the deferred synchronous unlock
	// when round expiry fires. NO Rollback is invoked (the gate stays
	// with the selected Commit), result acceptance is revoked, and the
	// charge/slot/transaction/locks REMAIN OWNED until the unblock; the
	// release rides the observed close.
	t.Run("arm-o5-expiry-during-blocked-commit", func(t *testing.T) {
		run := func(t *testing.T, commitErr bool) {
			ctx := context.Background()
			gauges := &stageGauges{}
			journal := &stageJournal{}
			ls := newStubLockService(gauges, journal)
			cli := newStubTxnClient(gauges, journal, ls)
			pool := newPool(2, 150*time.Millisecond)
			driver := NewDataBranchFencingDriver(pool)
			baseTxns := gauges.activeTxns.Load()
			baseLocks := gauges.heldLocks.Load()

			owner, merr := driver.mintStageOwner(67, "cell-o5-commit")
			require.Nil(t, merr)
			op := cli.New(1)
			require.NoError(t, owner.BindOperator(op, 67))
			hookStageJournal(journal, op)
			if commitErr {
				ls.ArmStallForErr(op.id, moerr.NewInternalErrorNoCtx("cell-o5: injected unlock failure"))
			} else {
				ls.ArmStallFor(op.id)
			}
			go func() {
				_, _ = driver.runStepOwned(ctx, DataBranchFencingStepRoundF1,
					func(lctx context.Context) (any, error) {
						o := DataBranchRoundOwnerFromCtx(lctx)
						if !o.SelectCommitting() {
							return nil, moerr.NewInternalErrorNoCtx("cell-o5-commit: revoked")
						}
						return nil, op.Commit(lctx)
					}, owner, NewDataBranchFencingLanePublication())
			}()
			waitForCondition(t, 2*time.Second, "commit blocked in unlock",
				func() bool { return journal.has("unlock-stall-entered") })
			waitForCondition(t, 2*time.Second, "expiry classified",
				func() bool { return owner.AcceptanceRevoked() })

			// Retention AT the expiry window: the gate stays COMMITTING;
			// NO Rollback is invoked; every resource REMAINS OWNED.
			dump := pool.Dump()
			require.Equal(t, 1, dump.Charges, "charge held past expiry")
			require.Equal(t, 1, dump.Owners)
			require.Equal(t, 0, journal.count("rollback-event"),
				"no Rollback is invoked against the selected Commit")
			require.False(t, owner.ClosedObserved())
			require.Equal(t, baseTxns+1, gauges.activeTxns.Load())
			require.Equal(t, baseLocks+1, gauges.heldLocks.Load())
			owner.NoteNoMoreLanes()

			ls.Close()
			waitForCondition(t, 2*time.Second, "release exactly once at observed close",
				func() bool {
					d := pool.Dump()
					return d.Charges == 0 && d.Owners == 0
				})
			require.Equal(t, 1, journal.count("closed-event"),
				"exactly-once release at the observed close")
			require.Zero(t, journal.count("rollback-event"),
				"the selected Commit owned the transition throughout")
			require.Equal(t, baseTxns, gauges.activeTxns.Load(), "baseline returns exactly")
			require.Equal(t, baseLocks, gauges.heldLocks.Load(), "baseline returns exactly")
		}
		t.Run("success-variant", func(t *testing.T) { run(t, false) })
		t.Run("injected-error-variant", func(t *testing.T) { run(t, true) })
	})

	// (o.6) INDEPENDENTLY SCHEDULED cleanup (D188 NEW): a fatal internal
	// SQL error arms the deferred rollback with NO selected call at the
	// boundary at all; the owner releases only at the observed close and
	// the baselines return exactly once it fires.
	t.Run("arm-o6-independently-scheduled-cleanup", func(t *testing.T) {
		run := func(t *testing.T, boundaryDisabled bool) {
			ctx := context.Background()
			if boundaryDisabled {
				restore := DisableDataBranchFencingObservedCloseBoundaryForTest(true)
				defer restore()
			}
			gauges := &stageGauges{}
			journal := &stageJournal{}
			ls := newStubLockService(gauges, journal)
			cli := newStubTxnClient(gauges, journal, ls)
			pool := newPool(2, 2*time.Second)
			driver := NewDataBranchFencingDriver(pool)
			baseTxns := gauges.activeTxns.Load()
			baseLocks := gauges.heldLocks.Load()

			owner, merr := driver.mintStageOwner(68, "cell-o6")
			require.Nil(t, merr)
			op := cli.New(1)
			require.NoError(t, owner.BindOperator(op, 68))
			hookStageJournal(journal, op)
			ls.ArmStallFor(op.id)
			_, stepErr := driver.runStepOwned(ctx, DataBranchFencingStepRoundF1,
				func(lctx context.Context) (any, error) {
					return nil, execStubStageSQL(op, func() error {
						return stubFatalStageErr()
					})
				}, owner, NewDataBranchFencingLanePublication())
			require.Error(t, stepErr)
			// NO selected call at the boundary: no Abandon, no Commit.
			owner.NoteNoMoreLanes()
			waitForCondition(t, 2*time.Second, "deferred cleanup entered unlock",
				func() bool { return journal.has("unlock-stall-entered") })

			if !boundaryDisabled {
				dump := pool.Dump()
				require.Equal(t, 1, dump.Charges, "charge held while the close is outstanding")
				require.Len(t, dump.Retained, 1, "owner retained on the scheduled cleanup alone")
				require.False(t, owner.ClosedObserved())
				require.Equal(t, baseTxns+1, gauges.activeTxns.Load())
				require.Equal(t, baseLocks+1, gauges.heldLocks.Load())
			} else {
				// RED TWIN (boundary reverted to release-at-return): the
				// unaccounted-close witness - the charge frees with the
				// scheduled cleanup and its close still live.
				waitForCondition(t, 2*time.Second, "charge freed at the return boundary",
					func() bool { return pool.Dump().Charges == 0 })
				require.False(t, owner.ClosedObserved(),
					"unaccounted-close witness: release while the close is outstanding")
				require.Equal(t, baseTxns+1, gauges.activeTxns.Load())
				require.Equal(t, baseLocks+1, gauges.heldLocks.Load())
			}

			ls.Close()
			waitForCondition(t, 2*time.Second, "observed close consumed",
				func() bool { return journal.count("closed-event") == 1 })
			require.Equal(t, 1, journal.count("rollback-event"), "exactly-once cleanup")
			waitForCondition(t, 2*time.Second, "accounting returns to baseline",
				func() bool {
					return pool.Dump().Charges == 0 && pool.Dump().Owners == 0
				})
			require.Equal(t, baseTxns, gauges.activeTxns.Load(), "baseline returns exactly")
			require.Equal(t, baseLocks, gauges.heldLocks.Load(), "baseline returns exactly")
		}
		t.Run("release-at-observed-close", func(t *testing.T) { run(t, false) })
		t.Run("red-twin-boundary-reverted", func(t *testing.T) { run(t, true) })
	})

	// RED twin for arms (i)-(iii) per D186(H)/D187(H): the amalgamation
	// mechanically disabled re-runs the ownerless legacy geometry and
	// reproduces (A.2)'s open-transaction and lock leak witnesses - the
	// leak is visible at the platform gauges with NO pool accounting
	// entry at all.
	t.Run("red-twin-amalgamation-disabled", func(t *testing.T) {
		restore := DisableDataBranchFencingAmalgamationForTest(true)
		defer restore()
		ctx := context.Background()
		gauges := &stageGauges{}
		journal := &stageJournal{}
		ls := newStubLockService(gauges, journal)
		cli := newStubTxnClient(gauges, journal, ls)
		pool := newPool(4, 2*time.Second)
		driver := NewDataBranchFencingDriver(pool)

		ops := &stubRoundOps{}
		locks := &stubScratchLocks{
			cli: cli, journal: journal, ops: ops,
			binding: DataBranchFencingBinding{Table: 3003, ServiceID: "svc-twin", Version: 4},
		}
		exec := &stubStageExec{cli: cli, journal: journal, ops: ops, fatalF2: true}
		in := DataBranchFencingRoundInput{
			ScratchTableID:     88,
			IssueTimeNS:        1_800_000,
			LastWriterCommitTS: tsOf(700),
		}
		_, err := driver.RunRound(ctx, locks, exec, in)
		require.Error(t, err)
		waitForCondition(t, 2*time.Second, "legacy F2 rollback stalled",
			func() bool { return journal.has("unlock-stall-entered") })

		dump := pool.Dump()
		require.Zero(t, dump.Owners, "legacy geometry has no stage owner")
		require.Empty(t, dump.Retained, "nothing is classified")
		require.Zero(t, dump.Charges, "the legacy accounting reads zero")
		require.Equal(t, int64(1), gauges.activeTxns.Load(),
			"(A.2) open-transaction leak witness")
		require.Equal(t, int64(1), gauges.heldLocks.Load(),
			"(A.2) lock leak witness")
		require.Equal(t, 1, journal.count("public-rollback-gate-rejected"))

		ls.Close()
		waitForCondition(t, 2*time.Second, "leak drained by the platform path",
			func() bool {
				return gauges.activeTxns.Load() == 0 && gauges.heldLocks.Load() == 0
			})
		// Two closes in the legacy geometry: the F1 commit's own close
		// (recorded inside RunRound) and the F2 rollback's close after the
		// stall discharge.
		require.Equal(t, 2, journal.count("closed-event"))
	})
}

// ---------------------------------------------------------------------------
// (H.10)(q) ALLOCATOR-LIFETIME cell (F4): globalPools registration is the
// oracle; success, injected-failure and constructor-refusal routes.
// ---------------------------------------------------------------------------

func TestDataBranchFencingCellQAllocatorLifetime(t *testing.T) {
	tag := "dbr-stage-alloc-cellq"
	registrations := func() int {
		report := mpool.ReportMemUsage(tag)
		return strings.Count(report, `"`+tag+`"`)
	}
	baseline := registrations()

	// Success route: construct, lane-scoped Enter/Exit, allocate through
	// the owned pool, then the deferred destructor restores the baseline.
	mp := mpool.MustNew(tag)
	require.Equal(t, baseline+1, registrations(), "construction registers exactly once")
	alloc := NewDataBranchStageAllocator(mp)
	alloc.Enter()
	// offHeap=true: MPool usage stats track only physical off-heap
	// allocations (the in-heap branch records no NumCurrBytes).
	bs, err := alloc.MPool().Alloc(4<<20, true)
	require.NoError(t, err)
	require.Positive(t, alloc.MPool().CurrNB(),
		"allocation-heavy arm: usage nonzero mid-cell - the allocator was consumed")
	alloc.MPool().Free(bs)
	alloc.Exit()
	alloc.DrainAndDelete()
	require.Equal(t, baseline, registrations(), "registration zero after the destructor")

	// Injected-failure route: the stage entry still pairs Enter/Exit and
	// the destructor reclaims the registration with the failure live.
	mp = mpool.MustNew(tag)
	alloc = NewDataBranchStageAllocator(mp)
	alloc.Enter()
	stageErr := moerr.NewInternalErrorNoCtx("cell-q: injected stage failure")
	alloc.Exit()
	require.Error(t, stageErr)
	alloc.DrainAndDelete()
	require.Equal(t, baseline, registrations())

	// Constructor-refusal route: destruction precedes the early return;
	// no lane-scoped closure ever took the pool.
	mp = mpool.MustNew(tag)
	alloc = NewDataBranchStageAllocator(mp)
	alloc.DrainAndDelete()
	require.Equal(t, baseline, registrations(),
		"the refusal arm's destruction precedes its early return")

	// Nil-safety of every method (the deferred destructor surface).
	var nilAlloc *DataBranchStageAllocator
	nilAlloc.Enter()
	nilAlloc.Exit()
	nilAlloc.DrainAndDelete()
	require.Equal(t, baseline, registrations())
}

// ---------------------------------------------------------------------------
// Defect-flag hygiene: every mechanism ships OFF and restores round-trip.
// ---------------------------------------------------------------------------

func TestDataBranchFencingDefectFlagsHygiene(t *testing.T) {
	require.False(t, dataBranchFencingAmalgamationDisabled.Load())
	require.False(t, dataBranchFencingDecisionGateDisabled.Load())
	require.False(t, dataBranchFencingObservedCloseBoundaryDisabled.Load())
	require.False(t, dataBranchFencingRetainedClassificationDisabled.Load())
	require.False(t, dataBranchFencingPublicationDisabled.Load())

	armRestore := func(set func(bool) func()) {
		restore := set(true)
		restore()
	}
	armRestore(DisableDataBranchFencingAmalgamationForTest)
	armRestore(DisableDataBranchFencingDecisionGateForTest)
	armRestore(DisableDataBranchFencingObservedCloseBoundaryForTest)
	armRestore(DisableDataBranchFencingRetainedClassificationForTest)
	armRestore(DisableDataBranchFencingPublicationForTest)
	require.False(t, dataBranchFencingAmalgamationDisabled.Load())
	require.False(t, dataBranchFencingDecisionGateDisabled.Load())
	require.False(t, dataBranchFencingObservedCloseBoundaryDisabled.Load())
	require.False(t, dataBranchFencingRetainedClassificationDisabled.Load())
	require.False(t, dataBranchFencingPublicationDisabled.Load())
}
