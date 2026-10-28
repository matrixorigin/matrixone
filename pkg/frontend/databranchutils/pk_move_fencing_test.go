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

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// Fabric-level tests for the fencing witness protocol and its driver: every
// lane/lock/commit interface is faked in memory — no live server, no sleeps
// beyond millisecond-range assertion windows, every wait is channel- or
// barrier-driven.

// fakeScratchLocks delivers a fixed binding/witness grant. When a stage
// client is wired the grant also creates and binds the shared stage
// operator (the cs.go grant-step mirror), so the round's owner lifecycle
// completes through observed closes.
type fakeScratchLocks struct {
	binding   DataBranchFencingBinding
	witnessTS timestamp.Timestamp
	err       error

	cli     *stubTxnClient
	journal *stageJournal
	ops     *stubRoundOps

	mu       sync.Mutex
	acquires int
}

func (f *fakeScratchLocks) AcquireScratchGrant(
	ctx context.Context, in DataBranchFencingRoundInput,
) (DataBranchScratchGrant, error) {
	f.mu.Lock()
	f.acquires++
	f.mu.Unlock()
	if f.err != nil {
		return DataBranchScratchGrant{}, f.err
	}
	if f.cli != nil {
		if owner := DataBranchRoundOwnerFromCtx(ctx); owner != nil {
			op := f.cli.New(0)
			hookStageJournal(f.journal, op)
			f.ops.mu.Lock()
			f.ops.grant = op
			f.ops.mu.Unlock()
			if err := owner.BindOperator(op, in.ScratchTableID); err != nil {
				op.closeWithCommitErr(err)
				return DataBranchScratchGrant{}, err
			}
		}
	}
	return DataBranchScratchGrant{Binding: f.binding, WitnessTS: f.witnessTS}, nil
}

func (f *fakeScratchLocks) grantCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acquires
}

// fakeFencingSession fakes the two short committed transactions: a strictly
// monotonic per-TN commit allocator, a durable scratch-row store, per-candidate
// sweep locks with skip-on-timeout, and the ownership-precondition probe.
type fakeFencingSession struct {
	mu sync.Mutex
	// clock is the strictly monotonic commit allocator (one shared TN).
	clock int64
	// rows is the durable kind=1 scratch-row store: id -> created marker.
	rows map[uint64]uint64
	// candidates are the aged rows the sweep considers.
	candidates []uint64
	// skipLock simulates candidates whose witnessed lock-wait times out.
	skipLock map[uint64]bool

	// cli/journal/ops, when wired, route the stage transactions through
	// the shared stub-operator family (the cs.go owned-geometry mirror):
	// F1 commits the grant's bound operator; F2 creates and binds its
	// own; every terminal close publishes into the owner lifecycle.
	cli     *stubTxnClient
	journal *stageJournal
	ops     *stubRoundOps

	f1Err      error
	f2Err      error
	probeSteal bool // an external party removes the row before the F2 probe

	f1Commits int
	f2Commits int
	probes    int
}

// withStageOps wires the stage-operator family for the owned geometry.
func (s *fakeFencingSession) withStageOps(
	cli *stubTxnClient, journal *stageJournal, ops *stubRoundOps,
) *fakeFencingSession {
	s.cli = cli
	s.journal = journal
	s.ops = ops
	return s
}

func newFakeFencingSession(lastWriterPhysical int64) *fakeFencingSession {
	return &fakeFencingSession{
		clock:    lastWriterPhysical,
		rows:     make(map[uint64]uint64),
		skipLock: make(map[uint64]bool),
	}
}

func (s *fakeFencingSession) commitTS() timestamp.Timestamp {
	s.clock += 100
	return timestamp.Timestamp{PhysicalTime: s.clock}
}

func (s *fakeFencingSession) CommitRoundF1(
	ctx context.Context, in DataBranchFencingRoundInput,
) (DataBranchFencingF1Commit, error) {
	s.mu.Lock()
	if s.f1Err != nil {
		s.mu.Unlock()
		return DataBranchFencingF1Commit{}, s.f1Err
	}
	var swept, skipped []uint64
	for _, cand := range s.candidates {
		if cand == in.ScratchTableID {
			// A live round's own row is unreachable to the sweep (kind=1
			// namespace + the round is inside its deadline).
			continue
		}
		if s.skipLock[cand] {
			// Bounded lock-wait timeout: SKIP for a later sweep.
			skipped = append(skipped, cand)
			continue
		}
		// Witnessed grant: presence under the grant is final.
		delete(s.rows, cand)
		swept = append(swept, cand)
	}
	s.rows[in.ScratchTableID] = in.IssueTimeNS
	s.f1Commits++
	commitTS := s.commitTS()
	s.mu.Unlock()

	if s.cli != nil {
		if owner := DataBranchRoundOwnerFromCtx(ctx); owner != nil {
			op := owner.BoundOperator()
			if op == nil {
				return DataBranchFencingF1Commit{}, moerr.NewInternalErrorNoCtx(
					"fake fencing round f1: scratch grant missing")
			}
			if pub := DataBranchLanePublicationFromCtx(ctx); pub != nil {
				pub.PublishCertificate(DataBranchFencingCertificate{
					CheckAEnvelopeDataObserved: true,
					CheckBEnvelopeDataObserved: true,
				})
				pub.PublishTerminalEntry()
			}
			if !owner.SelectCommitting() {
				return DataBranchFencingF1Commit{}, moerr.NewInternalErrorNoCtx(
					"fake fencing round f1: terminal decision revoked")
			}
			// NO in-body abandonment on a commit error (D187(B.3) mirror).
			stub, _ := op.(*stageOperatorStub)
			if stub != nil {
				if err := stub.Commit(ctx); err != nil {
					return DataBranchFencingF1Commit{}, err
				}
			}
		}
	}
	return DataBranchFencingF1Commit{
		CommitTS: commitTS,
		Swept:    swept,
		Skipped:  skipped,
		Shape: DataBranchFencingStageShape{
			TargetLedgerTable: true,
			InsertBatches:     1,
			SweepTombstones:   len(swept),
		},
	}, nil
}

func (s *fakeFencingSession) CommitRoundF2(
	ctx context.Context, in DataBranchFencingRoundInput,
) (DataBranchFencingF2Commit, error) {
	owner := DataBranchRoundOwnerFromCtx(ctx)
	var stub *stageOperatorStub
	if s.cli != nil && owner != nil {
		stub = s.cli.New(0)
		hookStageJournal(s.journal, stub)
		s.ops.mu.Lock()
		s.ops.f2 = stub
		s.ops.mu.Unlock()
		if err := owner.BindOperator(stub, in.ScratchTableID); err != nil {
			stub.closeWithCommitErr(err)
			return DataBranchFencingF2Commit{}, err
		}
	}

	s.mu.Lock()
	if s.f2Err != nil {
		s.mu.Unlock()
		// Commit issued and errored (the TxnUnknown family): the platform
		// closes with the commit error; the observed close releases.
		if stub != nil {
			stub.closeWithCommitErr(s.f2Err)
		}
		return DataBranchFencingF2Commit{}, s.f2Err
	}
	if s.probeSteal {
		delete(s.rows, in.ScratchTableID)
	}
	s.probes++
	if _, present := s.rows[in.ScratchTableID]; !present {
		s.mu.Unlock()
		// Ownership precondition failed: NO commit is issued; the lane
		// itself abandons the owner before returning ProbeFound=false.
		if stub != nil {
			owner.Abandon(ctx)
		}
		return DataBranchFencingF2Commit{ProbeFound: false}, nil
	}
	delete(s.rows, in.ScratchTableID)
	s.f2Commits++
	commitTS := s.commitTS()
	s.mu.Unlock()

	if stub != nil {
		if pub := DataBranchLanePublicationFromCtx(ctx); pub != nil {
			pub.PublishCertificate(DataBranchFencingCertificate{
				CheckAEnvelopeDataObserved: true,
				CheckBEnvelopeDataObserved: true,
			})
			pub.PublishTerminalEntry()
		}
		if !owner.SelectCommitting() {
			return DataBranchFencingF2Commit{}, moerr.NewInternalErrorNoCtx(
				"fake fencing round f2: terminal decision revoked")
		}
		if err := stub.Commit(ctx); err != nil {
			return DataBranchFencingF2Commit{}, err
		}
	}
	return DataBranchFencingF2Commit{
		ProbeFound: true,
		CommitTS:   commitTS,
		Shape: DataBranchFencingStageShape{
			TargetLedgerTable: true,
			PointTombstones:   1,
		},
	}, nil
}

func tsOf(physical int64) timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: physical}
}

// (a) Happy round: C_f > C_f1 > C, B_f recorded, witness sweep semantics
// (grant -> swept, timeout -> skipped), zero scratch residue after the pair.
func TestDataBranchFencingRoundCommitsOrdered(t *testing.T) {
	ctx := context.Background()
	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{LanePoolCapacity: 4})
	driver := NewDataBranchFencingDriver(pool)

	binding := DataBranchFencingBinding{Table: 1001, ServiceID: "svc-a", Version: 7}
	gauges := &stageGauges{}
	journal := &stageJournal{}
	ls := newStubLockService(gauges, journal)
	cli := newStubTxnClient(gauges, journal, ls)
	ops := &stubRoundOps{}
	locks := &fakeScratchLocks{
		binding: binding, witnessTS: tsOf(500),
		cli: cli, journal: journal, ops: ops,
	}
	exec := newFakeFencingSession(700).withStageOps(cli, journal, ops)
	exec.candidates = []uint64{11, 22}
	exec.rows[11] = 1
	exec.rows[22] = 1
	exec.skipLock[22] = true

	in := DataBranchFencingRoundInput{
		ScratchTableID:     42,
		IssueTimeNS:        1_700_000,
		LastWriterCommitTS: tsOf(700),
	}
	rec, err := driver.RunRound(ctx, locks, exec, in)
	require.NoError(t, err)
	require.True(t, in.LastWriterCommitTS.Less(rec.CommitTS1),
		"C_f1 %s must out-order C %s", rec.CommitTS1.DebugString(), in.LastWriterCommitTS.DebugString())
	require.True(t, rec.CommitTS1.Less(rec.CommitTS),
		"C_f %s must out-order C_f1 %s", rec.CommitTS.DebugString(), rec.CommitTS1.DebugString())
	require.Equal(t, binding, rec.Binding)
	require.Equal(t, 1, locks.grantCount())
	require.Equal(t, []uint64{11}, rec.Swept)
	require.Equal(t, []uint64{22}, rec.Skipped)
	require.Equal(t, 1, exec.f1Commits)
	require.Equal(t, 1, exec.f2Commits)
	require.Equal(t, 1, exec.probes)

	// Happy-path zero residue: the pair's delete removed the scratch row.
	exec.mu.Lock()
	_, residue := exec.rows[42]
	exec.mu.Unlock()
	require.False(t, residue)

	// The retirement accepts its own proven record; equality on fence is accept.
	require.True(t, AcceptDataBranchFencingRecord(rec, binding, rec.CommitTS))
	require.True(t, AcceptDataBranchFencingRecord(rec, binding, tsOf(950)))

	// Every round lane joined; both stage operators closed and each owner
	// released at its observed close: charges drain to zero.
	waitForCondition(t, 2*time.Second, "pool charges drained",
		func() bool { return pool.Dump().Charges == 0 })
	require.True(t, journal.has("closed-event"))
	driver.Retire()
	require.NoError(t, driver.DrainLedger(ctx))
}

// (b) T_f2's ownership precondition finds the row absent: the round is
// interrupted and NO commit is touched.
func TestDataBranchFencingRoundInterruptedWithoutCommit(t *testing.T) {
	ctx := context.Background()
	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{LanePoolCapacity: 2})
	driver := NewDataBranchFencingDriver(pool)
	gauges := &stageGauges{}
	journal := &stageJournal{}
	ls := newStubLockService(gauges, journal)
	cli := newStubTxnClient(gauges, journal, ls)
	ops := &stubRoundOps{}
	locks := &fakeScratchLocks{
		binding: DataBranchFencingBinding{Table: 1001, ServiceID: "svc-a", Version: 7},
		cli:     cli, journal: journal, ops: ops,
	}
	exec := newFakeFencingSession(700).withStageOps(cli, journal, ops)
	exec.probeSteal = true

	_, err := driver.RunRound(ctx, locks, exec, DataBranchFencingRoundInput{
		ScratchTableID:     77,
		IssueTimeNS:        1_700_000,
		LastWriterCommitTS: tsOf(700),
	})
	require.Error(t, err)
	fe, ok := AsDataBranchFencingError(err)
	require.True(t, ok)
	require.Equal(t, DataBranchFencingRoundInterrupted, fe.Class)
	require.Equal(t, 1, exec.f1Commits)
	require.Equal(t, 1, exec.probes)
	require.Equal(t, 0, exec.f2Commits, "round-interrupted issues no empty commit")

	// The classification is typed; surfaced statement errors stay internal
	// class, never a transaction-identity code.
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrTxnUnknown))
	stmtErr := fe.StatementError(ctx)
	require.True(t, moerr.IsMoErrCode(stmtErr, moerr.ErrInternal))
	require.False(t, moerr.MustRollbackTxn(stmtErr))
	waitForCondition(t, 2*time.Second, "pool charges drained",
		func() bool { return pool.Dump().Charges == 0 })
}

// ErrTxnUnknown is consumed by the classification layer and surfaces only as
// the determinate fencing-unavailable internal error.
func TestDataBranchFencingDriverConsumesTxnUnknown(t *testing.T) {
	ctx := context.Background()
	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{LanePoolCapacity: 2})
	driver := NewDataBranchFencingDriver(pool)
	gauges := &stageGauges{}
	journal := &stageJournal{}
	ls := newStubLockService(gauges, journal)
	cli := newStubTxnClient(gauges, journal, ls)
	ops := &stubRoundOps{}
	locks := &fakeScratchLocks{
		binding: DataBranchFencingBinding{Table: 1001, ServiceID: "svc-a", Version: 7},
		cli:     cli, journal: journal, ops: ops,
	}
	exec := newFakeFencingSession(700).withStageOps(cli, journal, ops)
	exec.f2Err = moerr.NewTxnUnknown(ctx, "fake-helper-txn")

	_, err := driver.RunRound(ctx, locks, exec, DataBranchFencingRoundInput{
		ScratchTableID:     88,
		IssueTimeNS:        1_700_000,
		LastWriterCommitTS: tsOf(700),
	})
	require.Error(t, err)
	fe, ok := AsDataBranchFencingError(err)
	require.True(t, ok)
	require.Equal(t, DataBranchFencingRoundCommitUnknown, fe.Class)

	// Never the frontend/txn.go unknown-commit path, never whole-txn rollback.
	stmtErr := fe.StatementError(ctx)
	require.True(t, moerr.IsMoErrCode(stmtErr, moerr.ErrInternal))
	require.False(t, moerr.IsMoErrCode(stmtErr, moerr.ErrTxnUnknown))
	require.False(t, moerr.MustRollbackTxn(stmtErr))
	waitForCondition(t, 2*time.Second, "pool charges drained",
		func() bool { return pool.Dump().Charges == 0 })
}

// (c) SATURATED lanes fail loud; reserve-before-spawn proven by counters.
func TestDataBranchFencingPoolSaturatedFailsLoud(t *testing.T) {
	ctx := context.Background()
	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{
		LanePoolCapacity: 3,
		RoundTimeout:     30 * time.Second,
	})
	driver := NewDataBranchFencingDriver(pool)

	started := make(chan int, 3)
	release := make(chan struct{})
	results := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			_, err := driver.RunStep(ctx, DataBranchFencingStepRoundF1, func(lctx context.Context) (any, error) {
				started <- i
				<-release
				return i, nil
			})
			results <- err
		}(i)
	}
	for i := 0; i < 3; i++ {
		<-started
	}
	dump := pool.Dump()
	require.Equal(t, 3, dump.Charges)
	require.Equal(t, uint64(3), dump.Spawned)

	// Fourth statement: fail-loud SATURATED without spawning anything.
	_, err := driver.RunStep(ctx, DataBranchFencingStepRoundF1, func(lctx context.Context) (any, error) {
		return nil, fmt.Errorf("lane must never be spawned on saturation")
	})
	require.Error(t, err)
	fe, ok := AsDataBranchFencingError(err)
	require.True(t, ok)
	require.Equal(t, DataBranchFencingRoundSaturated, fe.Class)
	dump = pool.Dump()
	require.Equal(t, uint64(4), dump.ReserveAttempts)
	require.Equal(t, uint64(3), dump.Spawned, "reserve-before-spawn: no spawn without a charge")
	require.Equal(t, 3, dump.Charges)

	close(release)
	for i := 0; i < 3; i++ {
		require.NoError(t, <-results)
	}
	require.Equal(t, 0, pool.Dump().Charges)

	// Recovery is ordinary once charges drain.
	_, err = driver.RunStep(ctx, DataBranchFencingStepRoundF1, func(lctx context.Context) (any, error) {
		return "ok", nil
	})
	require.NoError(t, err)
	driver.Retire()
}

// (d) CN-close lifecycle: CloseStep seals, detaches a terminal-in-flight
// lane without joining it; the FINAL DRAIN BARRIER blocks until
// outstanding lanes join to full exit and the retained registry is
// verified empty; the completion ledger joins exactly once.
func TestDataBranchFencingDrainBarrierJoinsOutstandingLanes(t *testing.T) {
	ctx := context.Background()
	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{
		LanePoolCapacity: 4,
		DrainStepTimeout: 200 * time.Millisecond,
		BarrierTimeout:   time.Second,
	})

	// Terminal-in-flight owned lane (D186(B.1)(ii)): the lane published
	// its certified certificate and entered its terminal segment, then
	// blocks INSIDE the terminal call after observing the drain cancel —
	// the commit-RPC-blocked tail the close step detaches without join
	// and the final barrier joins to full exit.
	owner, class := pool.MintStageOwner(77, 1, "t-in-flight")
	require.Equal(t, DataBranchFencingRoundCommitted, class)
	pub := NewDataBranchFencingLanePublication()
	unlockGate := make(chan struct{})
	lane, class := pool.spawnOwned(DataBranchFencingStepRoundF1, func(lctx context.Context) (any, error) {
		pub.PublishCertificate(DataBranchFencingCertificate{
			CheckAEnvelopeDataObserved: true,
			CheckBEnvelopeDataObserved: true,
		})
		pub.PublishTerminalEntry()
		<-lctx.Done()
		<-unlockGate
		return nil, lctx.Err()
	}, ctx, owner, pub, nil)
	require.Equal(t, DataBranchFencingRoundCommitted, class)

	require.NoError(t, pool.CloseStep(ctx))
	phase := pool.Dump().Phases[lane.id]
	require.Equal(t, DataBranchFencingLaneTerminalInFlight, phase)
	require.True(t, pool.Dump().Sealed)
	// Sealed admission refuses a new reserve.
	_, reason := pool.spawn(DataBranchFencingStepRoundF1, func(lctx context.Context) (any, error) {
		return nil, nil
	}, ctx)
	require.Equal(t, DataBranchFencingRoundPoolSealed, reason)

	// The barrier must block until the lane's full exit.
	barrierErr := make(chan error, 1)
	go func() { barrierErr <- pool.DrainBarrier(ctx) }()
	select {
	case err := <-barrierErr:
		t.Fatalf("drain barrier returned before lane joined: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(unlockGate)
	// Complete the owner: no operator ever bound, so the abandonment is
	// the vacuous close and the owner finishes exactly once.
	owner.Abandon(context.Background())
	owner.NoteNoMoreLanes()
	require.NoError(t, <-barrierErr)
	dump := pool.Dump()
	require.Zero(t, dump.Charges)
	require.Empty(t, dump.Phases)
	require.Empty(t, dump.Retained)

	// The per-statement completion ledger joins exactly once.
	pool2 := NewDataBranchFencingPool(DataBranchFencingDriverConfig{LanePoolCapacity: 2})
	driver2 := NewDataBranchFencingDriver(pool2)
	lane2, _ := pool2.spawn(DataBranchFencingStepGrant, func(lctx context.Context) (any, error) {
		return "done", nil
	}, ctx)
	driver2.ledger.record(lane2)
	require.NoError(t, driver2.ledger.join(lane2))
	err := driver2.ledger.join(lane2)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "duplicate completion"))
	// Idempotent retire bookkeeping: second joinAllCompleted is a no-op.
	driver2.ledger.joinAllCompleted()
	driver2.ledger.joinAllCompleted()
	require.NoError(t, driver2.DrainLedger(ctx))
	require.Zero(t, pool2.Dump().Charges)
}

// CloseStep on a lane that NEVER observes its cancellation fails the step
// loud, naming lane, phase and budget, and never reports a false zero.
func TestDataBranchFencingCloseStepFailsLoudOnPreSafeLane(t *testing.T) {
	ctx := context.Background()
	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{
		LanePoolCapacity: 2,
		DrainStepTimeout: 10 * time.Millisecond,
		BarrierTimeout:   time.Second,
	})
	release := make(chan struct{})
	lane, _ := pool.spawn(DataBranchFencingStepRoundF1, func(lctx context.Context) (any, error) {
		<-release
		return nil, nil
	}, ctx)

	err := pool.CloseStep(ctx)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), fmt.Sprintf("lane %d", lane.id)))
	require.True(t, strings.Contains(err.Error(), "running"))
	// Honest non-zero accounting is retained across the failed step.
	require.Equal(t, 1, pool.Dump().Charges)

	close(release)
	require.NoError(t, pool.DrainBarrier(ctx))
	require.Zero(t, pool.Dump().Charges)
}

// (e) The D182 micro-shape gate plus record assembly reject malformed
// rounds: empty binding, empty/zero commit timestamps, unordered pairs,
// content outside the kind=1 stage shape, no-write commits.
func TestDataBranchFencingMicroShapeGate(t *testing.T) {
	binding := DataBranchFencingBinding{Table: 1001, ServiceID: "svc-a", Version: 7}
	grant := DataBranchScratchGrant{Binding: binding, WitnessTS: tsOf(500)}
	base := DataBranchFencingRoundInput{ScratchTableID: 9, IssueTimeNS: 1_700_000, LastWriterCommitTS: tsOf(700)}
	f1 := DataBranchFencingF1Commit{
		CommitTS: tsOf(800),
		Shape:    DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1},
	}
	f2 := DataBranchFencingF2Commit{
		ProbeFound: true,
		CommitTS:   tsOf(900),
		Shape:      DataBranchFencingStageShape{TargetLedgerTable: true, PointTombstones: 1},
	}

	// Valid baseline assembles and passes both stage gates.
	rec, err := AssembleDataBranchFencingRecord(grant, f1, f2, base)
	require.NoError(t, err)
	require.Equal(t, tsOf(900), rec.CommitTS)
	require.NoError(t, CheckDataBranchFencingStageShape(DataBranchFencingStepRoundF1, f1.Shape))
	require.NoError(t, CheckDataBranchFencingStageShape(DataBranchFencingStepRoundF2, f2.Shape))

	assemblyCases := []struct {
		name   string
		mutate func(g *DataBranchScratchGrant, a *DataBranchFencingF1Commit, c *DataBranchFencingF2Commit, in *DataBranchFencingRoundInput)
	}{
		{"empty binding service id", func(g *DataBranchScratchGrant, a *DataBranchFencingF1Commit, c *DataBranchFencingF2Commit, in *DataBranchFencingRoundInput) {
			g.Binding.ServiceID = ""
		}},
		{"zero binding table", func(g *DataBranchScratchGrant, a *DataBranchFencingF1Commit, c *DataBranchFencingF2Commit, in *DataBranchFencingRoundInput) {
			g.Binding.Table = 0
		}},
		{"empty C_f1", func(g *DataBranchScratchGrant, a *DataBranchFencingF1Commit, c *DataBranchFencingF2Commit, in *DataBranchFencingRoundInput) {
			a.CommitTS = timestamp.Timestamp{}
		}},
		{"empty C_f", func(g *DataBranchScratchGrant, a *DataBranchFencingF1Commit, c *DataBranchFencingF2Commit, in *DataBranchFencingRoundInput) {
			c.CommitTS = timestamp.Timestamp{}
		}},
		{"unordered pair", func(g *DataBranchScratchGrant, a *DataBranchFencingF1Commit, c *DataBranchFencingF2Commit, in *DataBranchFencingRoundInput) {
			a.CommitTS = tsOf(950)
		}},
		{"pair not out-ordering writer", func(g *DataBranchScratchGrant, a *DataBranchFencingF1Commit, c *DataBranchFencingF2Commit, in *DataBranchFencingRoundInput) {
			in.LastWriterCommitTS = tsOf(850)
		}},
	}
	for _, tc := range assemblyCases {
		t.Run(tc.name, func(t *testing.T) {
			g, a, b, in := grant, f1, f2, base
			tc.mutate(&g, &a, &b, &in)
			_, err := AssembleDataBranchFencingRecord(g, a, b, in)
			require.Error(t, err)
		})
	}

	shapeCases := []struct {
		name  string
		step  DataBranchFencingStepKind
		shape DataBranchFencingStageShape
	}{
		{"f1 missing insert", DataBranchFencingStepRoundF1, DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 0}},
		{"f1 extra write", DataBranchFencingStepRoundF1, DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1, ExtraWrites: 1}},
		{"f1 off-table target", DataBranchFencingStepRoundF1, DataBranchFencingStageShape{TargetLedgerTable: false, InsertBatches: 1}},
		{"f1 no-write fast path", DataBranchFencingStepRoundF1, DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1, NoWriteFastPath: true}},
		{"f2 missing tombstone", DataBranchFencingStepRoundF2, DataBranchFencingStageShape{TargetLedgerTable: true, PointTombstones: 0}},
		{"f2 extra sweep tombstone", DataBranchFencingStepRoundF2, DataBranchFencingStageShape{TargetLedgerTable: true, PointTombstones: 1, SweepTombstones: 1}},
		{"f2 no-write fast path", DataBranchFencingStepRoundF2, DataBranchFencingStageShape{TargetLedgerTable: true, PointTombstones: 1, NoWriteFastPath: true}},
	}
	for _, tc := range shapeCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, CheckDataBranchFencingStageShape(tc.step, tc.shape))
		})
	}

	// Malformed round input is refused before any step.
	_, err = ExecuteDataBranchFencingRound(context.Background(),
		inlineFencingLaneRunner{}, &fakeScratchLocks{}, newFakeFencingSession(700),
		DataBranchFencingRoundInput{ScratchTableID: 0, IssueTimeNS: 1})
	require.Error(t, err)
	fe, ok := AsDataBranchFencingError(err)
	require.True(t, ok)
	require.Equal(t, DataBranchFencingRoundGateRejected, fe.Class)
}

type inlineFencingLaneRunner struct{}

func (inlineFencingLaneRunner) RunStep(
	ctx context.Context, step DataBranchFencingStepKind, fn DataBranchFencingLaneFunc,
) (any, error) {
	return fn(ctx)
}

// (f) Partition classes + acceptance gate + K-bounded re-fence counter.
func TestDataBranchFencingRetireRejectionClassifier(t *testing.T) {
	ctx := context.Background()
	rec := DataBranchFencingRecord{
		ScratchTableID: 42,
		CommitTS1:      tsOf(800),
		CommitTS:       tsOf(900),
		Binding:        DataBranchFencingBinding{Table: 1001, ServiceID: "svc-a", Version: 7},
	}
	sameGen := rec.Binding
	newGen := DataBranchFencingBinding{Table: 1001, ServiceID: "svc-b", Version: 8}

	classCases := []struct {
		name string
		view DataBranchRetireGrantView
		want DataBranchRetireRejectionClass
	}{
		{
			name: "recorded binding changed via AddLockTable",
			view: DataBranchRetireGrantView{
				CallerBindingRecorded: true,
				GrantErr:              moerr.NewLockTableBindChanged(ctx),
				GrantBinding:          newGen,
			},
			want: DataBranchRetireRejectionBindingChanged,
		},
		{
			name: "recorded binding changed defensive terminal",
			view: DataBranchRetireGrantView{
				CallerBindingRecorded: true,
				GrantBinding:          newGen,
				Fence:                 tsOf(910),
			},
			want: DataBranchRetireRejectionBindingChanged,
		},
		{
			name: "bound same-generation fold lag",
			view: DataBranchRetireGrantView{
				CallerBindingRecorded: true,
				GrantBinding:          sameGen,
				Fence:                 tsOf(850),
			},
			want: DataBranchRetireRejectionFoldLag,
		},
		{
			name: "unbound same-generation fold lag",
			view: DataBranchRetireGrantView{
				CallerBindingRecorded: false,
				GrantBinding:          sameGen,
				Fence:                 tsOf(850),
			},
			want: DataBranchRetireRejectionFoldLag,
		},
		{
			name: "unbound pre-grant flip",
			view: DataBranchRetireGrantView{
				CallerBindingRecorded: false,
				GrantBinding:          newGen,
				Fence:                 tsOf(910),
			},
			want: DataBranchRetireRejectionPreGrantFlip,
		},
	}
	for _, tc := range classCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyDataBranchRetireRejection(tc.view, rec)
			require.Equal(t, tc.want, got)
		})
	}

	// Acceptance gate: B_r == B_f AND fence >= C_f.
	acceptCases := []struct {
		name    string
		binding DataBranchFencingBinding
		fence   timestamp.Timestamp
		record  DataBranchFencingRecord
		want    bool
	}{
		{"exact frontier", sameGen, tsOf(900), rec, true},
		{"beyond frontier", sameGen, tsOf(999), rec, true},
		{"lagging fold", sameGen, tsOf(899), rec, false},
		{"stray generation", newGen, tsOf(999), rec, false},
		{"zero binding record", sameGen, tsOf(999), DataBranchFencingRecord{CommitTS: tsOf(900)}, false},
		{"zero fencing commit", sameGen, tsOf(999), DataBranchFencingRecord{Binding: sameGen}, false},
	}
	for _, tc := range acceptCases {
		t.Run("accept/"+tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, AcceptDataBranchFencingRecord(tc.record, tc.binding, tc.fence))
		})
	}

	// K bound: fold-lag and pre-grant flips consume; class (i) never
	// advances the counter and is terminal from the first occurrence.
	counter := NewDataBranchRefenceCounter(2)
	require.NoError(t, counter.Consume(ctx, DataBranchRetireRejectionFoldLag))
	require.Equal(t, 1, counter.Rounds())
	require.NoError(t, counter.Consume(ctx, DataBranchRetireRejectionPreGrantFlip))
	require.Equal(t, 2, counter.Rounds())
	err := counter.Consume(ctx, DataBranchRetireRejectionFoldLag)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
	require.True(t, moerr.MustRollbackTxn(err))

	terminal := NewDataBranchRefenceCounter(3)
	err = terminal.Consume(ctx, DataBranchRetireRejectionBindingChanged)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
	require.Equal(t, 0, terminal.Rounds())
}

// SQL shape builders keep every statement inside the kind=1 namespace.
func TestDataBranchFencingSQLShapesKeepKindOne(t *testing.T) {
	require.Contains(t, DataBranchFencingSweepCandidatesSQL(5), "row_kind = 1")
	require.Contains(t, DataBranchFencingSweepDeleteSQL(9), "row_kind = 1")
	require.Contains(t, DataBranchFencingScratchInsertSQL(9, 10), "values (1, 9, '', '', 0, 10, 0)")
	require.Contains(t, DataBranchFencingScratchDeleteSQL(9), "table_id = 9")
	require.Contains(t, DataBranchFencingScratchProbeSQL(9), "row_kind = 1")

	id, err := DataBranchFencingScratchDraw()
	require.NoError(t, err)
	require.NotZero(t, id)
	require.Equal(t, uint64(0), DataBranchFencingSweepAgeCutoff(10, 20*time.Second))
	require.Equal(t, uint64(90000000000), DataBranchFencingSweepAgeCutoff(100000000000, 10*time.Second))
}
