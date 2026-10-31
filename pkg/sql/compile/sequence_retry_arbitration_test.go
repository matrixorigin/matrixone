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

package compile

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	txn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Tier-2 arbitration oracles (design §4 D, v13–v21; §9 retry-boundary
// list): the arbitration phase at the retry quiescence boundary is
// authoritative — it re-verifies the full predicate (frozen envelope facts,
// RC isolation, generic acceptance, publication flag, sticky window), answers
// for the WHOLE consumed pending set as a unit (B3), admits only while the
// window can still be honored, and creates the window exactly once per live
// episode. v21: on admission the admission probe creates every NEWLY
// contended watch entry — carrying its committed-tuple baseline when the
// bounded sample succeeds, recorded young otherwise — and an elapsed window
// at the seam runs the shared probe-first expiry decision (renew / retire /
// terminal) instead of declining on the wall clock alone. The UT process
// has no engine on its context, so the probe machinery cannot be
// constructed here: admission samples fail into young entries and an
// elapsed-window decision fails safe to the terminal (default-deny), which
// the decline matrix uses deterministically. The sample/discard arms of the
// decision run against realistic probes in the process-level matrix and the
// function-level fixture tests.

func newTier2ArbitrationProc(t *testing.T, autocommit, byBegin bool, isolation txn.TxnIsolation) *process.Process {
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().TxnOptions().Return(txn.TxnOptions{
		Autocommit: autocommit, ByBegin: byBegin,
	}).AnyTimes()
	op.EXPECT().Txn().Return(txn.TxnMeta{Isolation: isolation}).AnyTimes()
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = op
	return proc
}

// tier2WatchID is the contended sequence identity the arbitration fixtures
// use. A non-zero relation id keeps its watch-set key distinct from any
// generation-replacement sibling.
var tier2WatchID = process.WatchIdentity{DBName: "db", TblName: "seq", WantRel: 7}

func tier2Pending(stmtDeadline time.Time) *process.SequenceRetryPending {
	return &process.SequenceRetryPending{
		Original: moerr.NewInternalErrorNoCtx(
			"sequence db.seq: allocation could not acquire the sequence row within 8 attempts: conflict"),
		StmtDeadline: stmtDeadline,
		Cause:        moerr.NewLockWaitTimeoutNoCtx(),
	}
}

// recordAndDrainPending performs the two production recording steps in
// production order: the writer path records the converted exhaustion for
// its contended identity, then the quiescence-boundary seam drains the
// whole pending set as one unit for a single arbitration answer (B3).
func recordAndDrainPending(st *process.SequenceRetryState, id process.WatchIdentity, pending *process.SequenceRetryPending) []process.SequencePendingItem {
	st.SetPending(id, pending)
	return st.ConsumePendings()
}

// requireArbAdmission runs the two-value arbitration verdict: the error
// lane carries only the caller's own cancellation (v21 precedence, F2).
func requireArbAdmission(t *testing.T, c *Compile, retryState *process.SequenceRetryState, items []process.SequencePendingItem, err error) bool {
	t.Helper()
	admitted, arbErr := c.arbitrateSequenceRetryAdmission(retryState, items, err)
	require.NoError(t, arbErr, "arbitration surfaces the caller's own cancellation only")
	return admitted
}

// Admission under the full predicate creates the sticky window capped by the
// statement deadline and the watch entry for the contended identity
// (A1/Z2); a second arbitration event of the same statement reuses the SAME
// window — the window never resets across retries while the episode is live
// (G2).
func TestSequenceRetryArbitrationAdmitsAndKeepsStickyWindow(t *testing.T) {
	proc := newTier2ArbitrationProc(t, true, false, txn.TxnIsolation_RC)
	retryState := process.NewSequenceRetryState()
	proc.GetSessionInfo().SeqRetry = retryState
	c := &Compile{proc: proc}

	items := recordAndDrainPending(retryState, tier2WatchID, tier2Pending(time.Now().Add(2*time.Second)))
	require.True(t, requireArbAdmission(t, c, retryState, items, moerr.NewTxnNeedRetryNoCtx()))
	window, ok := retryState.RetryDeadline()
	require.True(t, ok)
	require.True(t, items[0].Pending.StmtDeadline.Equal(window),
		"a statement deadline earlier than now+budget caps the window")
	require.True(t, retryState.HasWatch(tier2WatchID),
		"the admission probe created the watch entry (young: a UT process cannot sample)")

	second := recordAndDrainPending(retryState, tier2WatchID, tier2Pending(time.Now().Add(30*time.Minute)))
	require.True(t, requireArbAdmission(t, c, retryState, second, moerr.NewTxnNeedRetryNoCtx()))
	again, ok := retryState.RetryDeadline()
	require.True(t, ok)
	require.True(t, window.Equal(again), "the sticky window never resets across a statement's retries")
}

// Without a statement deadline the window is now + the calibrated budget
// (min(statement, 10 s) with no statement cap).
func TestSequenceRetryArbitrationWindowUsesCalibratedBudget(t *testing.T) {
	proc := newTier2ArbitrationProc(t, true, false, txn.TxnIsolation_RC)
	retryState := process.NewSequenceRetryState()
	proc.GetSessionInfo().SeqRetry = retryState
	c := &Compile{proc: proc}

	before := time.Now()
	items := recordAndDrainPending(retryState, tier2WatchID, tier2Pending(time.Time{}))
	require.True(t, requireArbAdmission(t, c, retryState, items, moerr.NewTxnNeedRetryNoCtx()))
	window, ok := retryState.RetryDeadline()
	require.True(t, ok)
	budget := function.SequenceAllocationRetryBudget()
	require.False(t, window.Before(before.Add(budget-time.Second)),
		"the window is at least the calibration from admission (minus scheduling slack)")
	require.False(t, window.After(time.Now().Add(budget+time.Second)),
		"the window is at most the calibration from admission plus slack")
}

// The conjunctive admission set (B3): one arbitration event answers for the
// whole drained pending set of the statement — pendings drain in
// first-recorded order, the single statement window comes from the tightest
// observation, and the admission probe creates the entry of every NEWLY
// contended identity, never just the first.
func TestSequenceRetryArbitrationAdmissionProbesEveryNewIdentity(t *testing.T) {
	proc := newTier2ArbitrationProc(t, true, false, txn.TxnIsolation_RC)
	retryState := process.NewSequenceRetryState()
	proc.GetSessionInfo().SeqRetry = retryState
	c := &Compile{proc: proc}
	second := process.WatchIdentity{DBName: "db", TblName: "seq2", WantRel: 8}

	retryState.SetPending(tier2WatchID, tier2Pending(time.Now().Add(5*time.Second)))
	retryState.SetPending(second, tier2Pending(time.Now().Add(5*time.Second)))
	items := retryState.ConsumePendings()
	require.Len(t, items, 2)
	require.Equal(t, tier2WatchID, items[0].Identity)
	require.Equal(t, second, items[1].Identity)
	require.True(t, requireArbAdmission(t, c, retryState, items, moerr.NewTxnNeedRetryNoCtx()))
	require.True(t, retryState.HasWatch(tier2WatchID))
	require.True(t, retryState.HasWatch(second))
	window, ok := retryState.RetryDeadline()
	require.True(t, ok)
	require.True(t, items[0].Pending.StmtDeadline.Equal(window),
		"the tightest statement-deadline observation caps the single conjunctive window")
}

// The publish-after-observation barrier oracle (G1/F1, deterministic
// choreography; executed under -race in the ladder): the pre-filter recorded
// a conversion while the flag was unset, then one batch entered the client
// write path before the quiescence boundary — arbitration declines, the
// pending carrying the ORIGINAL named error is consumed exactly once, and no
// window or watch entry is created. Publication always wins.
func TestSequenceRetryArbitrationPublishAfterObservationDeclines(t *testing.T) {
	proc := newTier2ArbitrationProc(t, true, false, txn.TxnIsolation_RC)
	retryState := process.NewSequenceRetryState()
	proc.GetSessionInfo().SeqRetry = retryState
	c := &Compile{proc: proc}

	require.False(t, retryState.Published())
	retryState.SetPending(tier2WatchID, tier2Pending(time.Time{}))
	retryState.MarkPublished() // the single publication path, before client bytes

	items := retryState.ConsumePendings()
	require.Len(t, items, 1)
	require.Empty(t, retryState.ConsumePendings(), "the pending conversion is consumed exactly once")
	require.False(t, requireArbAdmission(t, c, retryState, items, moerr.NewTxnNeedRetryNoCtx()),
		"publication wins: the delivered prefix is never replayed")
	require.ErrorContains(t, items[0].Pending.Original,
		"allocation could not acquire the sequence row within 8 attempts",
		"the client receives the ORIGINAL named error on decline")
	_, ok := retryState.RetryDeadline()
	require.False(t, ok, "a declined admission creates no window")
	require.False(t, retryState.HasWatch(tier2WatchID), "a declined admission creates no watch entry")
}

// Every denied term keeps the verdict final and creates no new window:
// elapsed window whose expiry decision must terminate (a stalled
// terminal-eligible identity in an environment that cannot probe fails
// safe), autocommit=0, explicit BEGIN, snapshot isolation, missing
// operator, and an error the generic retry policy would not accept.
func TestSequenceRetryArbitrationDeclineMatrix(t *testing.T) {
	rcAutocommit := &Compile{proc: newTier2ArbitrationProc(t, true, false, txn.TxnIsolation_RC)}

	cases := []struct {
		name  string
		c     *Compile
		state func() *process.SequenceRetryState
		err   error
	}{
		{"expired window, stalled eligible identity fails safe", rcAutocommit, func() *process.SequenceRetryState {
			st := process.NewSequenceRetryState()
			st.SetWatchingBaselined(tier2WatchID, process.SeqTuple{
				RelID:    tier2WatchID.WantRel,
				LastSeq:  1,
				IsCalled: true,
			}, time.Now().Add(-2*function.SequenceAllocationRetryBudget()))
			st.AdmitRetry(time.Now().Add(-time.Second))
			return st
		}, moerr.NewTxnNeedRetryNoCtx()},
		{"autocommit off", &Compile{proc: newTier2ArbitrationProc(t, false, false, txn.TxnIsolation_RC)},
			process.NewSequenceRetryState, moerr.NewTxnNeedRetryNoCtx()},
		{"explicit begin", &Compile{proc: newTier2ArbitrationProc(t, true, true, txn.TxnIsolation_RC)},
			process.NewSequenceRetryState, moerr.NewTxnNeedRetryNoCtx()},
		{"snapshot isolation", &Compile{proc: newTier2ArbitrationProc(t, true, false, txn.TxnIsolation_SI)},
			process.NewSequenceRetryState, moerr.NewTxnNeedRetryNoCtx()},
		{"internal statement has no operator", &Compile{proc: testutil.NewProcess(t)},
			process.NewSequenceRetryState, moerr.NewTxnNeedRetryNoCtx()},
		{"generic policy rejects the error", rcAutocommit, process.NewSequenceRetryState,
			moerr.NewInternalErrorNoCtx("not a retry class")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			retryState := tc.state()
			// Production wiring: the seam arbitrates the state installed on
			// its process, and the shared expiry decision pulls the same
			// instance from there.
			tc.c.proc.GetSessionInfo().SeqRetry = retryState
			items := recordAndDrainPending(retryState, tier2WatchID, tier2Pending(time.Time{}))
			require.False(t, requireArbAdmission(t, tc.c, retryState, items, tc.err),
				"%s must decline", tc.name)
			require.ErrorContains(t, items[0].Pending.Original, "allocation could not acquire",
				"the terminal surface stays the original named error")
		})
	}
}

func newTier2AutocommitRCTxn(t *testing.T) (client.TxnClient, client.TxnOperator) {
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(client.Workspace(&Ws{})).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
		Mode: txn.TxnMode_Optimistic, Isolation: txn.TxnIsolation_RC,
	}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{
		Autocommit: true, ByBegin: false,
	}).AnyTimes()
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().CheckLockTableBinds(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	return txnClient, txnOperator
}

// tier2RebuildSink models a statement whose sequence allocation converts its
// tier-1 exhaustion on every generation — the pending conversion is recorded
// for its contended identity from the writer path, exactly where the
// wrapper's pre-filter records it — and the attempt then fails retryable.
// From generation 2 on the writer blocks past the sticky window so the
// arbitration seam at that quiescence boundary MUST run the shared expiry
// decision — which in this engine-less UT environment fails safe to the
// terminal — and decline. stopAfter keeps a pre-fix (regressed) run from
// retrying forever: without the state carry-over each rebuild reseeds the
// window and the seam would admit indefinitely, so the sink lets the last
// generation succeed and the post-run assertions fail instead of hanging.
type tier2RebuildSink struct {
	mu           sync.Mutex
	proc         *process.Process
	initial      *process.SequenceRetryState
	stmtDeadline time.Time
	stopAfter    uint64
	begins       int
	writes       int
	aborts       []string
	conflicts    []string
	identityOK   bool
	deadlines    []time.Time
}

func (s *tier2RebuildSink) BeginAttempt(_ context.Context, generation uint64, proc *process.Process) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.proc = proc
	s.begins++
	if proc.GetSequenceRetryState() != s.initial {
		s.identityOK = false
	}
	if st := proc.GetSequenceRetryState(); st != nil {
		if d, ok := st.RetryDeadline(); ok {
			s.deadlines = append(s.deadlines, d)
		}
	}
	return nil
}

func (s *tier2RebuildSink) Write(generation uint64, bat *batch.Batch, _ *perfcounter.CounterSet) error {
	if bat == nil {
		return nil
	}
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	st := s.proc.GetSequenceRetryState()
	conflict := fmt.Sprintf(
		"sequence db.seq: allocation could not acquire the sequence row within 8 attempts: conflict-%d",
		generation)
	s.mu.Lock()
	s.conflicts = append(s.conflicts, conflict)
	s.mu.Unlock()
	if generation >= 2 {
		// Block the attempt past the sticky window created at the first
		// admission: the next arbitration must observe the ORIGINAL deadline
		// elapsed and let the probe-first expiry decision terminate the
		// statement. Sleeping to the OBSERVED deadline keeps this
		// deterministic regardless of host scheduling.
		if d, ok := st.RetryDeadline(); ok {
			if sleep := time.Until(d) + 25*time.Millisecond; sleep > 0 {
				time.Sleep(sleep)
			}
		}
	}
	st.SetPending(tier2WatchID, &process.SequenceRetryPending{
		Original:     moerr.NewInternalErrorNoCtx(conflict),
		StmtDeadline: s.stmtDeadline,
		Cause:        moerr.NewLockWaitTimeoutNoCtx(),
	})
	if generation >= s.stopAfter {
		return nil
	}
	return moerr.NewTxnNeedRetryNoCtx()
}

func (s *tier2RebuildSink) SealAttempt(generation uint64) error {
	return nil
}

func (s *tier2RebuildSink) AbortAttempt(generation uint64, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aborts = append(s.aborts, fmt.Sprintf("%d:%v", generation, cause))
	return nil
}

// The per-statement tier-2 retry state survives every retry rebuild (design
// §4 D G1/G2): buildRetryCompile re-installs the SAME instance after
// NewCompile's per-statement reseed, so the sticky window created at the
// first admission bounds every later generation of the SAME statement, and
// once that ORIGINAL deadline has elapsed the next converted exhaustion
// faces the probe-first expiry decision — terminal here by deterministic
// default-deny — and is declined: Compile.Run surfaces the statement's
// terminal named error exactly once and rebuilds no further generation.
// Before the carry-over the reseed discarded the window every rebuild: the
// seam re-admitted forever (asserted here as an identity mismatch plus an
// unexpected success/safety stop).
func TestCompileRunCarriesSequenceRetryStateAcrossRetryRebuilds(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().Buf = buffer.New()
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return ^uint64(0), nil
		}
		return "STRICT_TRANS_TABLES", nil
	})
	compilerCtx := plan2.NewEmptyCompilerContext(newPlanTestProcess(t))
	compilerCtx.SetContext(ctx)
	stmts, err := mysql.Parse(ctx, "select 1", 1)
	require.NoError(t, err)
	query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
	require.NoError(t, err)
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}

	txnCli, txnOp := newTier2AutocommitRCTxn(t)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	c := NewCompile("test", "test", "select 1", "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
	initial := proc.GetSequenceRetryState()
	require.NotNil(t, initial, "NewCompile installs a fresh per-statement retry state")
	require.NoError(t, c.Compile(ctx, pn, func(*batch.Batch, *perfcounter.CounterSet) error {
		return nil
	}))
	proc.Session = &remoteWarningCollector{}
	sink := &tier2RebuildSink{
		initial:      initial,
		stmtDeadline: time.Now().Add(1500 * time.Millisecond),
		stopAfter:    5,
		identityOK:   true,
	}
	c.SetResultSink(sink)
	_, runErr := c.Run(0)

	// The sticky window elapsed: the expiry decision terminated, the final
	// conversion was declined and the client receives THAT conversion's
	// original named error, once.
	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Error(t, runErr,
		"the statement must terminate with its named exhaustion error after the original deadline")
	require.NotEmpty(t, sink.conflicts)
	require.ErrorContains(t, runErr, sink.conflicts[len(sink.conflicts)-1],
		"the terminal surface is the LAST conversion's original error")
	require.GreaterOrEqual(t, sink.begins, 3,
		"admission rebuilt the statement at least twice before the window elapsed")
	require.True(t, sink.identityOK,
		"every retry generation must observe the SAME per-statement retry state instance")
	require.GreaterOrEqual(t, len(sink.deadlines), 2,
		"the sticky window must be observable on the later generations")
	for _, d := range sink.deadlines[1:] {
		require.True(t, sink.deadlines[0].Equal(d),
			"the sticky window never resets across the statement's retry rebuilds")
	}
	terminal := 0
	for _, aborted := range sink.aborts {
		if strings.Contains(aborted, "within 8 attempts") {
			terminal++
		}
	}
	require.Equal(t, 1, terminal,
		"exactly one aborted attempt carries the terminal named error; admitted retries carry the transient cause")
	require.Same(t, initial, proc.GetSequenceRetryState(),
		"the statement's retry state instance outlives every rebuild")

	c.Release()
	proc.Free()
	proc.GetSessionInfo().Buf.Free()
}

// tier2BarrierSink drives the publish-after-observation integration
// choreography (design §4 D barrier, executed end-to-end through Compile.Run
// under -race in the ladder). Its Write performs the two production steps in
// production order at the client-write point: first the allocation wrapper's
// pre-filter work (observing the flag still unset and recording the
// converted exhaustion for its identity), then the writeClientBatch work —
// publication BEFORE the batch's bytes (the resp_client call site is pinned
// separately from the real RespResult path, in the frontend package), with
// the delivered prefix bytes snapshotted. The attempt then fails retryable,
// so the seam at the quiescence boundary must arbitrate with publication
// already on the wire.
type tier2BarrierSink struct {
	mu               sync.Mutex
	proc             *process.Process
	observedUnset    bool
	pendingMsg       string
	begins           int
	deliveries       [][]byte
	prefixRows       int
	publishedAtWrite bool
	aborts           []string
	seals            int
}

func (s *tier2BarrierSink) BeginAttempt(_ context.Context, _ uint64, proc *process.Process) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.proc = proc
	s.begins++
	return nil
}

func (s *tier2BarrierSink) Write(generation uint64, bat *batch.Batch, _ *perfcounter.CounterSet) error {
	if bat == nil {
		return nil
	}
	st := s.proc.GetSequenceRetryState()
	// Pre-filter (allocation wrapper): converted the exhaustion while no
	// batch of THIS statement had reached the client yet.
	s.mu.Lock()
	s.observedUnset = !st.Published()
	s.pendingMsg = fmt.Sprintf(
		"sequence db.seq: allocation could not acquire the sequence row within 8 attempts: conflict-gen-%d",
		generation)
	st.SetPending(tier2WatchID, &process.SequenceRetryPending{
		Original: moerr.NewInternalErrorNoCtx(s.pendingMsg),
		Cause:    moerr.NewLockWaitTimeoutNoCtx(),
	})
	s.mu.Unlock()

	// Client write path (resp_client.writeClientBatch): publication lands
	// BEFORE any byte of this batch; then the prefix bytes are on the wire.
	st.MarkPublished()
	snapshot, serr := bat.MarshalBinary()
	if serr != nil {
		return serr
	}
	s.mu.Lock()
	s.publishedAtWrite = true
	s.prefixRows = bat.RowCount()
	s.deliveries = append(s.deliveries, snapshot)
	s.mu.Unlock()
	return moerr.NewTxnNeedRetryNoCtx()
}

func (s *tier2BarrierSink) SealAttempt(uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seals++
	return nil
}

func (s *tier2BarrierSink) AbortAttempt(generation uint64, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aborts = append(s.aborts, fmt.Sprintf("%d:%v", generation, cause))
	return nil
}

// Publish-after-observation barrier through the real Compile.Run retry
// boundary (design §4 D): a statement delivers a partial result prefix, its
// sequence allocation then converts an exhaustion (pre-filter observed the
// flag unset), and the attempt fails retryable with publication already on
// the wire. The arbitration seam MUST decline: no retry generation is
// rebuilt (nothing is replayed), the delivered prefix stays exactly as
// delivered (never resent or duplicated), and the client receives the
// ORIGINAL named exhaustion error exactly once. A watcher goroutine reads
// the publication flag concurrently with the Run so that the -race ladder
// proves the flag's synchronization on genuinely parallel access.
func TestCompileRunPublishAfterObservationDeclinesRetry(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().Buf = buffer.New()
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return ^uint64(0), nil
		}
		return "STRICT_TRANS_TABLES", nil
	})
	compilerCtx := plan2.NewEmptyCompilerContext(newPlanTestProcess(t))
	compilerCtx.SetContext(ctx)
	stmts, err := mysql.Parse(ctx, "select 1", 1)
	require.NoError(t, err)
	query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
	require.NoError(t, err)
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}

	txnCli, txnOp := newTier2AutocommitRCTxn(t)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	c := NewCompile("test", "test", "select 1", "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
	initial := proc.GetSequenceRetryState()
	require.NotNil(t, initial)
	require.NoError(t, c.Compile(ctx, pn, func(*batch.Batch, *perfcounter.CounterSet) error {
		return nil
	}))
	proc.Session = &remoteWarningCollector{}
	sink := &tier2BarrierSink{}
	c.SetResultSink(sink)

	// Concurrent flag access across the producer/boundary: the watcher only
	// ever READS Published, matching the production readers (arbitration
	// runs after the producer join while client-write goroutines may still
	// consult the flag). Deterministic order comes from the sequential
	// choreography; the watcher's overlap is what gives -race a genuine
	// concurrent reader/writer pair to validate.
	stopWatcher := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		for {
			select {
			case <-stopWatcher:
				return
			default:
				_ = initial.Published()
			}
		}
	}()
	_, runErr := c.Run(0)
	close(stopWatcher)
	<-watcherDone

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Error(t, runErr, "the declined statement surfaces its named exhaustion error")
	require.ErrorContains(t, runErr, sink.pendingMsg,
		"the client receives the ORIGINAL named error, not the transient retry cause")
	require.True(t, sink.observedUnset,
		"the pre-filter observed the flag unset when recording the conversion")
	require.True(t, sink.publishedAtWrite,
		"publication landed at the client-write point before the prefix bytes")
	require.Equal(t, 1, sink.begins,
		"publication wins: the statement is never rebuilt, so nothing is replayed")
	require.Equal(t, 0, sink.seals, "no attempt sealed successfully")
	require.Len(t, sink.deliveries, 1,
		"the delivered prefix was put on the wire exactly once — never duplicated")
	require.Equal(t, 1, sink.prefixRows, "the exact delivered prefix select 1 emits is a single row")
	require.Len(t, sink.aborts, 1,
		"exactly one attempt abort carries the terminal error to the client")
	require.Contains(t, sink.aborts[0], "within 8 attempts",
		"the abort carries the ORIGINAL named exhaustion error")
	require.True(t, initial.Published())
	_, windowOK := initial.RetryDeadline()
	require.False(t, windowOK, "a declined statement never opened a retry window")
	require.Empty(t, initial.ConsumePendings(), "the pending conversion was consumed exactly once at the seam")
	require.Same(t, initial, proc.GetSequenceRetryState(),
		"the statement's retry state instance is stable through a declined retry")

	c.Release()
	proc.Free()
	proc.GetSessionInfo().Buf.Free()
}

// sequenceCancelProbeExecutor satisfies prober construction: the cancelled
// entry check fires before any executor use, so no statement runs.
type sequenceCancelProbeExecutor struct{}

func (sequenceCancelProbeExecutor) Exec(context.Context, string, executor.Options) (executor.Result, error) {
	return executor.Result{}, moerr.NewInternalErrorNoCtx("sequenceCancelProbeExecutor: unexpected Exec")
}
func (sequenceCancelProbeExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	return moerr.NewInternalErrorNoCtx("sequenceCancelProbeExecutor: unexpected ExecTxn")
}

// F2 regression (v21 cancellation precedence): the retry window elapsed at
// the seam and the CALLER'S OWN context is cancelled. Arbitration must
// decline with context.Canceled on its error lane — never collapse the
// cancellation into the old pending's named terminal error.
func TestSequenceRetryArbitrationCallerCancellationWins(t *testing.T) {
	proc := newTier2ArbitrationProc(t, true, false, txn.TxnIsolation_RC)
	retryState := process.NewSequenceRetryState()
	proc.GetSessionInfo().SeqRetry = retryState
	rt := runtime.ServiceRuntime(proc.GetService())
	old, hadOld := rt.GetGlobalVariables(runtime.InternalSQLExecutor)
	rt.SetGlobalVariables(runtime.InternalSQLExecutor, sequenceCancelProbeExecutor{})
	t.Cleanup(func() {
		if hadOld {
			rt.SetGlobalVariables(runtime.InternalSQLExecutor, old)
		}
	})
	proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, newStubEngine())
	callerCtx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = callerCtx

	// Live but elapsed window with a contended watch entry: the expiry
	// decision runs at the seam.
	retryState.AdmitRetry(time.Now().Add(-time.Second))
	retryState.EnsureWatching(tier2WatchID)
	c := &Compile{proc: proc}
	items := recordAndDrainPending(retryState, tier2WatchID, tier2Pending(time.Now().Add(30*time.Minute)))

	cancel()
	admitted, arbErr := c.arbitrateSequenceRetryAdmission(retryState, items, moerr.NewTxnNeedRetryNoCtx())
	require.False(t, admitted, "a cancelled caller is never admitted")
	require.ErrorIs(t, arbErr, context.Canceled,
		"the caller's own cancellation surfaces unchanged, not the stale pending error")
	_, ok := retryState.RetryDeadline()
	require.True(t, ok, "the cancelled arbitration neither renews nor retires the window")
	require.Empty(t, retryState.ConsumePendings(), "the pending set stays consumed exactly once")
}
