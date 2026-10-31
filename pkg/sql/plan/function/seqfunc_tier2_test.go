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

package function

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	txn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Tier-2 conversion oracles (design §4 D, v13–v21; §9
// allocation-surface/retry-boundary list): the tier-1 wrapper exhausts into
// the compiler's statement-retry signal ONLY through the two-phase admission
// — the pre-filter here, the arbitration in pkg/sql/compile — the sticky
// retry window D shapes every execution of a live episode, D-expiries at the
// zero-attempt entry and at conversion run the shared probe-first expiry
// decision (B2), and the episode closes (Z1) exactly when the watch set
// empties.

// expectTier2Envelope registers the dispatch-envelope reads the pre-filter
// performs on the caller transaction: the frozen {Autocommit, ByBegin}
// options and the isolation level.
func expectTier2Envelope(f *seqRouteFixture, autocommit, byBegin bool, isolation txn.TxnIsolation) {
	f.callerTxn.EXPECT().TxnOptions().Return(txn.TxnOptions{
		Autocommit: autocommit, ByBegin: byBegin,
	}).AnyTimes()
	f.callerTxn.EXPECT().Txn().Return(txn.TxnMeta{Isolation: isolation}).AnyTimes()
}

func tier2WatchIdentity(f *seqRouteFixture) process.WatchIdentity {
	return process.WatchIdentity{DBName: "db", TblName: "seq", WantRel: f.relID}
}

// fixtureBaseline takes the production baseline sample against the fixture's
// committed row — the same complete committed tuple the admission probe and
// the conversion-path sample record.
func fixtureBaseline(t *testing.T, f *seqRouteFixture, id process.WatchIdentity) process.SeqTuple {
	t.Helper()
	tuple, err := SampleSequenceBaseline(f.proc.Ctx, f.proc, id, time.Second, time.Time{})
	require.NoError(t, err)
	return tuple
}

// Conversion under a full admission envelope: an exhausted transient-conflict
// class becomes the compiler's RC retry signal; the pending payload carries
// the named terminal error (for the decline path), the conflict residue and
// the statement deadline (none in this context); neither the sticky window
// nor the watch entry is created at conversion — the first admission probe
// at the arbitration seam owns both (A1/Z2).
func TestNextvalAutonomousExhaustionConvertsUnderRetryState(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()
	expectTier2Envelope(f, true, false, txn.TxnIsolation_RC)
	retryState := process.NewSequenceRetryState()
	f.proc.GetSessionInfo().SeqRetry = retryState

	f.exec.attemptOutcome = func(int) error { return moerr.NewLockWaitTimeoutNoCtx() }

	_, err := nextvalRow(f.proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
		"exhausted conflicts convert to the statement retry signal class, got %v", err)
	require.Equal(t, seqAutonomousMaxAttempts, f.exec.attempts)

	items := retryState.ConsumePendings()
	require.Len(t, items, 1)
	require.Equal(t, tier2WatchIdentity(f), items[0].Identity)
	require.ErrorContains(t, items[0].Pending.Original,
		"sequence db.seq: allocation could not acquire the sequence row within 8 attempts",
		"the decline path must return the ORIGINAL named error")
	require.NotNil(t, items[0].Pending.Cause)
	require.Zero(t, items[0].Pending.StmtDeadline, "this statement context carries no deadline")
	require.Empty(t, retryState.ConsumePendings(), "the pending conversion is consumed exactly once")
	_, hasWindow := retryState.RetryDeadline()
	require.False(t, hasWindow, "conversion never creates the window; arbitration does")
	require.False(t, retryState.HasWatch(tier2WatchIdentity(f)),
		"conversion never creates the watch entry before the first admission; the seam's admission probe does")
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues)
}

// Default-deny: without a per-statement retry state (background/internal
// processes) the same exhaustion stays the terminal named error.
func TestNextvalAutonomousExhaustionTerminalWithoutRetryState(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()

	f.exec.attemptOutcome = func(int) error { return moerr.NewLockWaitTimeoutNoCtx() }

	_, err := nextvalRow(f.proc)
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
		"no conversion without a retry state")
	require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
	require.Equal(t, seqAutonomousMaxAttempts, f.exec.attempts)
}

// Pre-filter matrix: each arm denies exactly one term (publication already
// won, autocommit=0, explicit BEGIN, snapshot isolation) while the rest of
// the envelope would admit. Every arm keeps the terminal named error and
// records no pending conversion.
func TestNextvalAutonomousConversionPreFilterMatrix(t *testing.T) {
	cases := []struct {
		name  string
		setup func(f *seqRouteFixture, st *process.SequenceRetryState)
	}{
		{"publication already won", func(f *seqRouteFixture, st *process.SequenceRetryState) {
			st.MarkPublished()
			// The publication check precedes the envelope reads.
		}},
		{"autocommit off", func(f *seqRouteFixture, st *process.SequenceRetryState) {
			expectTier2Envelope(f, false, false, txn.TxnIsolation_RC)
		}},
		{"explicit begin", func(f *seqRouteFixture, st *process.SequenceRetryState) {
			expectTier2Envelope(f, true, true, txn.TxnIsolation_RC)
		}},
		{"snapshot isolation", func(f *seqRouteFixture, st *process.SequenceRetryState) {
			expectTier2Envelope(f, true, false, txn.TxnIsolation_SI)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
			f.expectCommittedRoute()
			retryState := process.NewSequenceRetryState()
			f.proc.GetSessionInfo().SeqRetry = retryState
			tc.setup(f, retryState)

			f.exec.attemptOutcome = func(int) error { return moerr.NewLockWaitTimeoutNoCtx() }

			_, err := nextvalRow(f.proc)
			require.False(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
				"%s must deny conversion", tc.name)
			require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
			require.Empty(t, retryState.ConsumePendings(), "a denied conversion records no pending signal")
		})
	}
}

// The sticky window bounds every execution of a live episode (G2): the
// attempt loop derives its budget from D and stops early, and the A2-tagged
// in-flight expiry reaches the conversion pre-filter, which runs the shared
// probe-first expiry decision (site 3) instead of a wall-clock deny. With
// the contended identity terminal-eligible, committed-unchanged AND held,
// the decision produces the exactly-once terminal: the conversion keeps the
// named surface, no retry signal is emitted, and every later execution
// short-circuits on the stamped verdict.
func TestNextvalAutonomousExpiredWindowConversionRunsDecision(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()
	expectTier2Envelope(f, true, false, txn.TxnIsolation_RC)
	retryState := process.NewSequenceRetryState()
	f.proc.GetSessionInfo().SeqRetry = retryState

	id := tier2WatchIdentity(f)
	// Terminal-eligible from the first decision: the baseline is one full
	// renewal interval old. The attempts' honest lock-wait aborts die at
	// their own for-update reads, so the committed row behind the fixture
	// stays exactly at the sampled baseline across the whole episode.
	retryState.SetWatchingBaselined(id, fixtureBaseline(t, f, id),
		time.Now().Add(-2*seqAutonomousBudgetDur))
	retryState.AdmitRetry(time.Now().Add(120 * time.Millisecond))
	f.exec.allocLockBlocked = true
	f.exec.probeLockHeld = true // the row stays held for the decision too

	_, err := nextvalRow(f.proc)
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
		"a terminal verdict forbids conversion")
	require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
	require.Less(t, f.exec.attempts, seqAutonomousMaxAttempts,
		"post-admission executions derive their budget from D and stop early")
	require.Empty(t, retryState.ConsumePendings())
	stamped, terminal := retryState.TerminalIdentity()
	require.True(t, terminal, "the decision stamped the exactly-once verdict")
	require.Equal(t, id, stamped)

	// The stamped terminal short-circuits every later allocation execution
	// of the statement: no attempts, no conversion, no re-probe.
	attempts, probeAttempts := f.exec.attempts, f.exec.probeAttempts
	_, err = nextvalRow(f.proc)
	require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
	require.Equal(t, attempts, f.exec.attempts, "a stamped terminal opens no attempt loop")
	require.Equal(t, probeAttempts, f.exec.probeAttempts, "a stamped terminal re-probes nothing")
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
}

// At or after the elapsed live window an execution opens no new attempt
// loop: the zero-attempt entry decision (site 2) runs the shared probe-first
// expiry decision and, on committed-unchanged AND held, surfaces the
// terminal composed with the recorded conflict residue — the decision did
// probe the identity even though no allocation attempt ever ran.
func TestNextvalAutonomousZeroAttemptsDecisionTerminals(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()
	retryState := process.NewSequenceRetryState()
	f.proc.GetSessionInfo().SeqRetry = retryState

	id := tier2WatchIdentity(f)
	retryState.SetWatchingBaselined(id, fixtureBaseline(t, f, id),
		time.Now().Add(-2*seqAutonomousBudgetDur))
	retryState.SetPending(id, &process.SequenceRetryPending{
		Original: moerr.NewInternalErrorNoCtx("prior named surface"),
		Cause:    moerr.NewLockWaitTimeoutNoCtx(),
	})
	_ = retryState.ConsumePendings() // an admitted first conversion consumed it
	retryState.AdmitRetry(time.Now().Add(-time.Millisecond))
	f.exec.probeLockHeld = true

	_, err := nextvalRow(f.proc)
	require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire the sequence row within 8 attempts")
	require.ErrorContains(t, err, "Lock wait timeout exceeded",
		"the terminal surface composes the recorded residue")
	require.Equal(t, 0, f.exec.attempts, "no new attempt loop opens at or after D")
	require.Greater(t, f.exec.probeAttempts, 0, "the zero-attempt decision probes the watch set")
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
	require.Empty(t, retryState.ConsumePendings(), "the zero-attempt surface never converts")
}

// Episode closure (Z1) at the function layer: the watch set emptied by the
// statement's own committed allocation retires D, so the NEXT allocation —
// entered long after the retired deadline — runs on the ordinary
// per-invocation budget and succeeds with no trace of the old window. A
// later conflict after closure is a FRESH first admission: the retry signal
// is emitted, the payload recorded, and neither window nor watch state
// leaks from the closed episode.
func TestNextvalAutonomousClosedEpisodeRunsOnBaseBudget(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()
	retryState := process.NewSequenceRetryState()
	f.proc.GetSessionInfo().SeqRetry = retryState

	id := tier2WatchIdentity(f)
	retryState.EnsureWatching(id)
	retryState.AdmitRetry(time.Now().Add(-time.Millisecond)) // an old, live window
	retryState.RemoveWatchByName("db", "seq")                // the statement served its contention
	_, hasWindow := retryState.RetryDeadline()
	require.False(t, hasWindow, "emptying the watch set retired the window")

	value, err := nextvalRow(f.proc)
	require.NoError(t, err, "after closure the base per-invocation budget governs")
	require.Equal(t, "1", value, "the uncalled fixture's pending value")
	require.Equal(t, 1, f.exec.attempts)

	// Re-contention after closure: a fresh first admission — the retry
	// signal and payload are created, nothing carries over from the closed
	// episode.
	expectTier2Envelope(f, true, false, txn.TxnIsolation_RC)
	f.exec.attemptOutcome = func(int) error { return moerr.NewLockWaitTimeoutNoCtx() }
	_, err = nextvalRow(f.proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
		"a conflict after closure re-admits fresh")
	require.Equal(t, 1+seqAutonomousMaxAttempts, f.exec.attempts)
	items := retryState.ConsumePendings()
	require.Len(t, items, 1)
	require.Equal(t, id, items[0].Identity)
	_, hasWindow = retryState.RetryDeadline()
	require.False(t, hasWindow, "the fresh window belongs to the next arbitration")
	require.False(t, retryState.HasWatch(id),
		"the fresh watch entry belongs to the seam's first-admission probe")
}

// A live episode whose contention has fully resolved closes at the
// conversion decision rather than renewing: the all-resolved probe round
// retires D (Z1), and the conversion in flight re-admits on a fresh
// creation-sample baseline, emitting the retry signal for the arbitration
// seam.
func TestNextvalAutonomousAllResolvedRetiresAtConversionDecision(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()
	expectTier2Envelope(f, true, false, txn.TxnIsolation_RC)
	retryState := process.NewSequenceRetryState()
	f.proc.GetSessionInfo().SeqRetry = retryState

	id := tier2WatchIdentity(f)
	retryState.SetWatchingBaselined(id, fixtureBaseline(t, f, id),
		time.Now().Add(-2*seqAutonomousBudgetDur))
	retryState.AdmitRetry(time.Now().Add(120 * time.Millisecond)) // live at entry, elapsed at conversion
	// The conflict drains before the conversion: the row reads unchanged
	// (no peer progress — the honest lock-wait aborts never mutated the
	// committed row) but is ACQUIRABLE again: the attempts' own for-update
	// reads fail honestly while the probe knobs stay off.
	f.exec.allocLockBlocked = true

	_, err := nextvalRow(f.proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
		"episode closure admits: the conversion re-admits fresh after the all-resolved round")
	require.Less(t, f.exec.attempts, seqAutonomousMaxAttempts,
		"the attempts stopped at the elapsed window")
	_, hasWindow := retryState.RetryDeadline()
	require.False(t, hasWindow, "the all-resolved round retired the old window (Z1)")
	require.Equal(t, []process.WatchIdentity{id}, retryState.WatchIdentities(),
		"the conversion re-recorded the identity with its fresh creation baseline")
	items := retryState.ConsumePendings()
	require.Len(t, items, 1)
}

// pinFirstNow scripts the attempt loop's live-window clock seam
// (seqAutonomousNow): its FIRST call reports a time at which the given
// window still reads live (so the allocation-entry check sees no expiry),
// and every later call reports real time (whose window has already
// elapsed). This drives the expiry-before-first-attempt boundary
// deterministically: the entry check passes, the bounded context built
// from the real-elapsed window is born expired, and the attempt loop's
// first context check observes the boundary. The expiry decision machinery
// itself keeps the real clock.
func pinFirstNow(t *testing.T, window time.Time) {
	t.Helper()
	old := seqAutonomousNow
	read := 0
	seqAutonomousNow = func() time.Time {
		read++
		if read == 1 {
			return window.Add(-100 * time.Millisecond)
		}
		return time.Now()
	}
	t.Cleanup(func() { seqAutonomousNow = old })
}

// Expiry before the first attempt (design §4 D B2/Z1): a tier-2 live window
// that reads live at the allocation-entry check but has elapsed by the
// attempt loop's first context check must take the SAME shared probe-first
// expiry decision as the entry and conversion surfaces — renewal and
// resolution fund the continuation and the allocation succeeds, while
// committed-unchanged AND held contention surfaces the exactly-once named
// terminal. The pre-repair loop head returned the raw derived context
// error (lastErr == nil on the first iteration), which the compiler's
// retry classifier rejects — so even resolvable or progressing contention
// failed the statement with "context deadline exceeded".
func TestNextvalAutonomousPreAttemptWindowExpiryRunsDecision(t *testing.T) {
	newRetryFixture := func(t *testing.T) (*seqRouteFixture, *process.SequenceRetryState, process.WatchIdentity) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
		f.expectCommittedRoute()
		retryState := process.NewSequenceRetryState()
		f.proc.GetSessionInfo().SeqRetry = retryState
		return f, retryState, tier2WatchIdentity(f)
	}

	t.Run("unchanged and held surfaces the named terminal", func(t *testing.T) {
		f, retryState, id := newRetryFixture(t)
		retryState.SetWatchingBaselined(id, fixtureBaseline(t, f, id),
			time.Now().Add(-2*seqAutonomousBudgetDur))
		retryState.SetPending(id, &process.SequenceRetryPending{
			Original: moerr.NewInternalErrorNoCtx("prior named surface"),
			Cause:    moerr.NewLockWaitTimeoutNoCtx(),
		})
		_ = retryState.ConsumePendings() // an admitted first conversion consumed it
		window := time.Now().Add(-time.Millisecond)
		retryState.AdmitRetry(window)
		f.exec.probeLockHeld = true
		pinFirstNow(t, window)
		probesBefore := f.exec.probeAttempts // fixtureBaseline's own creation sample

		_, err := nextvalRow(f.proc)
		require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire the sequence row within 8 attempts")
		require.ErrorContains(t, err, "Lock wait timeout exceeded",
			"the terminal surface composes the recorded residue")
		require.NotContains(t, err.Error(), "context deadline exceeded",
			"the boundary never surfaces the raw derived-context error")
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
			"the terminal is final, not a fresh conversion")
		require.Equal(t, 0, f.exec.attempts, "no attempt loop opened before the arbitration")
		require.Greater(t, f.exec.probeAttempts, probesBefore,
			"the boundary ran the shared probe-first expiry decision")
	})

	t.Run("observed progress renews and the allocation succeeds", func(t *testing.T) {
		f, retryState, id := newRetryFixture(t)
		baseline := fixtureBaseline(t, f, id)
		stale := process.SeqTuple{RelID: baseline.RelID, LastSeq: baseline.LastSeq - 1, IsCalled: baseline.IsCalled}
		retryState.SetWatchingBaselined(id, stale, time.Now().Add(-2*seqAutonomousBudgetDur))
		window := time.Now().Add(-time.Millisecond)
		retryState.AdmitRetry(window)
		pinFirstNow(t, window)
		probesBefore := f.exec.probeAttempts

		var funded time.Time
		f.exec.beforeCommit = func(int) {
			// The statement's own success has not retired the watch set at
			// this point, so a RENEWED window is still live here; a wrongly
			// retired-or-skipped decision shows up as a zero value.
			if w, ok := retryState.RetryDeadline(); ok {
				funded = w
			}
		}

		value, err := nextvalRow(f.proc)
		require.NoError(t, err, "the renewal funds the continuation instead of failing the statement")
		require.Equal(t, "1", value, "the uncalled fixture's pending value")
		require.Equal(t, 1, f.exec.attempts)
		require.Greater(t, f.exec.probeAttempts, probesBefore,
			"the boundary ran the shared probe-first expiry decision")
		require.True(t, funded.After(window) && funded.After(time.Now()),
			"the decision RENEWED the window (not retired, not skipped) before the first attempt ran")
	})

	t.Run("unchanged but acquirable resolves and the allocation succeeds", func(t *testing.T) {
		f, retryState, id := newRetryFixture(t)
		retryState.SetWatchingBaselined(id, fixtureBaseline(t, f, id),
			time.Now().Add(-2*seqAutonomousBudgetDur))
		window := time.Now().Add(-time.Millisecond)
		retryState.AdmitRetry(window)
		pinFirstNow(t, window)
		probesBefore := f.exec.probeAttempts

		value, err := nextvalRow(f.proc)
		require.NoError(t, err, "an unchanged-but-acquirable row resolves the contention (Z1)")
		require.Equal(t, "1", value, "the uncalled fixture's pending value")
		require.Equal(t, 1, f.exec.attempts)
		require.Greater(t, f.exec.probeAttempts, probesBefore,
			"the boundary ran the shared probe-first expiry decision")
		_, hasWindow := retryState.RetryDeadline()
		require.False(t, hasWindow, "the resolved episode retired the window (Z1)")
	})

	t.Run("caller cancellation precedes the boundary decision", func(t *testing.T) {
		f, retryState, id := newRetryFixture(t)
		retryState.SetWatchingBaselined(id, fixtureBaseline(t, f, id),
			time.Now().Add(-2*seqAutonomousBudgetDur))
		window := time.Now().Add(-time.Millisecond)
		retryState.AdmitRetry(window)
		stmtCtx, cancel := context.WithCancel(f.proc.Ctx)
		f.proc.Ctx = stmtCtx
		pinFirstNow(t, window)
		cancel() // the caller context is already dead when the boundary fires
		probesBefore := f.exec.probeAttempts

		_, err := nextvalRow(f.proc)
		require.True(t, errors.Is(err, context.Canceled),
			"caller cancellation surfaces unchanged, got %v", err)
		require.Equal(t, 0, f.exec.attempts)
		require.Equal(t, probesBefore, f.exec.probeAttempts,
			"caller cancellation precedes any arbitration probe")
	})
}

// Renewal at the conversion decision: the peer kept committing progress
// (the committed tuple changed since the recorded baseline), so the shared
// decision renews the window and the conversion emits the retry signal —
// the episode continues under the renewed D.
func TestNextvalAutonomousRenewsAtConversionDecisionWhileProgressing(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()
	expectTier2Envelope(f, true, false, txn.TxnIsolation_RC)
	retryState := process.NewSequenceRetryState()
	f.proc.GetSessionInfo().SeqRetry = retryState

	id := tier2WatchIdentity(f)
	baseline := fixtureBaseline(t, f, id)
	// Observable committed change at the decision: the recorded baseline is
	// one full interval old AND behind the fixture's current row.
	stale := process.SeqTuple{RelID: baseline.RelID, LastSeq: baseline.LastSeq - 1, IsCalled: baseline.IsCalled}
	retryState.SetWatchingBaselined(id, stale, time.Now().Add(-2*seqAutonomousBudgetDur))
	retryState.AdmitRetry(time.Now().Add(120 * time.Millisecond)) // live at entry, elapsed at conversion
	// The attempts' honest lock-wait aborts leave the committed row at the
	// sampled tuple, so the sole change evidence at the decision is the
	// baseline's stale LastSeq — exactly the observed-progress arm (B1).
	f.exec.allocLockBlocked = true

	_, err := nextvalRow(f.proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
		"observable committed progress renews the window and admits the conversion")
	window, live := retryState.RetryDeadline()
	require.True(t, live, "the renewed window is live")
	require.True(t, window.After(time.Now()), "renewal extended the window")
	items := retryState.ConsumePendings()
	require.Len(t, items, 1)
	require.Equal(t, id, items[0].Identity)
}

// The window D is the deadline handed to every attempt of a live episode
// (remaining = D − now), not a fresh per-invocation budget.
func TestNextvalAutonomousDerivesAttemptDeadlineFromStickyWindow(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()
	retryState := process.NewSequenceRetryState()
	f.proc.GetSessionInfo().SeqRetry = retryState
	retryState.EnsureWatching(tier2WatchIdentity(f))
	window := time.Now().Add(250 * time.Millisecond)
	retryState.AdmitRetry(window)

	var attemptDeadline time.Time
	f.exec.inspectTxnCtx = func(ctx context.Context) {
		if deadline, ok := ctx.Deadline(); ok {
			attemptDeadline = deadline
		}
	}

	value, err := nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "1", value, "the uncalled fixture's pending value")
	require.False(t, attemptDeadline.IsZero(), "the attempt context carries the derived deadline")
	require.WithinDuration(t, window, attemptDeadline, 30*time.Millisecond,
		"the execution budget is derived from D")
}

// The in-flight budget-expiry route (A2): an attempt blocked past its
// D-derived context records the derived-budget tag and the conversion routes
// it into the shared probe-first expiry decision instead of failing the
// transient classifier. A LIVE episode renews on observable committed
// progress (converting to the retry signal) or terminals exactly once on
// committed-unchanged AND held; the SAME tagged expiry BEFORE any admission
// keeps its unchanged named surface — there is no episode to renew.
func TestNextvalAutonomousInFlightBudgetExpiryRoutesThroughDecision(t *testing.T) {
	oldBudget := seqAutonomousBudgetDur
	seqAutonomousBudgetDur = 150 * time.Millisecond
	t.Cleanup(func() { seqAutonomousBudgetDur = oldBudget })

	t.Run("pre-admission budget expiry keeps the named surface", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
		f.expectCommittedRoute()
		expectTier2Envelope(f, true, false, txn.TxnIsolation_RC)
		retryState := process.NewSequenceRetryState()
		f.proc.GetSessionInfo().SeqRetry = retryState
		f.exec.blockOnCtx = true

		_, err := nextvalRow(f.proc)
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
			"a pre-admission budget expiry never converts (A2)")
		require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
		require.Equal(t, 1, f.exec.attempts, "the blocked attempt ended at its budget")
		require.Empty(t, retryState.ConsumePendings())
	})

	t.Run("observable progress renews the live episode", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
		f.expectCommittedRoute()
		expectTier2Envelope(f, true, false, txn.TxnIsolation_RC)
		retryState := process.NewSequenceRetryState()
		f.proc.GetSessionInfo().SeqRetry = retryState

		id := tier2WatchIdentity(f)
		baseline := fixtureBaseline(t, f, id)
		stale := process.SeqTuple{RelID: baseline.RelID, LastSeq: baseline.LastSeq - 1, IsCalled: baseline.IsCalled}
		retryState.SetWatchingBaselined(id, stale, time.Now().Add(-2*seqAutonomousBudgetDur))
		retryState.AdmitRetry(time.Now().Add(150 * time.Millisecond))
		f.exec.blockOnCtx = true

		_, err := nextvalRow(f.proc)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry),
			"the A2-tagged expiry routes into the decision, which renews on peer progress")
		require.Equal(t, 1, f.exec.attempts)
		require.Greater(t, f.exec.probeAttempts, 0, "the decision ran its probe round")
		window, live := retryState.RetryDeadline()
		require.True(t, live && window.After(time.Now()), "the episode continues under the renewed window")
		require.Len(t, retryState.ConsumePendings(), 1)
	})

	t.Run("stalled and held terminals exactly once", func(t *testing.T) {
		f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
		f.expectCommittedRoute()
		expectTier2Envelope(f, true, false, txn.TxnIsolation_RC)
		retryState := process.NewSequenceRetryState()
		f.proc.GetSessionInfo().SeqRetry = retryState

		id := tier2WatchIdentity(f)
		retryState.SetWatchingBaselined(id, fixtureBaseline(t, f, id),
			time.Now().Add(-2*seqAutonomousBudgetDur))
		retryState.AdmitRetry(time.Now().Add(150 * time.Millisecond))
		f.exec.blockOnCtx = true
		f.exec.probeLockHeld = true

		_, err := nextvalRow(f.proc)
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
		require.ErrorContains(t, err, "sequence db.seq: allocation could not acquire")
		require.Equal(t, 1, f.exec.attempts)
		stamped, terminal := retryState.TerminalIdentity()
		require.True(t, terminal)
		require.Equal(t, id, stamped)
		require.Empty(t, retryState.ConsumePendings())
	})
}

// G2 no-conflict control: a statement NEVER admitted observes no new time
// limit — crossing the retry-window duration between two conflict-free
// allocations leaves each of them with a fresh per-invocation budget and
// creates no sticky window.
func TestSequenceRetryConflictFreeStatementKeepsPerInvocationBudgets(t *testing.T) {
	f := newSeqRouteFixture(t, 7, newSeqMetadataState(1, 1, 100, 1, 1, false, false))
	f.expectCommittedRoute()
	retryState := process.NewSequenceRetryState()
	f.proc.GetSessionInfo().SeqRetry = retryState

	oldBudget := seqAutonomousBudgetDur
	seqAutonomousBudgetDur = 150 * time.Millisecond
	t.Cleanup(func() { seqAutonomousBudgetDur = oldBudget })

	value, err := nextvalRow(f.proc)
	require.NoError(t, err)
	require.Equal(t, "1", value, "the uncalled fixture's pending value")

	time.Sleep(250 * time.Millisecond) // cross the retry-window duration

	value, err = nextvalRow(f.proc)
	require.NoError(t, err, "a conflict-free statement has no statement-wide sequence timeout")
	require.Equal(t, "2", value)
	_, hasWindow := retryState.RetryDeadline()
	require.False(t, hasWindow, "no admission ever created a window")
	require.Equal(t, 2, f.exec.attempts)
}

// A fresh statement execution's state carries no window and no publication:
// the window identity never leaks across statements (G2).
func TestSequenceRetryStateIsFreshPerStatement(t *testing.T) {
	retryState := process.NewSequenceRetryState()
	require.False(t, retryState.Published())
	_, hasWindow := retryState.RetryDeadline()
	require.False(t, hasWindow)
	require.Empty(t, retryState.ConsumePendings())
}
