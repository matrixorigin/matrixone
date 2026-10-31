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

package process

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Per-statement tier-2 retry state and shared expiry-decision semantics
// (design docs/design/sequence_ownership_state_consistency.md §4 D,
// v13–v21): the publication flag is once-only and synchronized, the pending
// payloads are consumed exactly once as a unit (B3), the retry window is
// created on the first admission and never reset (G2), the watch set is the
// retained contention set (A1) whose emptying retires the window (Z1), and
// the probe-first expiry decision (B2) renews only on observable committed
// change or availability resolution, terminals exactly once on a
// terminal-eligible identity committed-unchanged AND unavailable (A3/Z2).

const (
	seqRetryTestInterval = 10 * time.Second
)

func seqTestID(db, tbl string, rel uint64) WatchIdentity {
	return WatchIdentity{DBName: db, TblName: tbl, WantRel: rel}
}

// fakeClock drives SequenceRetryState.SetClockForTesting: the decision's
// ageing, renewal and eligibility arithmetic become fully deterministic.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Now()} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// stubSeqProber scripts the bounded committed observations of one expiry
// decision and records the probe order for the ordering oracles (young
// entries re-sampled first, then first-recorded order; no lock check for
// young or unobserved identities).
type stubTupleAnswer struct {
	tuple SeqTuple
	err   error
}

type stubLockAnswer struct {
	acquired bool
	err      error
}

type stubSeqProber struct {
	mu         sync.Mutex
	tuples     map[string]stubTupleAnswer
	locks      map[string]stubLockAnswer
	tupleCalls []string
	lockCalls  []string
	// onTuple/onLock fire mid-probe for the caller-cancellation oracles
	// (F2): the hook observes DecideExpiry between its blocking rounds.
	onTuple func(db, tbl string)
	onLock  func(db, tbl string)
}

func newStubSeqProber() *stubSeqProber {
	return &stubSeqProber{
		tuples: make(map[string]stubTupleAnswer),
		locks:  make(map[string]stubLockAnswer),
	}
}

func stubProbeKey(db, tbl string) string { return db + "." + tbl }

func (p *stubSeqProber) setTuple(db, tbl string, tuple SeqTuple, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tuples[stubProbeKey(db, tbl)] = stubTupleAnswer{tuple: tuple, err: err}
}

func (p *stubSeqProber) setLock(db, tbl string, acquired bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.locks[stubProbeKey(db, tbl)] = stubLockAnswer{acquired: acquired, err: err}
}

func (p *stubSeqProber) ProbeCommittedTuple(_ context.Context, db, tbl string) (SeqTuple, error) {
	p.mu.Lock()
	p.tupleCalls = append(p.tupleCalls, stubProbeKey(db, tbl))
	ans, ok := p.tuples[stubProbeKey(db, tbl)]
	hook := p.onTuple
	p.mu.Unlock()
	if hook != nil {
		hook(db, tbl)
	}
	if !ok {
		return SeqTuple{}, fmt.Errorf("stub prober: unscripted tuple probe %s", stubProbeKey(db, tbl))
	}
	return ans.tuple, ans.err
}

func (p *stubSeqProber) TryAcquireLock(_ context.Context, db, tbl string) (bool, error) {
	p.mu.Lock()
	p.lockCalls = append(p.lockCalls, stubProbeKey(db, tbl))
	ans, ok := p.locks[stubProbeKey(db, tbl)]
	hook := p.onLock
	p.mu.Unlock()
	if hook != nil {
		hook(db, tbl)
	}
	if !ok {
		return false, fmt.Errorf("stub prober: unscripted lock check %s", stubProbeKey(db, tbl))
	}
	return ans.acquired, ans.err
}

func (p *stubSeqProber) counts() (tupleCalls, lockCalls int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.tupleCalls), len(p.lockCalls)
}

func (p *stubSeqProber) tupleOrder() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.tupleCalls...)
}

// watchEntryForTest exposes the internal watch record for the arming and
// re-baseline oracles.
func watchEntryForTest(st *SequenceRetryState, id WatchIdentity) *watchEntry {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.watch[id.Key()]
}

// decideAt installs the fake clock, admits the window at
// start+interval and positions the clock at the expiry instant.
func decideAt(
	st *SequenceRetryState, clk *fakeClock, prober *stubSeqProber, sessionDeadline time.Time,
) DecideOutcome {
	return st.DecideExpiry(context.Background(), prober, time.Second, seqRetryTestInterval, sessionDeadline)
}

func TestSequenceRetryStateLifecycle(t *testing.T) {
	st := NewSequenceRetryState()
	require.False(t, st.Published())
	_, ok := st.RetryDeadline()
	require.False(t, ok)
	require.Empty(t, st.ConsumePendings())
	require.Nil(t, st.WatchCause("anything"))

	st.MarkPublished()
	require.True(t, st.Published())

	idA := seqTestID("db", "a", 101)
	idB := seqTestID("db", "b", 102)
	namedA := errors.New("sequence db.a: allocation could not acquire")
	causeA := errors.New("lock wait a")
	firstA := &SequenceRetryPending{Original: namedA, Cause: causeA,
		StmtDeadline: time.Now().Add(time.Minute)}
	st.SetPending(idA, firstA)
	namedB := errors.New("sequence db.b: allocation could not acquire")
	causeB := errors.New("lock wait b")
	st.SetPending(idB, &SequenceRetryPending{Original: namedB, Cause: causeB})
	// A repeated conversion on the same identity replaces only that entry
	// and keeps the first-recorded order (B3).
	replacementA := &SequenceRetryPending{Original: namedA, Cause: causeA}
	st.SetPending(idA, replacementA)

	items := st.ConsumePendings()
	require.Len(t, items, 2)
	require.Equal(t, idA, items[0].Identity)
	require.Equal(t, idB, items[1].Identity)
	require.Same(t, replacementA, items[0].Pending)
	require.Empty(t, st.ConsumePendings(), "the pending set is consumed exactly once")
	require.Same(t, causeA, st.WatchCause(idA.Key()), "the residue survives consumption")
	require.Same(t, causeB, st.WatchCause(idB.Key()))

	// The window exists only after the first arbitrated admission and is
	// LIVE only while the watch set is non-empty (Z1).
	window := time.Now().Add(10 * time.Second)
	require.True(t, window.Equal(st.AdmitRetry(window)))
	_, ok = st.RetryDeadline()
	require.False(t, ok, "an admitted window with an empty watch set is not live")
	st.EnsureWatching(idA)
	got, ok := st.RetryDeadline()
	require.True(t, ok)
	require.True(t, window.Equal(got))
	require.False(t, st.AdmitRetry(window.Add(-time.Hour)).Before(window),
		"a later admission cannot move the window backwards")
	require.False(t, st.AdmitRetry(time.Now().Add(time.Hour)).After(window),
		"a later admission cannot extend the window")
}

// Nil receivers are default-deny throughout: background/internal processes
// carry no state and must be safe to probe.
func TestSequenceRetryStateNilSafety(t *testing.T) {
	var st *SequenceRetryState
	st.MarkPublished()
	require.False(t, st.Published())
	st.SetPending(seqTestID("db", "s", 1), &SequenceRetryPending{})
	require.Empty(t, st.ConsumePendings())
	require.Nil(t, st.WatchCause("k"))
	require.Nil(t, st.TerminalCause())
	_, terminal := st.TerminalIdentity()
	require.False(t, terminal)
	_, ok := st.RetryDeadline()
	require.False(t, ok)
	st.EnsureWatching(seqTestID("db", "s", 1))
	st.SetWatchingBaselined(seqTestID("db", "s", 1), SeqTuple{}, time.Now())
	require.False(t, st.HasWatch(seqTestID("db", "s", 1)))
	require.Empty(t, st.WatchIdentities())
	st.RemoveWatch(seqTestID("db", "s", 1))
	st.RemoveWatchByName("db", "s")
	st.RetireDeadline()
	outcome := st.DecideExpiry(context.Background(), newStubSeqProber(), time.Second, seqRetryTestInterval, time.Time{})
	require.Equal(t, DecideProceed, outcome.Kind)

	var proc *Process
	require.Nil(t, proc.GetSequenceRetryState())
	require.Nil(t, (&Process{}).GetSequenceRetryState())
}

// Episode closure (Z1): removing the last watch entry retires the window;
// a later admission is a FRESH first admission carrying its own candidate.
func TestSequenceRetryWatchClosure(t *testing.T) {
	st := NewSequenceRetryState()
	idA := seqTestID("db", "s", 101)
	idAGen2 := seqTestID("db", "s", 202)
	idB := seqTestID("db", "t", 303)

	first := time.Now().Add(10 * time.Second)
	st.AdmitRetry(first)
	st.EnsureWatching(idA)
	st.EnsureWatching(idB)
	st.RemoveWatch(idA)
	got, ok := st.RetryDeadline()
	require.True(t, ok, "one unresolved identity keeps the episode live")
	require.True(t, first.Equal(got))
	st.RemoveWatch(idB)
	_, ok = st.RetryDeadline()
	require.False(t, ok, "emptying the watch set retires the window (Z1)")

	fresh := time.Now().Add(42 * time.Second)
	require.True(t, fresh.Equal(st.AdmitRetry(fresh)),
		"a conflict after closure is a fresh first admission, never the stale window")

	// RemoveWatchByName removes every generation of one sequence name and
	// closes the episode when nothing else is watched.
	st.EnsureWatching(idA)
	st.EnsureWatching(idAGen2)
	st.EnsureWatching(idB)
	require.Equal(t, []WatchIdentity{idA, idAGen2, idB}, st.WatchIdentities())
	st.RemoveWatchByName("db", "s")
	require.Equal(t, []WatchIdentity{idB}, st.WatchIdentities())
	st.RemoveWatchByName("db", "t")
	_, ok = st.RetryDeadline()
	require.False(t, ok)
	require.Empty(t, st.WatchIdentities())
}

// The synchronized shape of the admission terms: concurrent publishers
// (resp_client), converters (allocation wrapper), watch-set churn and the
// arbitrator may only ever observe consistent state. Value assertions stay
// deliberately weak — this test's evidence is the race detector's verdict
// (the ladder runs it with -race and requires zero warnings), which proves
// the state is properly synchronized while ResultRows-style unsynchronized
// reads are absent (F1).
func TestSequenceRetryStateConcurrentAccess(t *testing.T) {
	st := NewSequenceRetryState()
	ids := []WatchIdentity{
		seqTestID("db", "a", 101),
		seqTestID("db", "b", 102),
		seqTestID("db", "c", 103),
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					st.MarkPublished()
					_ = st.Published()
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := ids[i%len(ids)]
			for j := 0; j < 2000; j++ {
				st.SetPending(id, &SequenceRetryPending{Original: errors.New("x"), Cause: errors.New("c")})
				_ = st.Published()
				_ = st.ConsumePendings()
				_ = st.WatchCause(id.Key())
				st.EnsureWatching(id)
				st.SetWatchingBaselined(id, SeqTuple{RelID: id.WantRel, LastSeq: uint64(j), IsCalled: true}, time.Now())
				_ = st.HasWatch(id)
				_ = st.WatchIdentities()
				_ = st.AdmitRetry(time.Now().Add(time.Second))
				_, _ = st.RetryDeadline()
				if j%7 == 0 {
					st.RemoveWatchByName("db", "a")
				}
			}
		}(i)
	}
	for j := 0; j < 2000; j++ {
		_ = st.Published()
		_, _ = st.RetryDeadline()
	}
	close(stop)
	wg.Wait()

	// Deterministic sticky-state assertions once the workers have stopped.
	st.MarkPublished()
	st.EnsureWatching(ids[0])
	window := time.Now().Add(time.Second)
	st.AdmitRetry(window)
	require.True(t, st.Published())
	_, ok := st.RetryDeadline()
	require.True(t, ok)
}

// ---- Shared probe-first expiry decision (B2, A1–A4, Z1/Z2) ----

func decideFixture() (*SequenceRetryState, *fakeClock, *stubSeqProber, time.Time) {
	st := NewSequenceRetryState()
	clk := newFakeClock()
	st.SetClockForTesting(clk.now)
	prober := newStubSeqProber()
	start := clk.now()
	st.AdmitRetry(start.Add(seqRetryTestInterval))
	return st, clk, prober, start
}

// now < D ⇒ proceed, no probe (B2 step 2).
func TestDecideExpiryProceedsBeforeDeadline(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	st.SetWatchingBaselined(id, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)

	clk.set(start.Add(seqRetryTestInterval - time.Second))
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideProceed, outcome.Kind)
	tupleCalls, lockCalls := prober.counts()
	require.Zero(t, tupleCalls)
	require.Zero(t, lockCalls)
}

// An unchanged row that is acquirable again is RESOLVED and removed (A3);
// when the round removes the last entry the episode closes: D retires and
// the decision is Retire, never a renewal with nothing left to observe
// (Z1).
func TestDecideExpiryRetiresWhenAllResolved(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	baseline := SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}
	st.SetWatchingBaselined(id, baseline, start)

	clk.advance(seqRetryTestInterval)
	prober.setTuple("db", "s", baseline, nil)
	prober.setLock("db", "s", true, nil)
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideRetire, outcome.Kind)
	require.Empty(t, st.WatchIdentities())
	_, ok := st.RetryDeadline()
	require.False(t, ok, "episode closure retires the window (Z1)")
}

// An observable committed change re-baselines the entry (B1) and renews
// the window to now+interval; no lock check is needed.
func TestDecideExpiryRenewsOnCommittedChange(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	st.SetWatchingBaselined(id, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)

	clk.advance(seqRetryTestInterval)
	changed := SeqTuple{RelID: 101, LastSeq: 42, IsCalled: true}
	prober.setTuple("db", "s", changed, nil)
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideRenew, outcome.Kind)
	require.True(t, clk.now().Add(seqRetryTestInterval).Equal(outcome.RenewTo))
	entry := watchEntryForTest(st, id)
	require.NotNil(t, entry)
	require.Equal(t, changed, entry.baseline, "the changed tuple becomes the last-observed sample")
	_, lockCalls := prober.counts()
	require.Zero(t, lockCalls, "a changed identity never draws a lock check")

	// The renewed window is live again and sticky.
	got, ok := st.RetryDeadline()
	require.True(t, ok)
	require.True(t, outcome.RenewTo.Equal(got))
}

// Committed-unchanged AND held across one full interval ⇒ the exactly-once
// terminal over the retained identity, with the recorded conflict residue
// as the surface; the stamp re-reports on every later decision with no new
// probe work.
func TestDecideExpiryTerminalsStalledHeldExactlyOnce(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	baseline := SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}
	st.SetWatchingBaselined(id, baseline, start)
	cause := errors.New("lock wait timeout")
	st.SetPending(id, &SequenceRetryPending{Original: errors.New("named"), Cause: cause})
	_ = st.ConsumePendings()

	clk.advance(seqRetryTestInterval)
	prober.setTuple("db", "s", baseline, nil)
	prober.setLock("db", "s", false, errors.New("lock wait timeout"))
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideExhausted, outcome.Kind)
	require.Equal(t, id, outcome.Target)
	require.Same(t, cause, outcome.Terminal)
	stamped, terminal := st.TerminalIdentity()
	require.True(t, terminal)
	require.Equal(t, id, stamped)
	require.Same(t, cause, st.TerminalCause())

	tupleCalls, lockCalls := prober.counts()
	clk.advance(seqRetryTestInterval)
	second := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideExhausted, second.Kind)
	require.Equal(t, id, second.Target)
	tupleAfter, lockAfter := prober.counts()
	require.Equal(t, tupleCalls, tupleAfter, "the stamped terminal re-reports without new probes")
	require.Equal(t, lockCalls, lockAfter)
}

// A failed or timed-out tuple probe of a TERMINAL-ELIGIBLE identity means
// that identity persists committed-unchanged AND unavailable (A3 fail-safe)
// ⇒ D expires with no need for a lock check.
func TestDecideExpiryFailedProbeTerminalsEligible(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	st.SetWatchingBaselined(id, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)
	cause := errors.New("lock wait timeout")
	st.SetPending(id, &SequenceRetryPending{Original: errors.New("named"), Cause: cause})
	_ = st.ConsumePendings()

	clk.advance(seqRetryTestInterval)
	prober.setTuple("db", "s", SeqTuple{}, errors.New("snapshot read failed"))
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideExhausted, outcome.Kind,
		"a terminal-eligible identity whose tuple probe fails is committed-unchanged AND unavailable")
	require.Equal(t, id, outcome.Target)
	require.Same(t, cause, outcome.Terminal)
	_, lockCalls := prober.counts()
	require.Zero(t, lockCalls, "the probe failure is itself the unavailability evidence")
}

// A failed probe of a YOUNG baselined identity (less than one full
// interval from its own baseline) votes neither terminal nor renewal
// (Z2): the decision rides to a single renewal.
func TestDecideExpiryFailedProbeYoungRides(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	// The identity joins at D-epsilon carrying its creation baseline.
	clk.set(start.Add(seqRetryTestInterval - time.Second))
	joined := clk.now()
	st.SetWatchingBaselined(id, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, joined)

	clk.advance(time.Second) // D
	prober.setTuple("db", "s", SeqTuple{}, errors.New("snapshot read failed"))
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideRenew, outcome.Kind,
		"a young identity neither terminals nor renews, it rides")
	_, lockCalls := prober.counts()
	require.Zero(t, lockCalls)
}

// The initialization-failure arm (Z2): a failed creation sample records the
// entry YOUNG; while it is less than one full interval old a still-failing
// re-sample only rides the renewal; still un-baselined one full interval
// after its creation it fails safe to the exactly-once terminal.
func TestDecideExpiryNeverBaselinedFailSafe(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	clk.set(start.Add(seqRetryTestInterval - time.Second))
	st.EnsureWatching(id) // the failed creation sample: young entry at D-epsilon

	clk.advance(time.Second) // D
	prober.setTuple("db", "s", SeqTuple{}, errors.New("snapshot read failed"))
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideRenew, outcome.Kind,
		"a young un-baselined entry votes neither terminal nor renewal")
	require.NotNil(t, watchEntryForTest(st, id))
	require.False(t, watchEntryForTest(st, id).baselined, "the failed re-sample leaves it un-baselined")

	clk.advance(seqRetryTestInterval) // one full interval from creation
	outcome = decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideExhausted, outcome.Kind,
		"un-baselined for one full interval from creation fails safe to the terminal")
	require.Equal(t, id, outcome.Target)
	_, lockCalls := prober.counts()
	require.Zero(t, lockCalls, "the persisted unavailability needs no lock check")
}

// A successful re-sample inside the decision's own round arms the young
// entry at its first observation (Z2): it starts terminal-eligibility from
// ITS OWN baseline, rides this renewal, and terminals one full interval
// after that arming when observed unchanged AND held.
func TestDecideExpiryYoungArmsInsideRound(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	clk.set(start.Add(seqRetryTestInterval - time.Second))
	st.EnsureWatching(id)

	clk.advance(time.Second) // D
	armed := clk.now()
	sample := SeqTuple{RelID: 101, LastSeq: 9, IsCalled: true}
	prober.setTuple("db", "s", sample, nil)
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideRenew, outcome.Kind, "an only-young set renews once")
	entry := watchEntryForTest(st, id)
	require.NotNil(t, entry)
	require.True(t, entry.baselined, "the re-sample arms the baseline inside the round")
	require.Equal(t, sample, entry.baseline)
	require.True(t, armed.Equal(entry.baselineAt))
	_, lockCalls := prober.counts()
	require.Zero(t, lockCalls)

	// One full interval after ITS OWN arming, unchanged AND held ⇒ exactly
	// one terminal — the eligibility clock runs from the identity's own
	// baseline, never from D or from the entry's creation.
	clk.advance(seqRetryTestInterval)
	prober.setLock("db", "s", false, errors.New("lock wait timeout"))
	outcome = decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideExhausted, outcome.Kind)
	require.Equal(t, id, outcome.Target)
}

// The D-epsilon late join (Z2): the first decision for a fresh baseline is
// a renewal even when the tuple reads unchanged — youth is not a full
// no-progress interval — and the identity terminals exactly once one full
// interval after ITS baseline while it stays unchanged AND held.
func TestDecideExpiryLateJoinStaticHeldLifecycle(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	clk.set(start.Add(seqRetryTestInterval - time.Second))
	joined := clk.now()
	baseline := SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}
	st.SetWatchingBaselined(id, baseline, joined)

	clk.advance(time.Second) // D; age is one second
	prober.setTuple("db", "s", baseline, nil)
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideRenew, outcome.Kind, "a D-epsilon join rides the renewal, never terminals")
	_, lockCalls := prober.counts()
	require.Zero(t, lockCalls, "a young identity never draws a lock check")

	clk.advance(seqRetryTestInterval) // one full interval from the join baseline
	prober.setLock("db", "s", false, errors.New("lock wait timeout"))
	outcome = decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideExhausted, outcome.Kind)
	require.Equal(t, id, outcome.Target)
}

// The cannot-renew-A control (Z2): a young identity's initialization while
// a terminal-eligible co-identity is stalled-and-held neither renews the
// window nor delays the eligible identity's exactly-once terminal.
func TestDecideExpiryLateJoinCannotRenewStalledPeer(t *testing.T) {
	st, clk, prober, start := decideFixture()
	idA := seqTestID("db", "a", 101)
	idB := seqTestID("db", "b", 102)
	baselineA := SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}
	st.SetWatchingBaselined(idA, baselineA, start)
	// B joins at D-epsilon with its creation baseline.
	clk.set(start.Add(seqRetryTestInterval - time.Second))
	st.SetWatchingBaselined(idB, SeqTuple{RelID: 102, LastSeq: 3, IsCalled: true}, clk.now())

	clk.advance(time.Second)                                                          // D
	prober.setTuple("db", "a", baselineA, nil)                                        // unchanged
	prober.setTuple("db", "b", SeqTuple{RelID: 102, LastSeq: 3, IsCalled: true}, nil) // unchanged but young
	prober.setLock("db", "a", false, errors.New("lock wait timeout"))
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideExhausted, outcome.Kind,
		"B's initialization neither renews nor delays A's terminal")
	require.Equal(t, idA, outcome.Target, "first-recorded order selects the surfaced identity")
	require.Equal(t, []WatchIdentity{idA, idB}, st.WatchIdentities(), "B keeps riding the episode")
	_, lockCalls := prober.counts()
	require.Equal(t, 1, lockCalls, "only the eligible identity draws a lock check")
}

// Conjunctive renewal (B3): every terminal-eligible identity must
// observably change or resolve for the window to renew.
func TestDecideExpiryConjunctiveRenewal(t *testing.T) {
	newPair := func() (*SequenceRetryState, *fakeClock, *stubSeqProber, WatchIdentity, WatchIdentity) {
		st, clk, prober, start := decideFixture()
		idA := seqTestID("db", "a", 101)
		idB := seqTestID("db", "b", 102)
		st.SetWatchingBaselined(idA, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)
		st.SetWatchingBaselined(idB, SeqTuple{RelID: 102, LastSeq: 3, IsCalled: true}, start)
		clk.advance(seqRetryTestInterval)
		return st, clk, prober, idA, idB
	}

	t.Run("both changed renew", func(t *testing.T) {
		st, clk, prober, _, _ := newPair()
		prober.setTuple("db", "a", SeqTuple{RelID: 101, LastSeq: 8, IsCalled: true}, nil)
		prober.setTuple("db", "b", SeqTuple{RelID: 102, LastSeq: 4, IsCalled: true}, nil)
		outcome := decideAt(st, clk, prober, time.Time{})
		require.Equal(t, DecideRenew, outcome.Kind)
	})

	t.Run("changed plus resolved renews", func(t *testing.T) {
		st, clk, prober, idB, _ := newPair()
		prober.setTuple("db", "a", SeqTuple{RelID: 101, LastSeq: 8, IsCalled: true}, nil)
		prober.setTuple("db", "b", SeqTuple{RelID: 102, LastSeq: 3, IsCalled: true}, nil)
		prober.setLock("db", "b", true, nil)
		outcome := decideAt(st, clk, prober, time.Time{})
		require.Equal(t, DecideRenew, outcome.Kind)
		require.Equal(t, []WatchIdentity{seqTestID("db", "a", 101)}, st.WatchIdentities(),
			"the resolved identity leaves the set; the changed one renews")
		_ = idB
	})

	t.Run("one stalled held identity blocks renewal", func(t *testing.T) {
		st, clk, prober, _, idB := newPair()
		prober.setTuple("db", "a", SeqTuple{RelID: 101, LastSeq: 8, IsCalled: true}, nil)
		prober.setTuple("db", "b", SeqTuple{RelID: 102, LastSeq: 3, IsCalled: true}, nil)
		prober.setLock("db", "b", false, errors.New("lock wait timeout"))
		outcome := decideAt(st, clk, prober, time.Time{})
		require.Equal(t, DecideExhausted, outcome.Kind,
			"A's observable change cannot renew while B is stalled AND held")
		require.Equal(t, idB, outcome.Target)
	})

	t.Run("first recorded order selects the terminal", func(t *testing.T) {
		st, clk, prober, idA, _ := newPair()
		prober.setTuple("db", "a", SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, nil)
		prober.setTuple("db", "b", SeqTuple{RelID: 102, LastSeq: 3, IsCalled: true}, nil)
		prober.setLock("db", "a", false, errors.New("lock wait timeout"))
		prober.setLock("db", "b", false, errors.New("lock wait timeout"))
		outcome := decideAt(st, clk, prober, time.Time{})
		require.Equal(t, DecideExhausted, outcome.Kind)
		require.Equal(t, idA, outcome.Target)
		require.Equal(t, []string{"db.a", "db.b"}, prober.tupleOrder(),
			"the round probes baselined identities in first-recorded order")
	})
}

// Successive-generation exhaustion: generation 1 contends A, generation 2
// contends B — every later decision probes the whole retained set
// conjunctively, and a young generation-2 entry never saves a stalled
// generation-1 identity.
func TestDecideExpirySuccessiveGenerations(t *testing.T) {
	st, clk, prober, start := decideFixture()
	idA := seqTestID("db", "a", 101)
	idB := seqTestID("db", "b", 102)
	st.SetWatchingBaselined(idA, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)

	// First expiry of generation 1: A advances.
	clk.advance(seqRetryTestInterval)
	prober.setTuple("db", "a", SeqTuple{RelID: 101, LastSeq: 9, IsCalled: true}, nil)
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideRenew, outcome.Kind)

	// Generation 2 joins B young at the renewed D-epsilon; the next
	// decision probes {A, B} conjunctively with B re-checked... B carries
	// its creation baseline, so the round order is first-recorded.
	clk.advance(seqRetryTestInterval - time.Second)
	st.SetWatchingBaselined(idB, SeqTuple{RelID: 102, LastSeq: 3, IsCalled: true}, clk.now())
	clk.advance(time.Second)
	prober.setTuple("db", "a", SeqTuple{RelID: 101, LastSeq: 10, IsCalled: true}, nil)
	prober.setTuple("db", "b", SeqTuple{RelID: 102, LastSeq: 4, IsCalled: true}, nil)
	outcome = decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideRenew, outcome.Kind)
	require.Equal(t, []string{"db.a", "db.b"}, prober.tupleOrder()[1:],
		"the round covers the whole retained set")

	// A stalls AND stays held while B simply rides: the terminal targets A.
	clk.advance(seqRetryTestInterval)
	prober.setTuple("db", "a", SeqTuple{RelID: 101, LastSeq: 10, IsCalled: true}, nil)
	prober.setTuple("db", "b", SeqTuple{RelID: 102, LastSeq: 5, IsCalled: true}, nil)
	prober.setLock("db", "a", false, errors.New("lock wait timeout"))
	outcome = decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideExhausted, outcome.Kind)
	require.Equal(t, idA, outcome.Target)
}

// A young entry is re-sampled FIRST inside the decision's round (Z2
// initialization retry), ahead of the baselined first-recorded order.
func TestDecideExpiryYoungResampledFirst(t *testing.T) {
	st, clk, prober, start := decideFixture()
	idA := seqTestID("db", "a", 101)
	idB := seqTestID("db", "b", 102)
	st.SetWatchingBaselined(idA, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)
	clk.set(start.Add(seqRetryTestInterval - time.Second))
	st.EnsureWatching(idB)

	clk.advance(time.Second)
	prober.setTuple("db", "a", SeqTuple{RelID: 101, LastSeq: 8, IsCalled: true}, nil)
	prober.setTuple("db", "b", SeqTuple{RelID: 102, LastSeq: 3, IsCalled: true}, nil)
	outcome := decideAt(st, clk, prober, time.Time{})
	require.Equal(t, DecideRenew, outcome.Kind)
	require.Equal(t, []string{"db.b", "db.a"}, prober.tupleOrder(),
		"young entries are re-sampled first inside the same round")
	require.True(t, watchEntryForTest(st, idB).baselined, "the young entry armed at its first observation")
}

// Retirement (A1): an identity that no longer resolves leaves the watch
// set — the statement's own next allocation surfaces the deterministic
// not-found error, so the removal cannot mask a usable sequence.
func TestDecideExpiryRetiresGoneIdentity(t *testing.T) {
	t.Run("gone last entry closes the episode", func(t *testing.T) {
		st, clk, prober, start := decideFixture()
		id := seqTestID("db", "s", 101)
		st.SetWatchingBaselined(id, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)
		clk.advance(seqRetryTestInterval)
		prober.setTuple("db", "s", SeqTuple{}, SeqProbeIdentityGoneError("db", "s"))
		outcome := decideAt(st, clk, prober, time.Time{})
		require.Equal(t, DecideRetire, outcome.Kind)
		require.Empty(t, st.WatchIdentities())
	})

	t.Run("gone co-identity never blocks renewal", func(t *testing.T) {
		st, clk, prober, start := decideFixture()
		idA := seqTestID("db", "a", 101)
		idB := seqTestID("db", "b", 102)
		st.SetWatchingBaselined(idA, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)
		st.SetWatchingBaselined(idB, SeqTuple{RelID: 102, LastSeq: 3, IsCalled: true}, start)
		clk.advance(seqRetryTestInterval)
		prober.setTuple("db", "a", SeqTuple{}, SeqProbeIdentityGoneError("db", "a"))
		prober.setTuple("db", "b", SeqTuple{RelID: 102, LastSeq: 5, IsCalled: true}, nil)
		outcome := decideAt(st, clk, prober, time.Time{})
		require.Equal(t, DecideRenew, outcome.Kind)
		require.Equal(t, []WatchIdentity{idB}, st.WatchIdentities())
	})
}

// Caller-context cancellation propagates first — existing precedence —
// ahead of every other surface including a stamped terminal.
func TestDecideExpiryCallerCancellationFirst(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	st.SetWatchingBaselined(id, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)
	clk.advance(seqRetryTestInterval)
	prober.setTuple("db", "s", SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, nil)
	prober.setLock("db", "s", false, errors.New("held"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	outcome := st.DecideExpiry(ctx, prober, time.Second, seqRetryTestInterval, time.Time{})
	require.Equal(t, DecideCancelled, outcome.Kind)
	require.Error(t, outcome.Err)
	tupleCalls, lockCalls := prober.counts()
	require.Zero(t, tupleCalls)
	require.Zero(t, lockCalls)
}

// The caller's own session ceiling ends the decision without new probe
// work, surfacing the first-recorded identity with its recorded residue.
func TestDecideExpirySessionCeiling(t *testing.T) {
	st, clk, prober, start := decideFixture()
	idA := seqTestID("db", "a", 101)
	st.SetWatchingBaselined(idA, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)
	cause := errors.New("lock wait timeout")
	st.SetPending(idA, &SequenceRetryPending{Original: errors.New("named"), Cause: cause})
	_ = st.ConsumePendings()

	clk.advance(seqRetryTestInterval)
	outcome := decideAt(st, clk, prober, clk.now())
	require.Equal(t, DecideExhausted, outcome.Kind)
	require.Equal(t, idA, outcome.Target)
	require.Same(t, cause, outcome.Terminal)
	tupleCalls, lockCalls := prober.counts()
	require.Zero(t, tupleCalls, "no new probe work starts at the caller's ceiling")
	require.Zero(t, lockCalls)
}

// Renewal is capped at the session statement deadline.
func TestDecideExpiryRenewCapsAtSessionDeadline(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	st.SetWatchingBaselined(id, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)

	clk.advance(seqRetryTestInterval)
	prober.setTuple("db", "s", SeqTuple{RelID: 101, LastSeq: 8, IsCalled: true}, nil)
	ceiling := clk.now().Add(3 * time.Second)
	outcome := decideAt(st, clk, prober, ceiling)
	require.Equal(t, DecideRenew, outcome.Kind)
	require.True(t, ceiling.Equal(outcome.RenewTo), "renewal never extends past the session deadline")
}

// A state without an admission decides Proceed (no live window to decide
// on); a state whose deadline was retired decides Proceed as well.
func TestDecideExpiryNoLiveWindow(t *testing.T) {
	st := NewSequenceRetryState()
	outcome := st.DecideExpiry(context.Background(), newStubSeqProber(), time.Second, seqRetryTestInterval, time.Time{})
	require.Equal(t, DecideProceed, outcome.Kind)

	st, _, prober, _ := decideFixture()
	st.RetireDeadline()
	outcome = st.DecideExpiry(context.Background(), prober, time.Second, seqRetryTestInterval, time.Time{})
	require.Equal(t, DecideProceed, outcome.Kind)
	tupleCalls, lockCalls := prober.counts()
	require.Zero(t, tupleCalls)
	require.Zero(t, lockCalls)
}

// WatchIdentity keys: distinct generations of one name hold distinct keys;
// the zero relation id is the unkeyed caller that never records watch
// state (merge reconcile).
func TestWatchIdentityKey(t *testing.T) {
	require.Empty(t, seqTestID("db", "s", 0).Key())
	require.NotEqual(t, seqTestID("db", "s", 101).Key(), seqTestID("db", "s", 202).Key())
	require.True(t, errors.Is(SeqProbeIdentityGoneError("db", "s"), ErrSeqProbeIdentityGone))
}

// ---- F2: caller cancellation precedence inside the probe rounds ----

// A caller cancellation arriving MID tuple-probe round outranks every
// recorded observation: the cancelled parent also cancelled the derived
// probe context, so the round's failures may be the caller's own
// cancellation — classifying them as identity unavailability would
// mis-stamp the exactly-once terminal (v21 cancellation precedence).
func TestDecideExpiryCallerCancellationMidTupleProbe(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	st.SetWatchingBaselined(id, SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}, start)

	clk.advance(seqRetryTestInterval) // window elapsed; identity terminal-eligible
	// The derived probe context died with the parent: the tuple read
	// reports a budget-shaped failure.
	prober.setTuple("db", "s", SeqTuple{}, context.DeadlineExceeded)
	callerCtx, cancel := context.WithCancel(context.Background())
	prober.onTuple = func(_, _ string) { cancel() }

	outcome := st.DecideExpiry(callerCtx, prober, time.Second, seqRetryTestInterval, time.Time{})
	require.Equal(t, DecideCancelled, outcome.Kind)
	require.ErrorIs(t, outcome.Err, context.Canceled)
	_, terminal := st.TerminalIdentity()
	require.False(t, terminal, "a cancelled decision never stamps the exactly-once terminal")
}

// A caller cancellation arriving MID lock-acquisition check outranks the
// held-row classification the same way.
func TestDecideExpiryCallerCancellationMidLockCheck(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	baseline := SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}
	st.SetWatchingBaselined(id, baseline, start)

	clk.advance(seqRetryTestInterval)
	prober.setTuple("db", "s", baseline, nil) // committed-unchanged: the check runs
	prober.setLock("db", "s", false, context.DeadlineExceeded)
	callerCtx, cancel := context.WithCancel(context.Background())
	prober.onLock = func(_, _ string) { cancel() }

	outcome := st.DecideExpiry(callerCtx, prober, time.Second, seqRetryTestInterval, time.Time{})
	require.Equal(t, DecideCancelled, outcome.Kind)
	require.ErrorIs(t, outcome.Err, context.Canceled)
	_, terminal := st.TerminalIdentity()
	require.False(t, terminal, "a cancelled decision never stamps the exactly-once terminal")
}

// Control: the probe round's OWN budget expiry with a live caller keeps
// its committed-unchanged AND unavailable meaning (A3 fail-safe terminal);
// only the caller's cancellation takes precedence.
func TestDecideExpiryProbeBudgetExpiryKeepsHeldMeaning(t *testing.T) {
	st, clk, prober, start := decideFixture()
	id := seqTestID("db", "s", 101)
	baseline := SeqTuple{RelID: 101, LastSeq: 7, IsCalled: true}
	st.SetWatchingBaselined(id, baseline, start)

	clk.advance(seqRetryTestInterval)
	prober.setTuple("db", "s", baseline, nil)
	prober.setLock("db", "s", false, context.DeadlineExceeded) // budget-shaped failure, caller live
	outcome := st.DecideExpiry(context.Background(), prober, time.Second, seqRetryTestInterval, time.Time{})
	require.Equal(t, DecideExhausted, outcome.Kind)
	require.Equal(t, id, outcome.Target)
	_, terminal := st.TerminalIdentity()
	require.True(t, terminal, "a live caller keeps the fail-safe terminal")
}
