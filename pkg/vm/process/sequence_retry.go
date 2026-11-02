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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ErrSeqProbeIdentityGone marks the tuple-probe failure whose root is the
// identity no longer resolving in committed catalog state (a committed drop
// without a committed recreation). The shared expiry decision retires such
// entries (design §4 D, A1): the statement's own next allocation surfaces
// the deterministic not-found error, so the removal cannot mask a usable
// sequence. Every other probe failure keeps the entry unavailable.
var ErrSeqProbeIdentityGone = moerr.NewInternalErrorNoCtx("sequence identity no longer resolves")

// SeqProbeIdentityGoneError composes the prober's identity-gone failure for
// one watch identity; the decision matches it with errors.Is.
func SeqProbeIdentityGoneError(db, tbl string) error {
	return &seqProbeIdentityGoneError{db: db, tbl: tbl}
}

// seqProbeIdentityGoneError carries the unresolved identity on the
// sentinel's message; Unwrap keeps errors.Is(err, ErrSeqProbeIdentityGone)
// true across the executor's error join. A plain moerr composition cannot
// do that: moerr Error.Is matches only the two context-cancellation codes.
type seqProbeIdentityGoneError struct {
	db, tbl string
}

func (e *seqProbeIdentityGoneError) Error() string {
	return fmt.Sprintf("%s: %s.%s", ErrSeqProbeIdentityGone, e.db, e.tbl)
}

func (e *seqProbeIdentityGoneError) Unwrap() error {
	return ErrSeqProbeIdentityGone
}

// WatchIdentity is one contended sequence identity recorded by the retry
// state (design §4 D, A1/B3): the caller-resolved (relation id, generation)
// pair plus the namespace the probe needs to re-resolve it. Watch keys are
// relation ids — distinct generations have distinct relation ids, and one
// sequence name holds at most a bounded handful of generations per
// statement. WantRel of zero marks an unkeyed caller (merge reconcile) that
// never records watch state.
type WatchIdentity struct {
	DBName  string
	TblName string
	WantRel uint64
}

// Key is the watch-set map key: the relation id alone, unique across
// generations and namespaces for one statement's lifetime.
func (w WatchIdentity) Key() string {
	if w.WantRel == 0 {
		return ""
	}
	return fmt.Sprintf("%x", w.WantRel)
}

// SeqTuple is the B1 progress marker sample: the complete committed tuple
// (relation id, is_called, last_seq_num) read through the autonomous RC
// snapshot. Values normalize to the unsigned bit pattern via the same
// domain-aware conversion the allocation route uses, so equality is exact
// for every sequence numeric type.
type SeqTuple struct {
	RelID    uint64
	LastSeq  uint64
	IsCalled bool
}

// equalByValue implements the B1 marker comparison: an identical tuple is
// no-progress (the complete-cycle ABA shape is an accepted quantization
// limit), any deviation is an observable change (direction-consistent
// emission or reconfiguration — re-baseline and renew once).
func (t SeqTuple) equalByValue(other SeqTuple) bool {
	return t == other
}

// SequenceRetryPending is one converted tier-1 allocation exhaustion awaiting
// arbitration at the compiler's retry quiescence boundary (design
// docs/design/sequence_ownership_state_consistency.md §4 D, v13+; B3 per-
// identity payload map, v19). Original is the named terminal error the
// client must receive exactly once when the retry is declined; StmtDeadline
// is the statement deadline observed at the conversion site (zero when the
// statement context carries none); Cause is the transient-conflict residue
// that exhausted the tier-1 wrapper, kept to compose the message of a later
// zero-attempt or decision-terminal surface.
type SequenceRetryPending struct {
	Original     error
	Cause        error
	StmtDeadline time.Time
}

// SequencePendingItem is one pending payload with its contended identity,
// returned in stable first-recorded order (B3).
type SequencePendingItem struct {
	Identity WatchIdentity
	Pending  *SequenceRetryPending
}

// watchEntry is one retained contention-set record (A1): created the first
// time the statement contends the identity, CARRYING its baseline sample at
// creation (Z2) or recorded YOUNG when the creation sample failed; removed
// exactly when its contention observably ends for this statement — the
// statement's own committed allocation, the A3 availability resolution, or
// retirement (the identity no longer resolves).
type watchEntry struct {
	id         WatchIdentity
	baseline   SeqTuple
	baselineAt time.Time
	createdAt  time.Time
	baselined  bool
}

// SequenceRetryState is the per-statement tier-2 allocation-retry state
// (design §4 D, v13–v21):
//
//   - published — the synchronized (atomic) publication flag BOTH admission
//     phases read; set exactly once by the single publication path
//     (frontend resp_client) before the first result batch's bytes are
//     written. SessionInfo.ResultRows is never consulted (v16, F1).
//   - deadline (D) — the progress-renewed retry window, created LAZILY at
//     the statement's first arbitrated admission, renewed only by the
//     shared probe-first expiry decision (B2), RETIRED exactly when the
//     watch set becomes empty (episode closure, Z1).
//   - watch — the retained contention set (A1), one entry per contended
//     identity in first-recorded order; never consumed at arbitration and
//     surviving retry rebuilds and generation replacement.
//   - pendings — the per-identity converted exhaustion payloads (B3),
//     consumed by arbitration as a unit; a repeated conversion on the same
//     identity replaces only that entry.
//   - terminal — the exactly-once terminal verdict stamp (B2 step 5):
//     further conversion is forbidden and every later decision re-reports
//     the stamped identity.
//
// One instance is installed per statement execution (compile NewCompile and
// the pooled Reset) and shared with every pipeline process through
// SessionInfo (children share Base), which is what lets the allocation
// wrapper (plan function layer), the publication path (frontend) and the
// arbitration (compile2.go) observe the same flag, window and watch set. A
// nil state — background or internal executors, view-binding children,
// remote-rebuilt processes — is the design's default-deny: no conversion,
// no arbitration.
type SequenceRetryState struct {
	published atomic.Bool
	mu        sync.Mutex

	deadline    time.Time
	deadlineSet bool

	terminal    bool
	terminalID  WatchIdentity
	terminalErr error

	watch map[string]*watchEntry
	order []string

	pendings map[string]*SequenceRetryPending
	// pendingIDs records the identity of each pending key (pendings are
	// consumed without the watch entries, and repeated conversions keep the
	// first-recorded order).
	pendingIDs   map[string]WatchIdentity
	pendingOrder []string
	// causes keeps the last recorded conflict residue per watch key for
	// terminal-surface composition; it survives pending consumption exactly
	// like the v15 single-slot residue did.
	causes map[string]error

	now func() time.Time
}

// NewSequenceRetryState returns a fresh per-statement state: publication
// unset, no retry window, empty watch and pending sets.
func NewSequenceRetryState() *SequenceRetryState {
	return &SequenceRetryState{
		watch:      make(map[string]*watchEntry),
		pendings:   make(map[string]*SequenceRetryPending),
		pendingIDs: make(map[string]WatchIdentity),
		causes:     make(map[string]error),
	}
}

// SetClockForTesting installs a deterministic clock for the ageing and
// renewal arithmetic; production code never calls it. Passing nil restores
// the wall clock.
func (st *SequenceRetryState) SetClockForTesting(now func() time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.now = now
}

func (st *SequenceRetryState) nowT() time.Time {
	if st.now != nil {
		return st.now()
	}
	return time.Now()
}

// MarkPublished records that the statement's first result batch entered the
// client write path. Publication always wins over retry (G1).
func (st *SequenceRetryState) MarkPublished() {
	if st == nil {
		return
	}
	st.published.Store(true)
}

// Published reports whether any result batch has entered the client write
// path for this statement. Both admission phases read ONLY this flag (F1).
func (st *SequenceRetryState) Published() bool {
	if st == nil {
		return false
	}
	return st.published.Load()
}

// SetPending records one converted exhaustion payload for its identity
// (B3): a repeated conversion on the same identity replaces only that
// entry's payload, keeping the first-recorded order. It never touches the
// watch set: entry creation happens through SetWatchingBaselined (later
// conversions, Z2) or the arbitration admission probe (first admission).
func (st *SequenceRetryState) SetPending(id WatchIdentity, pending *SequenceRetryPending) {
	if st == nil || pending == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	key := id.Key()
	if _, exists := st.pendings[key]; !exists {
		st.pendingOrder = append(st.pendingOrder, key)
	}
	st.pendings[key] = pending
	st.pendingIDs[key] = id
	if pending.Cause != nil {
		st.causes[key] = pending.Cause
	}
}

// ConsumePendings drains the whole pending payload map as a unit in
// first-recorded order (B3): arbitration answers for the set once, and the
// verdict is final for that exhaustion event. The retained watch set and
// the per-identity causes are not consumed.
func (st *SequenceRetryState) ConsumePendings() []SequencePendingItem {
	if st == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	items := make([]SequencePendingItem, 0, len(st.pendingOrder))
	for _, key := range st.pendingOrder {
		if pending, ok := st.pendings[key]; ok {
			items = append(items, SequencePendingItem{Identity: st.pendingIDs[key], Pending: pending})
		}
	}
	st.pendings = make(map[string]*SequenceRetryPending)
	st.pendingIDs = make(map[string]WatchIdentity)
	st.pendingOrder = nil
	return items
}

// ensureWatchLocked creates the young entry for id when absent (Z2: the
// failed-creation-sample shape; its initialization is retried first inside
// the next expiry decision's single probe round).
func (st *SequenceRetryState) ensureWatchLocked(id WatchIdentity, at time.Time) *watchEntry {
	key := id.Key()
	if e, ok := st.watch[key]; ok {
		return e
	}
	e := &watchEntry{id: id, createdAt: at}
	st.watch[key] = e
	st.order = append(st.order, key)
	return e
}

// SetWatchingBaselined records a later conversion's creation-time baseline
// (Z2): taken BEFORE the entry is recorded, it arms the entry the first
// time this identity contends. An existing entry is left alone — its
// baseline is the decision's last-observed sample.
func (st *SequenceRetryState) SetWatchingBaselined(id WatchIdentity, tuple SeqTuple, at time.Time) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	key := id.Key()
	if e, ok := st.watch[key]; ok {
		if !e.baselined {
			e.baseline, e.baselineAt, e.baselined = tuple, at, true
		}
		return
	}
	e := &watchEntry{id: id, baseline: tuple, baselineAt: at, createdAt: at, baselined: true}
	st.watch[key] = e
	st.order = append(st.order, key)
}

// EnsureWatching records a young entry for id without a baseline: the
// creation-sample-failed arm (Z2) and the arbitration fallback when the
// admission probe cannot sample the identity.
func (st *SequenceRetryState) EnsureWatching(id WatchIdentity) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.ensureWatchLocked(id, st.nowT())
}

// HasWatch reports whether the identity already holds a watch entry.
func (st *SequenceRetryState) HasWatch(id WatchIdentity) bool {
	if st == nil {
		return false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	_, ok := st.watch[id.Key()]
	return ok
}

// RecordBaseline arms the baseline of an existing young entry, or creates
// the entry carrying this baseline (the first-admission probe of the
// arbitration seam). An already-baselined entry keeps its last-observed
// sample.
func (st *SequenceRetryState) RecordBaseline(id WatchIdentity, tuple SeqTuple, at time.Time) {
	st.SetWatchingBaselined(id, tuple, at)
}

// RemoveWatch drops the identity's entry — contention observably ended for
// this statement (its own committed allocation, or the A3 availability
// resolution). When the removal empties the watch set the episode closes
// (Z1): D retires and a later conflict is a fresh first admission.
func (st *SequenceRetryState) RemoveWatch(id WatchIdentity) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.removeWatchLocked(id.Key())
}

// RemoveWatchByName drops every entry of one sequence name regardless of
// generation: the statement's own committed allocation resolved the
// sequence name fresh, so a generation that moved mid-episode re-keys
// through the B1 marker rule while the removal still covers both slots.
func (st *SequenceRetryState) RemoveWatchByName(db, tbl string) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, key := range append([]string(nil), st.order...) {
		if e, ok := st.watch[key]; ok && e.id.DBName == db && e.id.TblName == tbl {
			st.removeWatchLocked(key)
		}
	}
}

func (st *SequenceRetryState) removeWatchLocked(key string) {
	if _, ok := st.watch[key]; !ok {
		return
	}
	delete(st.watch, key)
	for i, k := range st.order {
		if k == key {
			st.order = append(st.order[:i], st.order[i+1:]...)
			break
		}
	}
	if len(st.watch) == 0 {
		// Episode closure (Z1): the conflict-replay envelope the deadline
		// funded no longer exists, so the statement returns to its base
		// per-invocation budget and the next conflict re-admits fresh.
		st.deadline, st.deadlineSet = time.Time{}, false
	}
}

// WatchIdentities returns the current watch identities in first-recorded
// order (a defensive snapshot for diagnostics and the admission probe).
func (st *SequenceRetryState) WatchIdentities() []WatchIdentity {
	if st == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	ids := make([]WatchIdentity, 0, len(st.order))
	for _, key := range st.order {
		if e, ok := st.watch[key]; ok {
			ids = append(ids, e.id)
		}
	}
	return ids
}

// WatchCause returns the last recorded conflict residue for one watch key,
// used to compose the exactly-once terminal surface.
func (st *SequenceRetryState) WatchCause(key string) error {
	if st == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.causes[key]
}

// TerminalIdentity reports whether the exactly-once terminal verdict is
// stamped, and the identity whose committed-unchanged AND held observation
// produced it (B2 step 5; first-recorded order).
func (st *SequenceRetryState) TerminalIdentity() (WatchIdentity, bool) {
	if st == nil {
		return WatchIdentity{}, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.terminalID, st.terminal
}

// TerminalCause returns the recorded conflict residue of the stamped
// terminal identity for surface composition.
func (st *SequenceRetryState) TerminalCause() error {
	if st == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.terminalErr
}

// RetryDeadline returns the statement's live retry window and whether it
// exists — false until the statement's first arbitrated admission, and
// false again once the episode has closed (the watch set is empty, Z1).
func (st *SequenceRetryState) RetryDeadline() (time.Time, bool) {
	if st == nil {
		return time.Time{}, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.deadline, st.deadlineSet && len(st.watch) > 0
}

// AdmitRetry establishes the retry window at the statement's FIRST
// arbitrated admission and returns the effective window: the candidate on
// first admission (or on the fresh admission after an episode closure, Z1),
// the stored window while an episode is live — the live window never resets
// across a statement's retries (G2), it only renews through the expiry
// decision (B2) or retires at closure (Z1).
func (st *SequenceRetryState) AdmitRetry(candidate time.Time) time.Time {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.deadlineSet {
		st.deadline, st.deadlineSet = candidate, true
	}
	return st.deadline
}

// RetireDeadline drops the window unconditionally (the defensive residual
// of episode closure when the watch set is observed empty at a decision).
func (st *SequenceRetryState) RetireDeadline() {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.deadline, st.deadlineSet = time.Time{}, false
}

// DecideKind classifies the shared probe-first expiry decision (B2).
type DecideKind int

const (
	// DecideProceed leaves the statement on its current course: now < D
	// (B2 step 2), or there was no live window to decide on.
	DecideProceed DecideKind = iota
	// DecideRenew renewed the window to RenewTo: every terminal-eligible
	// retained identity either observably changed or resolved, with young
	// identities riding without a vote (B2 step 6, B3 conjunctive, Z2).
	DecideRenew
	// DecideRetire closed the episode: the round removed the last watch
	// entry, or the set was already empty at the decision (Z1). D is
	// retired and the statement proceeds attempt-first under its base
	// per-invocation budget; a later conflict is a fresh first admission.
	DecideRetire
	// DecideExhausted is the exactly-once terminal verdict: some
	// terminal-eligible retained identity is committed-unchanged AND held
	// (A3), or the caller's own session ceiling was reached. Target names
	// the surfaced identity (first-recorded order); Terminal carries the
	// pre-composed error when the decision computed one.
	DecideExhausted
	// DecideCancelled propagates the caller's own context cancellation
	// unchanged (B2 step 1 — first, existing precedence).
	DecideCancelled
)

// DecideOutcome is the verdict of one shared expiry decision.
type DecideOutcome struct {
	Kind     DecideKind
	Target   WatchIdentity
	Terminal error
	RenewTo  time.Time
	Err      error
}

// SeqProber performs the bounded committed observations of one expiry
// decision (B2 steps 4–5). The tuple probe reads the complete committed
// tuple lock-free through an autonomous RC snapshot; an unresolvable
// identity returns an error matching ErrSeqProbeIdentityGone (retirement),
// any other failure counts as unavailability. The lock check acquires and
// releases the sequence row inside the same probe round (the probe
// transaction rolls back and consumes no values): success means the
// contention resolved (A3), any failure or timeout means the row persists
// committed-unchanged AND held.
type SeqProber interface {
	ProbeCommittedTuple(ctx context.Context, db, tbl string) (SeqTuple, error)
	TryAcquireLock(ctx context.Context, db, tbl string) (bool, error)
}

// DecideExpiry is the shared probe-first expiry decision (design §4 D, B2
// v19; A1–A4 v20; Z1/Z2 v21). Every expiry decision — the arbitration seam,
// the zero-attempt entry surface, the conversion pre-filter, and the
// in-flight budget-expiry route (A2) — runs exactly this procedure, in this
// order:
//
//  1. caller-context cancellation propagates the caller's own error
//     unchanged (first, existing precedence);
//  2. now < D ⇒ proceed, no probe;
//  3. empty watch set ⇒ retire D and proceed attempt-first (the defensive
//     residual of episode closure, Z1);
//  4. ONE bounded probe round over the watch set — young entries
//     (failed creation samples, Z2) are re-sampled first inside this same
//     round; every entry whose committed tuple changed since its
//     last-observed sample is renew-eligible and re-baselined (B1);
//     an entry whose identity no longer resolves retires (A1);
//  5. for each still-unchanged TERMINAL-ELIGIBLE identity (Z2 — one full
//     renewal interval old from its own baseline, or from its creation
//     while never baselined), in first-recorded order, ONE bounded
//     lock-acquisition check inside the same round: an acquired identity
//     is RESOLVED and removed; a failed or timed-out tuple probe, a
//     lock-wait timeout/conflict, or any other lock-check error means the
//     identity persists committed-unchanged AND unavailable ⇒ D has
//     expired and the terminal surfaces exactly once (first-recorded
//     order);
//  6. renewal: with no unchanged-and-unavailable terminal-eligible
//     identity remaining, D renews to min(session statement deadline,
//     now + interval); if instead the round removed the LAST watch entry
//     (all-resolved), the episode closes: D retires and the statement
//     proceeds attempt-first (Z1).
func (st *SequenceRetryState) DecideExpiry(
	ctx context.Context,
	prober SeqProber,
	probeBudget, renewalInterval time.Duration,
	sessionDeadline time.Time,
) DecideOutcome {
	if err := ctx.Err(); err != nil {
		return DecideOutcome{Kind: DecideCancelled, Err: err}
	}
	if st == nil {
		return DecideOutcome{Kind: DecideProceed}
	}
	st.mu.Lock()
	if st.terminal {
		target, termErr := st.terminalID, st.terminalErr
		st.mu.Unlock()
		return DecideOutcome{Kind: DecideExhausted, Target: target, Terminal: termErr}
	}
	if !st.deadlineSet {
		st.mu.Unlock()
		return DecideOutcome{Kind: DecideProceed}
	}
	now := st.nowT()
	if now.Before(st.deadline) {
		st.mu.Unlock()
		return DecideOutcome{Kind: DecideProceed}
	}
	type snapshot struct {
		key        string
		id         WatchIdentity
		baseline   SeqTuple
		baselineAt time.Time
		createdAt  time.Time
		baselined  bool
	}
	snaps := make([]snapshot, 0, len(st.order))
	for _, key := range st.order {
		if e, ok := st.watch[key]; ok {
			snaps = append(snaps, snapshot{key, e.id, e.baseline, e.baselineAt, e.createdAt, e.baselined})
		}
	}
	st.mu.Unlock()
	if len(snaps) == 0 {
		// Defensive residual of episode closure (B2 step 3): an empty set
		// cannot coexist with a live D by construction, so this never
		// terminals.
		st.RetireDeadline()
		return DecideOutcome{Kind: DecideRetire}
	}
	if !sessionDeadline.IsZero() && !now.Before(sessionDeadline) {
		// The caller's own ceiling is reached: no new probe work starts.
		// The verdict is bounded exactly like the base retry loop's
		// session-deadline exhaustion; the caller context surfaces its own
		// cancellation with precedence wherever it is observed.
		return DecideOutcome{Kind: DecideExhausted, Target: snaps[0].id, Terminal: st.WatchCause(snaps[0].key)}
	}
	budget := probeBudget
	if !sessionDeadline.IsZero() {
		if remaining := time.Until(sessionDeadline); remaining < budget {
			budget = remaining
		}
	}
	probeCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	// (4) ONE bounded probe round: young entries are re-sampled first, then
	// the baselined entries in first-recorded order.
	ordered := make([]int, 0, len(snaps))
	for i := range snaps {
		if !snaps[i].baselined {
			ordered = append(ordered, i)
		}
	}
	for i := range snaps {
		if snaps[i].baselined {
			ordered = append(ordered, i)
		}
	}
	type reading struct {
		tuple SeqTuple
		ok    bool
		gone  bool
	}
	readings := make([]reading, len(snaps))
	arrival := now
	for _, i := range ordered {
		tuple, err := prober.ProbeCommittedTuple(probeCtx, snaps[i].id.DBName, snaps[i].id.TblName)
		arrival = st.nowT()
		if err != nil {
			readings[i].gone = errors.Is(err, ErrSeqProbeIdentityGone)
			continue
		}
		readings[i].tuple, readings[i].ok = tuple, true
	}
	// Mid-round caller cancellation outranks every probe observation
	// (v21 cancellation precedence, F2): cancelling the parent ctx also
	// cancels the derived probeCtx, so failures this round recorded may be
	// the caller's own cancellation — they must never be classified as
	// identity unavailability feeding DecideExhausted. The recheck reads
	// only the PARENT context: the probe budget's own expiry (probeCtx
	// deadline) keeps its unavailability meaning.
	if err := ctx.Err(); err != nil {
		return DecideOutcome{Kind: DecideCancelled, Err: err}
	}

	// Retirement and re-baselining: a missing identity retires (A1), a
	// changed tuple renews eligibility by re-baselining (B1), a successful
	// young re-sample arms the entry at its first observation (Z2).
	changed := make([]bool, len(snaps))
	armed := make([]bool, len(snaps))
	st.mu.Lock()
	for i, snap := range snaps {
		if readings[i].gone {
			st.removeWatchLocked(snap.key)
			continue
		}
		if !readings[i].ok {
			continue
		}
		if !snap.baselined {
			if e, live := st.watch[snap.key]; live && !e.baselined {
				e.baseline, e.baselineAt, e.baselined = readings[i].tuple, arrival, true
				armed[i] = true
			}
			continue
		}
		if !readings[i].tuple.equalByValue(snap.baseline) {
			if e, live := st.watch[snap.key]; live {
				e.baseline, e.baselineAt = readings[i].tuple, arrival
			}
			changed[i] = true
		}
	}
	st.mu.Unlock()

	// (5) the availability discriminant (A3) over still-unchanged
	// terminal-eligible identities, in first-recorded order. Terminal
	// eligibility (Z2) is one full renewal interval measured from the
	// identity's OWN baseline — or from its creation while never
	// baselined; young identities vote neither terminal nor renewal and
	// draw no lock check. A failed or timed-out tuple probe of a
	// terminal-eligible identity is itself the committed-unchanged AND
	// unavailable evidence, as is an entry still un-baselined one full
	// interval after creation — both fail safe to the terminal with no
	// lock check.
	targets := make([]WatchIdentity, 0, len(snaps))
	for i, snap := range snaps {
		if changed[i] || readings[i].gone {
			continue
		}
		if !snap.baselined {
			if armed[i] {
				// Armed inside this round: its own observation starts now
				// and it rides this renewal (Z2).
				continue
			}
			// Still un-baselined: young while less than one interval from
			// creation; past that it fails safe — its unavailability has
			// persisted a full renewal interval (Z2).
			if arrival.Sub(snap.createdAt) < renewalInterval {
				continue
			}
			targets = append(targets, snap.id)
			continue
		}
		if arrival.Sub(snap.baselineAt) < renewalInterval {
			// Young baselined identity: its first comparison is not a full
			// no-progress interval, and youth is not an observable change
			// either (Z2 cannot-renew): it rides the eligible identities'
			// verdict.
			continue
		}
		if !readings[i].ok {
			// The tuple probe of a terminal-eligible identity failed or
			// timed out: it persists committed-unchanged AND unavailable
			// (A3 fail-safe — a probe that cannot observe cannot clear the
			// identity).
			targets = append(targets, snap.id)
			continue
		}
		// ONE bounded lock-acquisition check of this unchanged,
		// terminal-eligible identity inside the same round.
		acquired, err := prober.TryAcquireLock(probeCtx, snap.id.DBName, snap.id.TblName)
		arrival = st.nowT()
		if cerr := ctx.Err(); cerr != nil {
			// Caller cancellation mid-check outranks every availability
			// classification (v21, F2): a failed acquisition under a
			// cancelled parent may be the caller's own cancellation — a
			// held-row classification would mis-stamp the exactly-once
			// terminal. Parent ctx only, so probeCtx budget expiry keeps
			// its held-row meaning.
			return DecideOutcome{Kind: DecideCancelled, Err: cerr}
		}
		if err == nil && acquired {
			// The unchanged row is acquirable again: contention is gone
			// (A3), the entry resolves and is removed from the watch set.
			st.mu.Lock()
			st.removeWatchLocked(snap.key)
			st.mu.Unlock()
			continue
		}
		targets = append(targets, snap.id)
	}
	if len(targets) > 0 {
		// D has expired: stamp the exactly-once terminal over the
		// first-recorded unresolved, terminal-eligible, committed-unchanged
		// AND held identity (B2 step 5).
		key := targets[0].Key()
		st.mu.Lock()
		st.terminal = true
		st.terminalID = targets[0]
		st.terminalErr = st.causes[key]
		st.mu.Unlock()
		return DecideOutcome{Kind: DecideExhausted, Target: targets[0], Terminal: st.terminalErr}
	}

	st.mu.Lock()
	if len(st.watch) == 0 {
		// The round removed the LAST watch entry: every recorded contention
		// resolved, so the episode closes (Z1) — D retires and the statement
		// proceeds attempt-first under its base per-invocation budget. A
		// renewal can never substitute for retirement here: renewal requires
		// an observable change and an empty set offers nothing to observe.
		st.deadline, st.deadlineSet = time.Time{}, false
		st.mu.Unlock()
		return DecideOutcome{Kind: DecideRetire}
	}
	// (6) renewal (B3 conjunctive over terminal-eligible identities, young
	// identities riding without a vote): every eligible identity either
	// observably changed or resolved.
	renewTo := arrival.Add(renewalInterval)
	if !sessionDeadline.IsZero() && sessionDeadline.Before(renewTo) {
		renewTo = sessionDeadline
	}
	st.deadline, st.deadlineSet = renewTo, true
	st.mu.Unlock()
	return DecideOutcome{Kind: DecideRenew, RenewTo: renewTo}
}

// GetSequenceRetryState returns the per-statement tier-2 retry state this
// process shares through its session info, or nil for processes that cannot
// host one (background/internal executors, view-binding children); every
// consumer treats nil as default-deny.
func (proc *Process) GetSequenceRetryState() *SequenceRetryState {
	if proc == nil || proc.Base == nil {
		return nil
	}
	return proc.Base.SessionInfo.SeqRetry
}

// ReplaceSequenceRetryState installs the freshly created state of a new
// statement execution. Retry generations of that statement never reseed it:
// the sticky window, the watch set and the publication flag must survive
// the retry transition (G1/G2, A1, C1).
func (proc *Process) ReplaceSequenceRetryState(st *SequenceRetryState) {
	if proc == nil || proc.Base == nil {
		return
	}
	proc.Base.SessionInfo.SeqRetry = st
}
