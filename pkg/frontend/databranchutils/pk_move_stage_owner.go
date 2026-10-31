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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// This file is the shared stage-operator ownership component (design
// D186(B.2) as refined by D187/D188, generalized by D189(B.1), drained per
// D189(B.2)/D190, and validated per D191). ONE ownership component covers
// every stage transaction opened through newStageTxn:
//
//   - the T_f1-pair amalgamation: ONE owner covers the round's grant and
//     F1 stage operators (D186(B.2)(i)); its charge is reserved BEFORE the
//     grant lane spawns and held through the F1 stage's terminal cleanup -
//     neither lane exit releases it;
//   - T_f2 gets its own owner on the same component
//     (D189(B.1)(i)/(ii)): on a fatal stage-SQL error the executor
//     schedules the deferred rollback before fail() runs; fail()'s public
//     Rollback gate-rejects; the lane exits WITHOUT releasing; the owner
//     enters RETAINING at its last lane exit and releases only at the
//     scheduled cleanup's observed ClosedEvent;
//   - the watermark probe's throwaway operator consumes the same
//     component and ordinarily releases in-call.
//
// Decision machine (D187(B.2)(i), D188(B.1)(ii)): ACTIVE -> COMMITTING or
// ACTIVE -> ABANDONING, then RETAINING, then DONE. Exactly one selector
// wins and the decision is recorded BEFORE any Commit/Rollback
// invocation, so exactly one public terminal invocation per operator
// exists by construction (the operator's own terminal-call gate stays the
// platform belt to this decision gate's braces). Expiry during COMMITTING
// revokes result acceptance ONLY - no Rollback is invoked against a
// claimed gate, and a gate-rejected ErrTxnClosed return carries no
// cleanup.
//
// Release rides ONLY the observed operator close (D188(B.1)(i)): the
// ClosedEvent closeLocked publishes exactly once (operator.go:2037-2060),
// registered through the public AppendEventCallback surface at stage
// construction while the operator is provably open (append panics on
// closed, operator_events.go:65-67). DONE - and the single release -
// fires only when BOTH the owner's stage lanes have exited AND the
// observed close has been consumed (D188(B.1)(ii)). A laneless owner
// whose close is outstanding is the pool's RETAINED registry entry
// (D189(B.2)(ii)); the transition keys on the owner state machine at
// lane exit, never on the RollbackEvent witness (review carry-over:
// PH-2 includes scheduled cleanup, and classification must NOT wait for
// the later RollbackEvent publication).

// DataBranchStageOwnerToken is the fresh per-round owner identity
// (D187(B.1)(i)): the round's own ScratchTableID paired with a
// driver-local monotonic ordinal. Two live owners never share a token.
type DataBranchStageOwnerToken struct {
	Scratch uint64
	Ordinal uint64
}

// dataBranchStageOwnerState is the owner's terminal-decision state
// machine under its own mutex.
type dataBranchStageOwnerState int

const (
	dataBranchStageOwnerActive dataBranchStageOwnerState = iota
	dataBranchStageOwnerCommitting
	dataBranchStageOwnerAbandoning
	dataBranchStageOwnerRetaining
	dataBranchStageOwnerDone
)

func (s dataBranchStageOwnerState) String() string {
	switch s {
	case dataBranchStageOwnerActive:
		return "active"
	case dataBranchStageOwnerCommitting:
		return "committing"
	case dataBranchStageOwnerAbandoning:
		return "abandoning"
	case dataBranchStageOwnerRetaining:
		return "retaining"
	case dataBranchStageOwnerDone:
		return "done"
	}
	return "unknown"
}

// DataBranchStageOwner charges one stage-transaction lifetime to the pool:
// minted (charged) before its first lane dispatches, released only after
// the observed ClosedEvent AND every covered lane exit. The owner never
// invokes Commit: the ordinary stage path selects COMMITTING and invokes
// the platform commit itself; the abandonment edges select ABANDONING and
// the owner performs exactly one public Rollback.
type DataBranchStageOwner struct {
	pool  *DataBranchFencingPool
	seq   uint64
	token DataBranchStageOwnerToken
	label string

	mu sync.Mutex
	// state is the D187(B.2) terminal-decision machine.
	state dataBranchStageOwnerState
	// acceptRevoked records an expiry-side result-acceptance revocation
	// (D187(B.2)(ii)): the selected Commit still owns the terminal
	// transition; its outcome is discarded at the round layer.
	acceptRevoked bool
	// lanesSpawned/lanesExited count covered stage lanes; noMoreLanes is
	// set by the round once no further stage lane can spawn (after the
	// final stage step returns, or at the first bail, or at abandonment
	// for never-spawned tails).
	lanesSpawned int
	lanesExited  int
	noMoreLanes  bool
	// op is the bound stage operator (the per-owner slot replacing the
	// driver-wide f1op singleton, D186(B.2)(i)); opScratch is the token
	// assertion the binder passed.
	op        client.TxnOperator
	opBound   bool
	opScratch uint64
	// vacuousClose marks an abandonment selected before any stage
	// transaction existed: the no-transaction reap of D187(B.2)(iv).
	vacuousClose bool
	// done guards the exactly-once release.
	done bool

	// closedObserved is the non-blocking D188(B.1)(iii) publication fed by
	// the ClosedEvent callback (an atomic store plus a channel close); the
	// observation consumer below performs the state-machine consumption.
	closedObserved atomic.Bool
	closedCh       chan struct{}
	closedOnce     sync.Once

	doneCh   chan struct{}
	doneOnce sync.Once
}

// Token returns the owner's round identity (D187(B.1)(i)).
func (o *DataBranchStageOwner) Token() DataBranchStageOwnerToken { return o.token }

// BoundOperator returns the bound stage operator, or nil before binding.
func (o *DataBranchStageOwner) BoundOperator() client.TxnOperator {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.op
}

// BindOperator installs the stage operator into the owner's slot and
// registers the ClosedEvent observation through the public
// AppendEventCallback surface - at stage construction, while the operator
// is provably open (operator_events.go:65-67 panics on closed). The
// scratch assertion pins the token-keyed slot authority (D187(B.1)(ii)):
// a mismatched binding is a NO-OP error, so an expired predecessor can
// never publish into a live round's owner.
func (o *DataBranchStageOwner) BindOperator(op client.TxnOperator, scratch uint64) error {
	if op == nil {
		return moerr.NewInternalErrorNoCtxf(
			"data branch fencing owner %d/%d: cannot bind a nil stage operator",
			o.token.Scratch, o.token.Ordinal)
	}
	if scratch != o.token.Scratch {
		// Token-keyed authority: a mismatched token never mutates the
		// slot (D187(B.1)(ii), (C.28)).
		return moerr.NewInternalErrorNoCtxf(
			"data branch fencing owner token mismatch: owner %d/%d, bind scratch %d",
			o.token.Scratch, o.token.Ordinal, scratch)
	}
	o.mu.Lock()
	if o.opBound {
		o.mu.Unlock()
		return moerr.NewInternalErrorNoCtxf(
			"data branch fencing owner %d/%d: stage operator already bound",
			o.token.Scratch, o.token.Ordinal)
	}
	if o.state != dataBranchStageOwnerActive {
		// Late binding after a recorded terminal decision (an abandonment
		// with the vacuous no-transaction reap, or a selected commit, or
		// a completed owner): the owner NEVER adopts the operator. The
		// caller owns the unbound operator and rolls it back itself -
		// every production bind site already does (pk_move_fencing_cs.go
		// grant/f2/probe) - and the owner's completion rides the recorded
		// vacuous close (D187(B.2)(iv)), so no abandoning decision later
		// skips the operator's rollback while accounting finishes without
		// observing its close.
		state := o.state.String()
		o.mu.Unlock()
		return moerr.NewInternalErrorNoCtxf(
			"data branch fencing owner %d/%d: binding rejected, owner already %s",
			o.token.Scratch, o.token.Ordinal, state)
	}
	o.op = op
	o.opBound = true
	o.opScratch = scratch
	// Publication and callback installation are ONE ownership transition
	// under the owner's own mutex: abandon() selects its terminal
	// decision under the same mutex, so no abandonment can interleave
	// between the slot publication and this registration and close the
	// operator first (AppendEventCallback panics on a closed operator,
	// operator_events.go:65-67). Registration precedes EVERY terminal
	// action by construction: the binder installs the callback before the
	// operator's first statement, commit or rollback is issued. The
	// callback performs ONLY the recorded non-blocking publication
	// discipline (atomic store + channel close, operator_events.go under
	// the operator mu).
	op.AppendEventCallback(client.ClosedEvent, client.TxnEventCallback{
		Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error {
			o.noteClosed()
			return nil
		},
	})
	o.mu.Unlock()
	// The observation consumer is the registry's close-future consumer
	// (D189(B.2)(ii)): after the driver's last lane exits, THIS is what
	// consumes the observed close and performs the single release.
	go o.observeClose()
	return nil
}

// noteClosed is the ClosedEvent callback body: non-blocking publication
// only - an atomic store and a channel close (D188(B.1)(iii)). It fires
// under the operator mu, so it must never take a lock or block.
func (o *DataBranchStageOwner) noteClosed() {
	o.closedObserved.Store(true)
	o.closedOnce.Do(func() { close(o.closedCh) })
}

// ClosedObserved reports whether the observed operator close was consumed
// (test/cell surface; the production reads ride the state machine).
func (o *DataBranchStageOwner) ClosedObserved() bool { return o.closedObserved.Load() }

// DoneCh closes when the owner reaches DONE (single release performed).
func (o *DataBranchStageOwner) DoneCh() <-chan struct{} { return o.doneCh }

// State snapshots the owner's decision-machine state (cell surface).
func (o *DataBranchStageOwner) State() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.state.String()
}

// AcceptanceRevoked reports whether expiry already revoked result
// acceptance (cell oracle for D187(B.2)(ii)).
func (o *DataBranchStageOwner) AcceptanceRevoked() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.acceptRevoked
}

// SelectCommitting records the ordinary completion path's terminal
// selection BEFORE the stage invokes op.Commit (D187(B.2)(i)). It fails
// only when acceptance was already revoked or another selector won: the
// caller must then abandon its attempt WITHOUT invoking Commit.
func (o *DataBranchStageOwner) SelectCommitting() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if dataBranchFencingDecisionGateDisabled.Load() {
		// RED-twin geometry: no recorded decision; the r76 first-return
		// rule applies (any terminal verb may race in).
		return true
	}
	if o.state != dataBranchStageOwnerActive || o.acceptRevoked || o.done {
		return false
	}
	o.state = dataBranchStageOwnerCommitting
	return true
}

// noteRoundExpiry records the round expiry from the driver side
// (D187(B.2)(ii)): result acceptance is revoked; the terminal selection
// itself is never rewritten - a selected Commit continues to own the
// transition and an ACTIVE owner is abandoned by its stage lane's own
// failure path (or by abandonStaleOwner on the bail route).
func (o *DataBranchStageOwner) noteRoundExpiry() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.acceptRevoked = true
}

// abandon is the shared decision path. It returns (invoke, op): invoke
// reports whether THIS call won the ABANDONING selection and must make
// the single public Rollback invocation against op. With the decision
// gate mechanically disabled (RED twin), every call re-invokes,
// reproducing the r76 first-return race against the operator's own gate.
func (o *DataBranchStageOwner) abandon() (invoke bool, op client.TxnOperator) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if dataBranchFencingDecisionGateDisabled.Load() {
		return o.opBound && !o.done, o.op
	}
	switch o.state {
	case dataBranchStageOwnerActive:
		o.state = dataBranchStageOwnerAbandoning
		if !o.opBound {
			// The round was rejected before ANY scratch transaction
			// existed: the recorded no-transaction reap (D187(B.2)(iv)).
			o.vacuousClose = true
		}
		return o.opBound, o.op
	default:
		// A later selector observing a recorded decision NEVER invokes a
		// second public terminal call (D187(B.2)(i)); an expiry observer
		// records the revocation only.
		o.acceptRevoked = true
		return false, nil
	}
}

// Abandon selects ABANDONING and performs the owner's single public
// Rollback IN the caller (a stage lane's own failure path or the
// probe's in-call release). The invocation's return is a RESULT input
// only; the charge releases only at the observed close (or the vacuous
// no-transaction reap). A gate-rejected return (the deferred cleanup
// already owned by the platform, e.g. the T_f2 fatal geometry of
// D189(B.1)(ii)) is the recorded NO-OP edge - cleanup runs, the owner
// enters RETAINING at lane exit, and the release rides the observed
// ClosedEvent.
func (o *DataBranchStageOwner) Abandon(ctx context.Context) {
	invoke, op := o.abandon()
	if invoke {
		// Exactly one public terminal invocation per operator exists BY
		// CONSTRUCTION (C.27); the operator gate stays the platform belt.
		_ = op.Rollback(ctx)
	}
	o.consumeAndFinish()
}

// AbandonAsync is the driver-side abandonment (F1-stage reservation
// rejection, round bail with a bound operator): the decision is recorded
// synchronously so no later selector can race in, and the single Rollback
// runs detached - the statement thread never waits on a cleanup that may
// legitimately stall in the platform's service-lifetime unlock retry.
func (o *DataBranchStageOwner) AbandonAsync() {
	invoke, op := o.abandon()
	if invoke {
		go func() {
			_ = op.Rollback(context.Background())
			o.consumeAndFinish()
		}()
		return
	}
	o.consumeAndFinish()
}

// NoteNoMoreLanes records that no further stage lane will spawn for this
// owner: after the round's final stage step returns, or at every error
// bail before a later stage, or at abandonment of a never-spawned tail.
// DONE requires it: an owner mid-round (grant exited, F1 not yet
// dispatched) is NOT a retained registry entry - it is ordinary in-flight
// accounting under its own charge.
func (o *DataBranchStageOwner) NoteNoMoreLanes() {
	o.mu.Lock()
	o.noMoreLanes = true
	o.mu.Unlock()
	o.consumeAndFinish()
}

// noteLaneSpawn records a covered lane spawn (lane accounting only; the
// charge is the owner's own, reserved at mint per D186(B.2)(i)).
func (o *DataBranchStageOwner) noteLaneSpawn() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lanesSpawned++
}

// noteLaneExit records a covered lane's actual exit (D179/D180 lane
// accounting is consumed by the DONE predicate, D188(B.1)(ii)).
func (o *DataBranchStageOwner) noteLaneExit() {
	o.mu.Lock()
	o.lanesExited++
	o.mu.Unlock()
	o.consumeAndFinish()
}

// observeClose consumes the observed operator close after the driver's
// lanes are gone: THE completion consumer of the retained registry entry
// (D189(B.2)(ii)) whenever the close outlives the lane window.
func (o *DataBranchStageOwner) observeClose() {
	<-o.closedCh
	o.consumeAndFinish()
}

// consumeAndFinish folds every release input - lane exits, observed close,
// abandonment of a vacuous no-transaction reap - into the DONE decision
// and, when it fires, performs the single release:
//
//	DONE := noMoreLanes AND lanesExited == lanesSpawned AND
//	        (closed observed OR vacuous no-transaction reap)
//
// RETAINING is the presentation state of the same geometry with the close
// outstanding (D188(B.1)(ii), D189(B.2)(ii)); the pool's retained
// registry keys exactly on it (review carry-over: the transition keys on
// the state machine at lane exit, PH-2 includes scheduled cleanup, and
// NO RollbackEvent witness is awaited).
func (o *DataBranchStageOwner) consumeAndFinish() {
	o.mu.Lock()
	if o.done {
		o.mu.Unlock()
		return
	}
	lanesComplete := o.noMoreLanes && o.lanesExited >= o.lanesSpawned
	closeConsumed := o.closedObserved.Load() || o.vacuousClose
	if dataBranchFencingObservedCloseBoundaryDisabled.Load() {
		// RED-twin geometry: release at the selected call's return /
		// lane-exit boundary (the r76-r77 rule), never awaiting the
		// observed close.
		closeConsumed = true
	}
	switch {
	case lanesComplete && closeConsumed:
		o.state = dataBranchStageOwnerDone
		o.done = true
	case lanesComplete && !closeConsumed:
		if o.state != dataBranchStageOwnerRetaining {
			o.state = dataBranchStageOwnerRetaining
		}
	}
	done := o.done
	o.mu.Unlock()
	if done {
		o.pool.releaseOwner(o)
		o.doneOnce.Do(func() { close(o.doneCh) })
	}
}

// snapshot describes the owner for pool dumps and drain classifiers.
type dataBranchStageOwnerSnapshot struct {
	token         DataBranchStageOwnerToken
	label         string
	state         string
	done          bool
	lanesSpawned  int
	lanesExited   int
	noMoreLanes   bool
	closed        bool
	acceptRevoked bool
}

func (o *DataBranchStageOwner) snapshot() dataBranchStageOwnerSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	return dataBranchStageOwnerSnapshot{
		token:         o.token,
		label:         o.label,
		state:         o.state.String(),
		done:          o.done,
		lanesSpawned:  o.lanesSpawned,
		lanesExited:   o.lanesExited,
		noMoreLanes:   o.noMoreLanes,
		closed:        o.closedObserved.Load(),
		acceptRevoked: o.acceptRevoked,
	}
}

// retained reports whether the owner is a RETAINED registry entry right
// now: all covered lanes exited, the final-stage tail is sealed, the
// observed close is still outstanding, and release has not fired. The
// boundary-reverted RED geometry never retains (release fires at lane
// exit), matching the defect it reproduces.
func (s dataBranchStageOwnerSnapshot) retained() bool {
	if s.done || !s.noMoreLanes || s.lanesExited < s.lanesSpawned {
		return false
	}
	if s.closed {
		return false
	}
	if s.state == dataBranchStageOwnerDone.String() {
		return false
	}
	return true
}

// DataBranchFencingRetainedEntry is the pool Dump view of one retained
// owner (D189(B.2)(ii)): keyed by owner token with its stage label and
// decision state; every charge remains visible to every dump (C.30).
type DataBranchFencingRetainedEntry struct {
	Token DataBranchStageOwnerToken
	Stage string
	State string
}

// ---------------------------------------------------------------------------
// Per-lane publication record (D186(B.1)(i)/(ii)).
// ---------------------------------------------------------------------------

// DataBranchFencingLanePublication is the drain-observable twin of one
// stage lane's certification path: the certificate record published BEFORE
// the op.Commit invocation, plus the terminal-entry edge (op.Commit about
// to be invoked). The drain's safe-phase classifier consumes the pair
// (delivery) OR (certificate-complete AND terminal-entered): a lane
// blocked INSIDE its terminal call classifies as the certified
// terminal-in-flight family, never Running (D186(B.1)(ii)).
type DataBranchFencingLanePublication struct {
	cert            atomic.Pointer[DataBranchFencingCertificate]
	terminalEntered atomic.Bool
}

// NewDataBranchFencingLanePublication builds one lane's publication
// record.
func NewDataBranchFencingLanePublication() *DataBranchFencingLanePublication {
	return &DataBranchFencingLanePublication{}
}

// PublishCertificate writes the certificate record; it must run BEFORE
// the terminal call is invoked (D186(B.1)(i)).
func (p *DataBranchFencingLanePublication) PublishCertificate(cert DataBranchFencingCertificate) {
	if p == nil {
		return
	}
	c := cert
	p.cert.Store(&c)
}

// PublishTerminalEntry records the terminal-entry edge; it must run after
// the certificate publication and immediately before op.Commit is
// invoked (D186(B.1)(ii)).
func (p *DataBranchFencingLanePublication) PublishTerminalEntry() {
	if p == nil {
		return
	}
	p.terminalEntered.Store(true)
}

// Certificate returns the published record for cell oracles.
func (p *DataBranchFencingLanePublication) Certificate() (DataBranchFencingCertificate, bool) {
	c := p.cert.Load()
	if c == nil {
		return DataBranchFencingCertificate{}, false
	}
	return *c, true
}

// certificateComplete reports the certified pair the safe-phase
// classifier consumes: certificate-complete AND terminal-entered.
func (p *DataBranchFencingLanePublication) certificateComplete() bool {
	if p == nil {
		return false
	}
	c := p.cert.Load()
	return c != nil && c.Complete() && p.terminalEntered.Load()
}

// ---------------------------------------------------------------------------
// Lane context injection (the owned round path).
// ---------------------------------------------------------------------------

type dataBranchStageOwnerCtxKeyT struct{}
type dataBranchLanePublicationCtxKeyT struct{}
type dataBranchStageAllocatorCtxKeyT struct{}

var (
	dataBranchStageOwnerCtxKey      dataBranchStageOwnerCtxKeyT
	dataBranchLanePublicationCtxKey dataBranchLanePublicationCtxKeyT
	dataBranchStageAllocatorCtxKey  dataBranchStageAllocatorCtxKeyT
)

// AttachDataBranchStageAllocator carries the statement's stage allocator
// into the fencing driver: every owned lane's allocator coverage is then
// RESERVED in the dispatching thread BEFORE the lane goroutine is spawned
// (D186(B.4), review F3). Recording coverage inside the lane goroutine
// would let the detached zero-drain observe the pool before a
// not-yet-scheduled lane ever runs its first Enter - a premature
// destruction window. A nil allocator attaches nothing.
func AttachDataBranchStageAllocator(
	ctx context.Context, a *DataBranchStageAllocator,
) context.Context {
	if a == nil {
		return ctx
	}
	return context.WithValue(ctx, dataBranchStageAllocatorCtxKey, a)
}

// dataBranchStageAllocatorFromCtx returns the attached statement
// allocator, or nil when the caller's substrate owns no fresh allocator
// (the compile path rides process-owned pools).
func dataBranchStageAllocatorFromCtx(ctx context.Context) *DataBranchStageAllocator {
	if ctx == nil {
		return nil
	}
	a, _ := ctx.Value(dataBranchStageAllocatorCtxKey).(*DataBranchStageAllocator)
	return a
}

// DataBranchRoundOwnerFromCtx returns the round's stage owner the lane
// body must route its stage operator through, or nil on legacy (ownerless)
// runners.
func DataBranchRoundOwnerFromCtx(ctx context.Context) *DataBranchStageOwner {
	o, _ := ctx.Value(dataBranchStageOwnerCtxKey).(*DataBranchStageOwner)
	return o
}

// DataBranchLanePublicationFromCtx returns the lane's publication record,
// or nil on legacy runners or under the publication defect flag.
func DataBranchLanePublicationFromCtx(ctx context.Context) *DataBranchFencingLanePublication {
	p, _ := ctx.Value(dataBranchLanePublicationCtxKey).(*DataBranchFencingLanePublication)
	return p
}

// ---------------------------------------------------------------------------
// Defect flags (test-only; same discipline as the certification defect
// flag: harness-only installers, never enabled on any production path,
// never referenced from production flows except the single check site).
// ---------------------------------------------------------------------------

// Amalgamation disabled (D186(H.10)(o) RED twin): ExecuteDataBranchFencingRound
// runs the ownerless legacy geometry, reproducing (A.2)'s escape window.
var dataBranchFencingAmalgamationDisabled atomic.Bool

// DisableDataBranchFencingAmalgamationForTest flips the amalgamation
// defect flag and returns a restore func; harness-only.
func DisableDataBranchFencingAmalgamationForTest(disabled bool) (restore func()) {
	prev := dataBranchFencingAmalgamationDisabled.Swap(disabled)
	return func() { dataBranchFencingAmalgamationDisabled.Store(prev) }
}

// Decision gate disabled (D187(H.10)(o.5) RED twin): no terminal decision
// is recorded and every abandonment re-invokes Rollback, re-running the
// r76 first-return rule against the operator's own gate.
var dataBranchFencingDecisionGateDisabled atomic.Bool

// DisableDataBranchFencingDecisionGateForTest flips the decision-gate
// defect flag and returns a restore func; harness-only.
func DisableDataBranchFencingDecisionGateForTest(disabled bool) (restore func()) {
	prev := dataBranchFencingDecisionGateDisabled.Swap(disabled)
	return func() { dataBranchFencingDecisionGateDisabled.Store(prev) }
}

// Observed-close boundary disabled (D188(H.10)(o)/(o.5)/(o.6) RED twin,
// D191(H.10)(t) RED twin): the charge releases at the selected call's
// return / lane-exit boundary instead of the observed close.
var dataBranchFencingObservedCloseBoundaryDisabled atomic.Bool

// DisableDataBranchFencingObservedCloseBoundaryForTest flips the boundary
// defect flag and returns a restore func; harness-only.
func DisableDataBranchFencingObservedCloseBoundaryForTest(disabled bool) (restore func()) {
	prev := dataBranchFencingObservedCloseBoundaryDisabled.Swap(disabled)
	return func() { dataBranchFencingObservedCloseBoundaryDisabled.Store(prev) }
}

// Retained classification disabled (D190(H.10)(u) RED twin (a)): the
// pool's retained registry is invisible to Dump, CloseStep and the final
// barrier, reproducing the silent-retained lane-only zero.
var dataBranchFencingRetainedClassificationDisabled atomic.Bool

// DisableDataBranchFencingRetainedClassificationForTest flips the
// registry-classification defect flag and returns a restore func;
// harness-only.
func DisableDataBranchFencingRetainedClassificationForTest(disabled bool) (restore func()) {
	prev := dataBranchFencingRetainedClassificationDisabled.Swap(disabled)
	return func() { dataBranchFencingRetainedClassificationDisabled.Store(prev) }
}

// Publication disabled (D186(H.10)(n) RED twin (a)): owned lanes carry no
// publication record, so a terminally blocked lane classifies Running and
// the close step fail-stops - (A.1)(iii)'s drain halt.
var dataBranchFencingPublicationDisabled atomic.Bool

// DisableDataBranchFencingPublicationForTest flips the publication defect
// flag and returns a restore func; harness-only.
func DisableDataBranchFencingPublicationForTest(disabled bool) (restore func()) {
	prev := dataBranchFencingPublicationDisabled.Swap(disabled)
	return func() { dataBranchFencingPublicationDisabled.Store(prev) }
}

// ---------------------------------------------------------------------------
// Retirement substrate allocator ownership (D186(B.4)).
// ---------------------------------------------------------------------------

// DataBranchStageAllocator owns one retirement substrate's fresh
// allocator through every outstanding lane spawned under it: destruction
// rides the platform's own mpool.DeleteMPool, sequenced only after the
// fencing closures confirm zero outstanding lane-scoped work on the
// SUCCESS, FAILURE, and REFUSAL paths alike (D186(B.4)(i)). NO platform
// mpool API or behavior change (D186(B.4)(ii)).
type DataBranchStageAllocator struct {
	mp          *mpool.MPool
	outstanding sync.WaitGroup
}

// NewDataBranchStageAllocator wraps the freshly constructed pool.
func NewDataBranchStageAllocator(mp *mpool.MPool) *DataBranchStageAllocator {
	return &DataBranchStageAllocator{mp: mp}
}

// MPool returns the owned pool (the substrate constructor's argument).
func (a *DataBranchStageAllocator) MPool() *mpool.MPool { return a.mp }

// Enter records one lane-scoped fencing closure that can still touch the
// pool (the grant/F1/F2 stage entry points and the watermark probe).
func (a *DataBranchStageAllocator) Enter() {
	if a == nil {
		return
	}
	a.outstanding.Add(1)
}

// Exit records that closure's return.
func (a *DataBranchStageAllocator) Exit() {
	if a == nil {
		return
	}
	a.outstanding.Done()
}

// DrainAndDelete waits for zero outstanding lane-scoped closures, then
// destroys the pool through the platform's terminal destructor
// (mpool.DeleteMPool removes the globalPools registration). The
// constructor-refusal arm calls it immediately before its early return.
func (a *DataBranchStageAllocator) DrainAndDelete() {
	if a == nil {
		return
	}
	a.outstanding.Wait()
	mpool.DeleteMPool(a.mp)
}

// DetachDrain releases ONE outstanding reservation and rides the final
// zero-drain plus pool destruction on a detached goroutine: the bounded
// statement-return path (DROP SNAPSHOT/PITR) never joins a terminal tail
// stalled in the lockservice's service-lifetime unlock retry
// (lockservice/service.go:769 ignores request cancellation), while
// destruction still observes the same exactly-once zero-outstanding
// accounting on SUCCESS, FAILURE and REFUSAL alike (D186(B.4)). The
// statement root must hold the reservation it releases here BEFORE the
// fencing critical section runs, and every lane's coverage must be
// reserved in the dispatching thread (runStepOwned), so no Add can ever
// land after the root release.
func (a *DataBranchStageAllocator) DetachDrain() {
	if a == nil {
		return
	}
	go func() {
		a.Exit()
		a.DrainAndDelete()
	}()
}
