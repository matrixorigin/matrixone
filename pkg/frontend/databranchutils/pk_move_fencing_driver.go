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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// This file is the driver half of the pk-move retention-ledger fencing
// system (design D179-D182). Every round step runs on a driver-OWNED lane
// with its own context; the retiring statement's thread performs only a
// driver-local await: select over the lane's result channel and the
// driver's own T_round timer, armed BEFORE lane dispatch. At T_round the
// driver cancels the lane context (best effort), records the lane as
// expired-abandoned in its completion ledger, refuses the late result at
// intake, and classifies the round — the statement returns in bounded
// time independent of any blocked helper.
//
// Lane charges live in the driver-instance-level pool (G_lane, one pool
// per CN firmware, surviving every statement): reserve-before-spawn, held
// through every lane phase including expiry-abandonment, released only at
// actual lane exit. The pool owns two CN-close lifecycle steps wired into
// pkg/cnservice/server.go:
//
//   - CloseStep (between the pipeline/incr-service drains and the runtime
//     closes): seal admission with the pool's own flag, cancel every
//     outstanding lane context, then bounded try-join T_step, classifying
//     each lane DYNAMICALLY at join completion; lanes blocked past a
//     delivered result (rollback-blocked / commit-blocked) are DETACHED
//     WITHOUT being joined — the platform's service-lifetime cancel
//     inside lockService.Close releases them; a lane still pre-result at
//     the deadline returns a LOUD error naming lane id, phase, budget.
//   - DrainBarrier (immediately after stopRPCs): bounded T_bar join of
//     every remaining lane to FULL ACTUAL EXIT (charge and session
//     released), then a zero lanes/zero charges dump assertion; the
//     barrier never reports a zero it did not verify.
//
// Call contract: one DataBranchFencingDriver per retiring statement
// (acquires lanes, joins abort/cancel lanes at round retire); drivers
// delegate resource accounting to the shared pool (seal/cancel/detach/join
// happen only through CloseStep/DrainBarrier); classifications are the
// typed classes of data_branch_fencing.go, and the driver CONSUMES
// ErrTxnUnknown — it never re-throws it as the statement's own error.

// DataBranchFencingDriverConfig carries the driver's bounds. G_lane must
// be finite and small; T_round is the round deadline; T_step bounds the
// pool close step's per-lane try-join; T_bar bounds the final barrier's
// per-lane join.
type DataBranchFencingDriverConfig struct {
	LanePoolCapacity int
	RoundTimeout     time.Duration
	DrainStepTimeout time.Duration
	BarrierTimeout   time.Duration
}

func (c DataBranchFencingDriverConfig) fillDefaults() DataBranchFencingDriverConfig {
	if c.LanePoolCapacity <= 0 {
		c.LanePoolCapacity = 16
	}
	if c.RoundTimeout <= 0 {
		c.RoundTimeout = 10 * time.Second
	}
	if c.DrainStepTimeout <= 0 {
		c.DrainStepTimeout = 5 * time.Second
	}
	if c.BarrierTimeout <= 0 {
		c.BarrierTimeout = 10 * time.Second
	}
	return c
}

// DataBranchFencingLanePhase is the drain classifier's dynamical read at
// join completion (never a snapshot).
type DataBranchFencingLanePhase int

const (
	// DataBranchFencingLaneRunning is pre-result: the lane's step body is
	// still executing (a blocked helper call sits inside), with NO
	// certification pair published.
	DataBranchFencingLaneRunning DataBranchFencingLanePhase = iota
	// DataBranchFencingLaneRollbackBlocked: the lane observed its cancel
	// and delivered an aborted result, but its post-result tail (unlock
	// class) has not exited. Detach, do not join.
	DataBranchFencingLaneRollbackBlocked
	// DataBranchFencingLaneCommitBlocked: the lane delivered a committed
	// result and its tail has not exited. Detach, do not join.
	DataBranchFencingLaneCommitBlocked
	// DataBranchFencingLaneTerminalInFlight: pre-result, but the lane's
	// publication record carries certificate-complete AND terminal-entered
	// (D186(B.1)(ii)) - it sits INSIDE its certified terminal call (a
	// decided commit's synchronous unlock, an unknown-outcome commit
	// bounded by the resolver). Detach, do not join, never fail-stop: the
	// platform's own release paths apply and the FINAL DRAIN BARRIER joins
	// the full actual exit.
	DataBranchFencingLaneTerminalInFlight
	// DataBranchFencingLaneTerminal: full actual exit; joined.
	DataBranchFencingLaneTerminal
)

func (p DataBranchFencingLanePhase) String() string {
	switch p {
	case DataBranchFencingLaneRunning:
		return "running"
	case DataBranchFencingLaneRollbackBlocked:
		return "rollback-blocked"
	case DataBranchFencingLaneCommitBlocked:
		return "commit-blocked"
	case DataBranchFencingLaneTerminalInFlight:
		return "terminal-in-flight"
	case DataBranchFencingLaneTerminal:
		return "terminal"
	}
	return "unknown"
}

// dataBranchFencingLaneResult is the only record crossing a lane boundary.
type dataBranchFencingLaneResult struct {
	laneID uint64
	step   DataBranchFencingStepKind
	value  any
	err    error
}

// dataBranchFencingLane is one reserved pool charge: a goroutine with its
// own context, single-shot terminal action, and exactly one exit (which
// releases the charge). A lane whose owner is non-nil runs COVERED by the
// owner's reserved charge-unit (D186(B.2)(i)); its exit feeds the owner's
// lane accounting instead of releasing a lane charge.
type dataBranchFencingLane struct {
	id   uint64
	step DataBranchFencingStepKind
	pool *DataBranchFencingPool

	ctx    context.Context
	cancel context.CancelFunc
	fn     DataBranchFencingLaneFunc
	// owner is the stage owner covering this lane (nil on legacy/raw
	// lanes); pub is the lane's drain-observable publication record
	// (D186(B.1)), nil on legacy runners or under the publication defect
	// flag. The artificial exitGate park of the r75 cell schedule is
	// DELETED (D186(B.1)(iv)): blocked tails are the real stage-operator
	// geometries tracked by the owner and the publication record.
	owner *DataBranchStageOwner
	pub   *DataBranchFencingLanePublication
	// alloc is the statement's stage allocator whose coverage for this
	// lane was RESERVED in the dispatching thread before spawn
	// (D186(B.4), review F3); the lane releases it at exit. Nil for
	// lanes whose substrate owns no fresh allocator (compile path,
	// legacy spawns).
	alloc *DataBranchStageAllocator

	resultCh     chan dataBranchFencingLaneResult
	intakeClosed chan struct{}
	done         chan struct{}

	mu        sync.Mutex
	delivered bool
	cancelled bool
	// certFailed records that the delivered result was a typed
	// certification failure: by (B.1)(v) that lane rolled back IN-BODY
	// with NO commit RPC sent, so it can never be commit-blocked on the
	// wire. The pool's detachment classifier reads this certificate
	// state, never the checks alone (D184(B.1)(iv), D185(B.1)(iv)).
	certFailed bool
}

func (l *dataBranchFencingLane) run() {
	defer func() {
		// The allocator coverage reserved at dispatch is released at lane
		// exit, strictly after the lane body returned - never inside a
		// still-running closure (D186(B.4), review F3).
		if l.alloc != nil {
			l.alloc.Exit()
		}
	}()
	defer func() {
		// The charge is released ONLY at actual lane exit; ledger joins
		// and classifications never release it. A covered lane's exit feeds
		// the owner's DONE predicate AFTER the pool accounting update, so
		// a join observing the lane's done channel already sees the owner
		// note applied.
		l.pool.removeLane(l.id)
		if l.owner != nil {
			l.owner.noteLaneExit()
		}
		close(l.done)
	}()
	lctx := l.ctx
	if l.owner != nil {
		lctx = context.WithValue(lctx, dataBranchStageOwnerCtxKey, l.owner)
	}
	if l.pub != nil {
		lctx = context.WithValue(lctx, dataBranchLanePublicationCtxKey, l.pub)
	}
	v, err := l.fn(lctx)
	_, certFailed := AsDataBranchFencingCertificationError(err)
	l.mu.Lock()
	l.delivered = true
	l.cancelled = l.ctx.Err() != nil
	l.certFailed = certFailed
	l.mu.Unlock()
	res := dataBranchFencingLaneResult{laneID: l.id, step: l.step, value: v, err: err}
	select {
	case <-l.intakeClosed:
		// The driver already classified this lane expired/abandoned BEFORE
		// the result was produced: refuse it at intake deterministically.
		// ( resultCh is buffered, so a plain send select would resolve
		// randomly against an already-closed intake channel and silently
		// swallow the late-result accounting. )
		l.pool.noteLateResult()
		return
	default:
	}
	select {
	case l.resultCh <- res:
	case <-l.intakeClosed:
		// The driver classified the lane expired/abandoned while the send
		// was pending: the late result is refused at intake exactly as at
		// assembly.
		l.pool.noteLateResult()
	}
}

// phase classifies the lane DYNAMICALLY at its current completion state.
func (l *dataBranchFencingLane) phase() DataBranchFencingLanePhase {
	select {
	case <-l.done:
		return DataBranchFencingLaneTerminal
	default:
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.delivered {
		// The safe-phase classifier consumes the pair (delivery) OR
		// (certificate-complete AND terminal-entered): a lane blocked
		// INSIDE its certified terminal call is the terminal-in-flight
		// class, never Running (D186(B.1)(ii)).
		if l.pub != nil && l.pub.certificateComplete() {
			return DataBranchFencingLaneTerminalInFlight
		}
		return DataBranchFencingLaneRunning
	}
	if l.cancelled || l.certFailed {
		// A certification-failed lane rolled back IN-BODY without any
		// commit RPC: its delivered result is the rollback shape, so its
		// tail classifies rollback-blocked even when the lane context was
		// never cancelled. Detachment reads this certificate state.
		return DataBranchFencingLaneRollbackBlocked
	}
	return DataBranchFencingLaneCommitBlocked
}

// certificateFailed reports the lane's recorded certification state: true
// only when the delivered result was the typed certification failure.
// The pool's close-step detachment consumes exactly this value (the
// certificate), never the checks the lane's executor ran.
func (l *dataBranchFencingLane) certificateFailed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.certFailed
}

// DataBranchFencingPoolDump is the pool's honest — possibly non-zero —
// accounting. Charges equal the charged resource-holders: ownerless lanes
// (= open sessions) plus one charge-unit per live stage owner (covered
// lanes consume the owner's unit, D186(B.2)(i)/D189(B.1)(iii)); Spawned
// counts real goroutine spawns (vs. reserve attempts); LateResults counts
// results refused at intake after an expiry classification; Retained is
// the retained-owner registry view keyed by owner token with stage/step
// (D189(B.2)(ii)) - every charge is visible at every instant as exactly
// one of {live lane, detached lane, retained registry entry} (C.30).
type DataBranchFencingPoolDump struct {
	Capacity        int
	Charges         int
	Owners          int
	Spawned         uint64
	ReserveAttempts uint64
	LateResults     uint64
	Sealed          bool
	Phases          map[uint64]DataBranchFencingLanePhase
	Retained        map[DataBranchStageOwnerToken]DataBranchFencingRetainedEntry
}

// DataBranchFencingPool is the driver-instance-level global lane pool
// (G_lane): one finite charge table covering every lane phase and every
// stage owner, including expired-abandoned-not-yet-exited lanes and
// retained owners whose observed close is outstanding.
type DataBranchFencingPool struct {
	cfg DataBranchFencingDriverConfig

	mu              sync.Mutex
	sealed          bool
	nextLaneID      uint64
	lanes           map[uint64]*dataBranchFencingLane
	spawned         uint64
	reserveAttempts uint64
	lateResults     uint64

	// owners holds one charge-unit per live stage owner, from mint to the
	// single observed-close release (D186(B.2)(i), D188(B.1)(i)). The
	// retained registry of D189(B.2)(ii) is the laneless-and-open subset,
	// surfaced through Dump.Retained and the drain classifiers.
	nextOwnerSeq uint64
	owners       map[uint64]*DataBranchStageOwner
	// probeOrdinal allocates owner ordinals for laneless (watermark-probe)
	// owners far above the driver-local round ordinals, so two live owners
	// NEVER share a token even within one scratch identity (D187(B.1)(i)).
	probeOrdinal atomic.Uint64
	// lastCloseStepRetained records how many retained owners the most
	// recent CloseStep classified (no fail-stop, no join) - the cell
	// oracle for the D189(B.2)(iii) classifier.
	lastCloseStepRetained atomic.Int64
}

// NewDataBranchFencingPool constructs one pool with the given bounds;
// zero fields take the compiled defaults.
func NewDataBranchFencingPool(cfg DataBranchFencingDriverConfig) *DataBranchFencingPool {
	p := &DataBranchFencingPool{
		cfg:    cfg.fillDefaults(),
		lanes:  make(map[uint64]*dataBranchFencingLane),
		owners: make(map[uint64]*DataBranchStageOwner),
	}
	p.probeOrdinal.Store(1 << 32)
	return p
}

// Config returns the pool's effective bounds.
func (p *DataBranchFencingPool) Config() DataBranchFencingDriverConfig { return p.cfg }

// chargedLocked is the aggregate reservation (C.29”(a)): ownerless live
// lanes plus live owners; it never exceeds LanePoolCapacity while every
// reserve refuses at the bound.
func (p *DataBranchFencingPool) chargedLocked() int {
	uncovered := 0
	for _, lane := range p.lanes {
		if lane.owner == nil {
			uncovered++
		}
	}
	return uncovered + len(p.owners)
}

// spawn reserves one charge and starts the lane; the charge is registered
// BEFORE the goroutine is spawned (reserve-before-spawn), so a refusal
// never leaves an untracked goroutine. A lane covered by an owner consumes
// the owner's reserved unit and bypasses the lane reservation - the
// owner's is the charge (D186(B.2)(i)). The returned class is
// DataBranchFencingRoundCommitted on success, else the fail-loud reason.
func (p *DataBranchFencingPool) spawn(
	step DataBranchFencingStepKind, fn DataBranchFencingLaneFunc, parentCtx context.Context,
) (*dataBranchFencingLane, DataBranchFencingRoundClass) {
	return p.spawnOwned(step, fn, parentCtx, nil, nil, nil)
}

// spawnOwned reserves and starts one lane, optionally covered by a stage
// owner, carrying a publication record, and releasing a pre-reserved
// allocator coverage at exit. The ALLOCATION reservation (alloc) must
// already be recorded by the dispatching caller BEFORE this spawn - the
// assign-then-spawn order here keeps it visible to the lane goroutine
// without a publication race.
func (p *DataBranchFencingPool) spawnOwned(
	step DataBranchFencingStepKind, fn DataBranchFencingLaneFunc, parentCtx context.Context,
	owner *DataBranchStageOwner, pub *DataBranchFencingLanePublication,
	alloc *DataBranchStageAllocator,
) (*dataBranchFencingLane, DataBranchFencingRoundClass) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reserveAttempts++
	if p.sealed {
		return nil, DataBranchFencingRoundPoolSealed
	}
	if owner == nil && p.chargedLocked() >= p.cfg.LanePoolCapacity {
		return nil, DataBranchFencingRoundSaturated
	}
	p.nextLaneID++
	ctx, cancel := context.WithCancel(parentCtx)
	lane := &dataBranchFencingLane{
		id:           p.nextLaneID,
		step:         step,
		pool:         p,
		ctx:          ctx,
		cancel:       cancel,
		fn:           fn,
		owner:        owner,
		pub:          pub,
		alloc:        alloc,
		resultCh:     make(chan dataBranchFencingLaneResult, 1),
		intakeClosed: make(chan struct{}),
		done:         make(chan struct{}),
	}
	p.lanes[lane.id] = lane
	p.spawned++
	if owner != nil {
		owner.noteLaneSpawn()
	}
	go lane.run()
	return lane, DataBranchFencingRoundCommitted
}

// MintStageOwner reserves ONE charge-unit for a lane-covered stage owner
// (the T_f1 pair or T_f2 owner of a round), minted BEFORE its first lane
// dispatches (D186(B.2)(i)).
func (p *DataBranchFencingPool) MintStageOwner(
	scratch uint64, ordinal uint64, label string,
) (*DataBranchStageOwner, DataBranchFencingRoundClass) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reserveAttempts++
	if p.sealed {
		return nil, DataBranchFencingRoundPoolSealed
	}
	if p.chargedLocked() >= p.cfg.LanePoolCapacity {
		return nil, DataBranchFencingRoundSaturated
	}
	return p.mintStageOwnerLocked(scratch, ordinal, label, false), DataBranchFencingRoundCommitted
}

// MintLanelessStageOwner reserves ONE charge-unit for an owner with no
// covered lanes (the watermark probe's throwaway operator,
// D189(B.1)(i)); its ordinal is allocated from the probe range, far above
// any driver-local round ordinal.
func (p *DataBranchFencingPool) MintLanelessStageOwner(
	scratch uint64, label string,
) (*DataBranchStageOwner, DataBranchFencingRoundClass) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reserveAttempts++
	if p.sealed {
		return nil, DataBranchFencingRoundPoolSealed
	}
	if p.chargedLocked() >= p.cfg.LanePoolCapacity {
		return nil, DataBranchFencingRoundSaturated
	}
	return p.mintStageOwnerLocked(scratch, p.probeOrdinal.Add(1), label, true), DataBranchFencingRoundCommitted
}

func (p *DataBranchFencingPool) mintStageOwnerLocked(
	scratch uint64, ordinal uint64, label string, laneless bool,
) *DataBranchStageOwner {
	p.nextOwnerSeq++
	owner := &DataBranchStageOwner{
		pool:        p,
		seq:         p.nextOwnerSeq,
		token:       DataBranchStageOwnerToken{Scratch: scratch, Ordinal: ordinal},
		label:       label,
		noMoreLanes: laneless,
		closedCh:    make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	p.owners[owner.seq] = owner
	return owner
}

// releaseOwner performs the owner's single charge release; the DONE
// transition makes it exactly-once by construction.
func (p *DataBranchFencingPool) releaseOwner(owner *DataBranchStageOwner) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.owners, owner.seq)
}

func (p *DataBranchFencingPool) removeLane(id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.lanes, id)
}

func (p *DataBranchFencingPool) noteLateResult() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lateResults++
}

// snapshotOwners copies the live owner set (the per-owner reads run AFTER
// the pool mutex is released - owner state reads take the owner mutex,
// and the pool mutex is never held across them).
func (p *DataBranchFencingPool) snapshotOwners() []*DataBranchStageOwner {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*DataBranchStageOwner, 0, len(p.owners))
	for _, owner := range p.owners {
		out = append(out, owner)
	}
	return out
}

// snapshotRetained returns the RETAINED registry view (D189(B.2)(ii)):
// laneless owners whose observed close is still outstanding. With the
// registry-classification defect flag armed the view is silenced - the
// (H.10)(u) RED twin's silent-retained witness.
func (p *DataBranchFencingPool) snapshotRetained() []dataBranchStageOwnerSnapshot {
	if dataBranchFencingRetainedClassificationDisabled.Load() {
		return nil
	}
	owners := p.snapshotOwners()
	out := make([]dataBranchStageOwnerSnapshot, 0, len(owners))
	for _, owner := range owners {
		snap := owner.snapshot()
		if snap.retained() {
			out = append(out, snap)
		}
	}
	return out
}

// Dump snapshots the pool accounting.
func (p *DataBranchFencingPool) Dump() DataBranchFencingPoolDump {
	p.mu.Lock()
	dump := DataBranchFencingPoolDump{
		Capacity:        p.cfg.LanePoolCapacity,
		Charges:         p.chargedLocked(),
		Owners:          len(p.owners),
		Spawned:         p.spawned,
		ReserveAttempts: p.reserveAttempts,
		LateResults:     p.lateResults,
		Sealed:          p.sealed,
		Phases:          make(map[uint64]DataBranchFencingLanePhase, len(p.lanes)),
		Retained:        make(map[DataBranchStageOwnerToken]DataBranchFencingRetainedEntry),
	}
	for id, lane := range p.lanes {
		// Dynamical phase read at dump time; lane exit races phases as
		// terminal, which stays honest.
		dump.Phases[id] = lane.phase()
	}
	p.mu.Unlock()
	for _, snap := range p.snapshotRetained() {
		dump.Retained[snap.token] = DataBranchFencingRetainedEntry{
			Token: snap.token,
			Stage: snap.label,
			State: snap.state,
		}
	}
	return dump
}

// CloseStep is the pool's CN-drain step: seal lane admission with the
// pool's own flag, cancel every outstanding lane context (a best-effort
// retire hint), then bounded try-join each lane with the T_step budget.
// Rollback/commit-blocked lanes (result delivered, tail in flight) are
// DETACHED without join — their unlocking tails belong to the platform's
// service-lifetime cancellation. A lane still pre-result at the deadline
// fails the step LOUD with lane id, phase, and budget, inheriting the
// fail-stop drain chain.
func (p *DataBranchFencingPool) CloseStep(ctx context.Context) error {
	p.mu.Lock()
	p.sealed = true
	lanes := make([]*dataBranchFencingLane, 0, len(p.lanes))
	for _, lane := range p.lanes {
		lanes = append(lanes, lane)
	}
	p.mu.Unlock()

	for _, lane := range lanes {
		lane.cancel()
	}
	for _, lane := range lanes {
		select {
		case <-lane.done:
			continue
		default:
		}
		timer := time.NewTimer(p.cfg.DrainStepTimeout)
		select {
		case <-lane.done:
			timer.Stop()
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			switch phase := lane.phase(); phase {
			case DataBranchFencingLaneRollbackBlocked, DataBranchFencingLaneCommitBlocked,
				DataBranchFencingLaneTerminalInFlight:
				// Detached WITHOUT join: the platform's service-lifetime
				// cancel during lockService.Close releases the tail; the
				// FINAL DRAIN BARRIER joins it to full exit. The
				// classifier consumes the lane's recorded certificate
				// here (D185(B.1)(iv)): a certification-failed lane shed
				// its rollback PRE-DETACH with no commit RPC, so the
				// rollback-blocked phase is its honest classification. A
				// terminal-in-flight lane published the full certified
				// pair and sits inside its terminal call - it is the
				// certified C-family (D186(B.1)(ii)/(C.19'''''')), never
				// Running, never fail-stopping the drain.
				_ = lane.certificateFailed()
			default:
				return moerr.NewInternalErrorNoCtxf(
					"data branch fencing pool close step expired: lane %d step %s phase %s budget %s",
					lane.id, lane.step, phase, p.cfg.DrainStepTimeout)
			}
		}
	}
	// D189(B.2)(iii): retained registry entries are NOT lanes - the step
	// RECORDS them and neither fail-stops on nor joins them (retention
	// past lane exit is a legal transient: deferred cleanup armed); the
	// FINAL DRAIN BARRIER owns the terminal join. The recorded count is
	// the cell oracle for the classification.
	p.lastCloseStepRetained.Store(int64(len(p.snapshotRetained())))
	return nil
}

// DrainBarrier is the FINAL DRAIN BARRIER: bounded T_bar join of every
// outstanding (typically detached) lane AND every retained registry entry
// to FULL ACTUAL EXIT / full close observation, then the zero lanes /
// zero retained / zero charges assertion (D189(B.2)(iii), D190(C.29”)(c)).
// It never reports a zero it did not verify.
func (p *DataBranchFencingPool) DrainBarrier(ctx context.Context) error {
	p.mu.Lock()
	lanes := make([]*dataBranchFencingLane, 0, len(p.lanes))
	for _, lane := range p.lanes {
		lanes = append(lanes, lane)
	}
	p.mu.Unlock()
	for _, lane := range lanes {
		select {
		case <-lane.done:
			continue
		default:
		}
		timer := time.NewTimer(p.cfg.BarrierTimeout)
		select {
		case <-lane.done:
			timer.Stop()
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			return moerr.NewInternalErrorNoCtxf(
				"data branch fencing final drain barrier expired: lane %d step %s phase %s budget %s",
				lane.id, lane.step, lane.phase(), p.cfg.BarrierTimeout)
		}
	}
	// Join every retained registry entry to full close observation: the
	// reordered stopRPCs cancellation segment (D190(B.1)(i)) discharges
	// the stalled unlock, the drained rollback closes, the ClosedEvent
	// publishes, and the owner releases EXACTLY ONCE - all within the
	// same BarrierTimeout.
	retained := p.snapshotRetained()
	for _, snap := range retained {
		owner := p.ownerByToken(snap.token)
		if owner == nil {
			continue
		}
		timer := time.NewTimer(p.cfg.BarrierTimeout)
		select {
		case <-owner.doneCh:
			timer.Stop()
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			return moerr.NewInternalErrorNoCtxf(
				"data branch fencing final drain barrier expired joining retained owner %d/%d stage %s budget %s",
				snap.token.Scratch, snap.token.Ordinal, snap.label, p.cfg.BarrierTimeout)
		}
	}
	p.mu.Lock()
	remainingLanes := len(p.lanes)
	remainingCharges := p.chargedLocked()
	p.mu.Unlock()
	remainingRetained := len(p.snapshotRetained())
	if remainingLanes != 0 || remainingRetained != 0 || remainingCharges != 0 {
		return moerr.NewInternalErrorNoCtxf(
			"data branch fencing final drain barrier found unverified residue: %d lanes, %d retained owners, %d charges",
			remainingLanes, remainingRetained, remainingCharges)
	}
	return nil
}

// ownerByToken finds a live owner by its token (barrier join helper).
func (p *DataBranchFencingPool) ownerByToken(token DataBranchStageOwnerToken) *DataBranchStageOwner {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, owner := range p.owners {
		if owner.token == token {
			return owner
		}
	}
	return nil
}

// dataBranchFencingLedgerEntry records one lane's completion state inside
// one statement's (transient, never durable) completion ledger.
type dataBranchFencingLedgerEntry struct {
	lane      *dataBranchFencingLane
	step      DataBranchFencingStepKind
	class     DataBranchFencingRoundClass
	abandoned bool
	joined    bool
}

// dataBranchFencingCompletionLedger is the per-statement ledger:
// expired-abandoned recording, classification, and the exactly-once
// idempotent join table.
type dataBranchFencingCompletionLedger struct {
	mu      sync.Mutex
	entries map[uint64]*dataBranchFencingLedgerEntry
}

func newDataBranchFencingCompletionLedger() *dataBranchFencingCompletionLedger {
	return &dataBranchFencingCompletionLedger{entries: make(map[uint64]*dataBranchFencingLedgerEntry)}
}

func (l *dataBranchFencingCompletionLedger) record(lane *dataBranchFencingLane) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries[lane.id] = &dataBranchFencingLedgerEntry{lane: lane, step: lane.step}
}

func (l *dataBranchFencingCompletionLedger) classify(laneID uint64, class DataBranchFencingRoundClass, abandoned bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.entries[laneID]; ok {
		e.class = class
		e.abandoned = abandoned
	}
}

// join marks the lane joined exactly once and waits its full exit; the
// SECOND completion of the same lane is rejected, keeping the ledger's
// join table single-shot.
func (l *dataBranchFencingCompletionLedger) join(lane *dataBranchFencingLane) error {
	l.mu.Lock()
	e, ok := l.entries[lane.id]
	if !ok {
		l.mu.Unlock()
		return moerr.NewInternalErrorNoCtxf("data branch fencing ledger rejected completion of unknown lane %d", lane.id)
	}
	if e.joined {
		l.mu.Unlock()
		return moerr.NewInternalErrorNoCtxf(
			"data branch fencing ledger rejected duplicate completion of lane %d step %s", lane.id, e.step)
	}
	e.joined = true
	l.mu.Unlock()
	<-lane.done
	return nil
}

// joinAllCompleted joins every lane whose result the driver took (abort /
// cancel / success) at round retire; expired-abandoned lanes are left to
// the ledger drain. Joins already recorded are skipped — the exactly-once
// table is the ledger's, bookkeeping stays idempotent.
func (l *dataBranchFencingCompletionLedger) joinAllCompleted() {
	l.mu.Lock()
	pending := make([]*dataBranchFencingLane, 0, len(l.entries))
	for _, e := range l.entries {
		if !e.joined && !e.abandoned {
			pending = append(pending, e.lane)
		}
	}
	l.mu.Unlock()
	for _, lane := range pending {
		// First completion recorded; a duplicate here cannot occur
		// because only the ledger owner joins, and the skip filter above.
		_ = l.join(lane)
	}
}

// dump copies the ledger table (join table, for assertions).
func (l *dataBranchFencingCompletionLedger) dump() map[uint64]dataBranchFencingLedgerEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[uint64]dataBranchFencingLedgerEntry, len(l.entries))
	for id, e := range l.entries {
		out[id] = *e
	}
	return out
}

// DataBranchFencingDriver is one retiring statement's fencing driver: it
// implements DataBranchFencingLaneRunner with lanes, timers, and the
// completion ledger, and it CONSUMES ErrTxnUnknown (never re-thrown). A
// driver retires with its statement; its round-scoped charges all belong
// to the shared pool and outlive it until actual lane exit. It also
// implements the owned-runner contract: the round's stage owners are
// minted here (the driver-local monotonic round ordinal of D187(B.1)(i))
// and every owned step dispatches under its owner with the lane
// publication record.
type DataBranchFencingDriver struct {
	pool    *DataBranchFencingPool
	ledger  *dataBranchFencingCompletionLedger
	retired atomic.Bool

	ordMu       sync.Mutex
	nextOrdinal uint64
}

// NewDataBranchFencingDriver builds the statement's driver on the shared
// pool (the generation's pool in production, D186(B.3)).
func NewDataBranchFencingDriver(pool *DataBranchFencingPool) *DataBranchFencingDriver {
	return &DataBranchFencingDriver{
		pool:   pool,
		ledger: newDataBranchFencingCompletionLedger(),
	}
}

// allocateOrdinal mints the driver-local monotonic round ordinal
// (D187(B.1)(i)): paired with the round's fresh ScratchTableID so two live
// owners NEVER share a token even if a draw were ever duplicated.
func (d *DataBranchFencingDriver) allocateOrdinal() uint64 {
	d.ordMu.Lock()
	defer d.ordMu.Unlock()
	d.nextOrdinal++
	return d.nextOrdinal
}

// mintStageOwner reserves the round owner charge on the driver's pool
// (D186(B.2)(i)): ONE charged owner whose covered lanes ride its unit.
func (d *DataBranchFencingDriver) mintStageOwner(
	scratch uint64, label string,
) (*DataBranchStageOwner, *DataBranchFencingError) {
	if d.retired.Load() {
		return nil, &DataBranchFencingError{
			Class: DataBranchFencingRoundStageFailed, Step: DataBranchFencingStepGrant,
			Cause: moerr.NewInternalErrorNoCtx("fencing driver already retired"),
		}
	}
	owner, class := d.pool.MintStageOwner(scratch, d.allocateOrdinal(), label)
	if class != DataBranchFencingRoundCommitted {
		return nil, &DataBranchFencingError{
			Class: class,
			Cause: moerr.NewInternalErrorNoCtxf("data branch fencing owner reserve for %s: %s", label, class),
		}
	}
	return owner, nil
}

// RunStep reserves a lane charge, arms T_round BEFORE dispatch, dispatches
// the step, and awaits: lane result (classified and joined at retire) or
// T_round (lane cancelled best-effort, recorded expired-abandoned, late
// results refused at intake). This is the legacy ownerless entry; the
// owned round path uses runStepOwned.
func (d *DataBranchFencingDriver) RunStep(
	ctx context.Context, step DataBranchFencingStepKind, fn DataBranchFencingLaneFunc,
) (any, error) {
	return d.runStepOwned(ctx, step, fn, nil, nil)
}

// runStepOwned is RunStep with owner coverage and the publication record:
// spawn refusals abandon the owner asynchronously (the never-spawned-tail
// abandon rule of D186(B.2)(iii) as re-selected by D187(B.2)/(B.2)(i)),
// and the expiry edges revoke the owner's result acceptance
// (D187(B.2)(ii)).
func (d *DataBranchFencingDriver) runStepOwned(
	ctx context.Context, step DataBranchFencingStepKind, fn DataBranchFencingLaneFunc,
	owner *DataBranchStageOwner, pub *DataBranchFencingLanePublication,
) (any, error) {
	if d.retired.Load() {
		if owner != nil {
			owner.AbandonAsync()
		}
		return nil, &DataBranchFencingError{
			Class: DataBranchFencingRoundStageFailed, Step: step,
			Cause: moerr.NewInternalErrorf(ctx, "fencing driver already retired"),
		}
	}
	timer := time.NewTimer(d.pool.cfg.RoundTimeout)
	if dataBranchFencingPublicationDisabled.Load() {
		// RED twin (D186(H.10)(n)(a)): no drain-observable publication, so
		// the terminal-blocked lane classifies Running and the close step
		// fail-stops - (A.1)(iii)'s drain halt.
		pub = nil
	}
	alloc := dataBranchStageAllocatorFromCtx(ctx)
	if alloc != nil {
		// Reserve this lane's allocator coverage in the DISPATCHING
		// thread, before the lane goroutine exists: the statement-root
		// detached drain can then never observe a zero while this lane
		// is still scheduling (D186(B.4), review F3). Released at lane
		// exit; released HERE on spawn refusal.
		alloc.Enter()
	}
	lane, reason := d.pool.spawnOwned(step, fn, ctx, owner, pub, alloc)
	if lane == nil {
		timer.Stop()
		if alloc != nil {
			alloc.Exit()
		}
		if owner != nil {
			// Reservation REJECTION (saturated or sealed): the owner
			// abandons with exactly-once rollback (D186(B.2)(iii),
			// (H.10)(o)(i)).
			owner.AbandonAsync()
		}
		return nil, &DataBranchFencingError{
			Class: reason, Step: step,
			Cause: moerr.NewInternalErrorf(ctx, "data branch fencing lane pool: %s", reason),
		}
	}
	defer timer.Stop()
	d.ledger.record(lane)
	select {
	case res := <-lane.resultCh:
		// Result intake: the lane's terminal action is recorded; the lane
		// is joined once, at this step's retire.
		if err := d.ledger.join(lane); err != nil {
			return nil, &DataBranchFencingError{
				Class: DataBranchFencingRoundStageFailed, Step: step, Cause: err,
			}
		}
		return res.value, res.err
	case <-ctx.Done():
		if owner != nil {
			owner.noteRoundExpiry()
		}
		lane.cancel()
		d.ledger.classify(lane.id, DataBranchFencingRoundStageFailed, true)
		close(lane.intakeClosed)
		select {
		case <-lane.resultCh:
			d.pool.noteLateResult()
		default:
		}
		return nil, ctx.Err()
	case <-timer.C:
		if owner != nil {
			// EXPIRY: result acceptance revoked, the terminal selection
			// untouched - a selected Commit keeps owning the transition
			// (D187(B.2)(ii)); an ACTIVE owner is abandoned by its own
			// lane's failure path.
			owner.noteRoundExpiry()
		}
		lane.cancel()
		close(lane.intakeClosed)
		// Accepting authority is stripped irrevocably; the abandoned lane
		// passes to the driver's ledger drain, joined exactly once.
		d.ledger.classify(lane.id, DataBranchFencingRoundExpired, true)
		return nil, &DataBranchFencingError{
			Class: DataBranchFencingRoundExpired, Step: step,
			Cause: moerr.NewInternalErrorf(ctx,
				"fencing round deadline %s expired at %s", d.pool.cfg.RoundTimeout, step),
		}
	}
}

// RunRound executes one full fencing round through ExecuteDataBranchFencingRound
// with this driver as the lane runner, then joins the round's completed
// lanes at retire.
func (d *DataBranchFencingDriver) RunRound(
	ctx context.Context,
	locks DataBranchScratchLocks,
	exec DataBranchFencingStageExecutor,
	in DataBranchFencingRoundInput,
) (DataBranchFencingRecord, error) {
	rec, err := ExecuteDataBranchFencingRound(ctx, d, locks, exec, in)
	d.ledger.joinAllCompleted()
	return rec, err
}

// DrainLedger joins this statement's expired-abandoned lanes — postponed
// past round retire on purpose; the pool charge still outlives it until
// actual lane exit. Waits honor ctx.
func (d *DataBranchFencingDriver) DrainLedger(ctx context.Context) error {
	for _, e := range d.ledger.dump() {
		if !e.abandoned {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := d.ledger.join(e.lane); err != nil {
			return err
		}
	}
	return nil
}

// Retire seals the driver's own admission; outstanding lanes keep draining
// through the pool and DrainLedger.
func (d *DataBranchFencingDriver) Retire() { d.retired.Store(true) }

// Generation registry (D186(B.3)): ONE fencing pool per CN-service
// generation, keyed by the owning CN service's identity. Pool
// construction, open admission and destruction are generation-scoped;
// each CN's close chain drains ONLY its own generation and removes the
// verified-drained entry so a later same-identity construction receives a
// FRESH, INDEPENDENT, OPEN generation (C.24). The "" key is the stock
// single-generation resolution: byte-identical construction, seal and
// barrier oracles for every single-CN schedule (D186(B.3)(iii)). Test
// code must NOT route through the registry — build a local pool per test.
var dataBranchFencingPools sync.Map // generation (CN service UUID) -> *DataBranchFencingPool

// DataBranchFencingPoolFor returns the owning generation's lane pool,
// constructing it with compiled defaults on first use.
func DataBranchFencingPoolFor(generation string) *DataBranchFencingPool {
	if v, ok := dataBranchFencingPools.Load(generation); ok {
		return v.(*DataBranchFencingPool)
	}
	np := NewDataBranchFencingPool(DataBranchFencingDriverConfig{})
	actual, _ := dataBranchFencingPools.LoadOrStore(generation, np)
	return actual.(*DataBranchFencingPool)
}

// DataBranchFencingPoolDrainFor is the CN drain-chain step registered
// between the pipeline/incr-service drains and the runtime closes, routed
// by the CLOSING service's generation (D186(B.3)(ii)): seal, cancel,
// bounded try-join, detach. No pool ever constructed is a clean no-op -
// and another generation's admission, lanes, charges and seals stay
// untouched.
func DataBranchFencingPoolDrainFor(generation string) error {
	if v, ok := dataBranchFencingPools.Load(generation); ok {
		return v.(*DataBranchFencingPool).CloseStep(context.Background())
	}
	return nil
}

// DataBranchFencingDrainBarrierFor is the FINAL DRAIN BARRIER registered
// immediately after stopRPCs in the CN drain chain, routed by the closing
// generation: join every detached lane and retained owner to full exit /
// close observation and verify the zero lanes/retained/charges dump. A
// verified-drained generation leaves the registry - no inherited seal or
// drained charge for a later same-identity construction (D186(B.3)(iii)).
func DataBranchFencingDrainBarrierFor(generation string) error {
	v, ok := dataBranchFencingPools.Load(generation)
	if !ok {
		return nil
	}
	if err := v.(*DataBranchFencingPool).DrainBarrier(context.Background()); err != nil {
		return err
	}
	dataBranchFencingPools.Delete(generation)
	return nil
}

// DataBranchFencingGlobalPool returns the stock single-generation pool
// (the "" key), constructing it with compiled defaults on first use -
// the single-CN equivalence of D186(B.3)(iii).
func DataBranchFencingGlobalPool() *DataBranchFencingPool {
	return DataBranchFencingPoolFor("")
}

// DataBranchFencingPoolDrain is the stock single-generation CN drain step
// (the "" key).
func DataBranchFencingPoolDrain() error {
	return DataBranchFencingPoolDrainFor("")
}

// DataBranchFencingDrainBarrier is the stock single-generation FINAL
// DRAIN BARRIER (the "" key).
func DataBranchFencingDrainBarrier() error {
	return DataBranchFencingDrainBarrierFor("")
}
