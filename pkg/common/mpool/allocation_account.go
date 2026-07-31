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

package mpool

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

// AllocationOwner and AllocationSite are bounded diagnostic dimensions.
// Callers assign stable values in their own allocation-site ledger. Zero is
// reserved so an accounted allocation can never be published without an
// explicit owner and site.
type AllocationOwner uint8
type AllocationSite uint8

const (
	AllocationOwnerMin AllocationOwner = 1
	AllocationOwnerMax AllocationOwner = 63
	AllocationSiteMin  AllocationSite  = 1
	AllocationSiteMax  AllocationSite  = math.MaxUint8
)

var (
	ErrAllocationAccountCapacity    error = allocationAccountSentinel("allocation account capacity exceeded")
	ErrAllocationAccountSealed      error = allocationAccountSentinel("allocation account is sealed")
	ErrAllocationAccountInvalid     error = allocationAccountSentinel("invalid allocation account")
	ErrAllocationAccountStale       error = allocationAccountSentinel("stale allocation account handle")
	ErrAllocationAccountMismatch    error = allocationAccountSentinel("allocation account ownership mismatch")
	ErrAllocationAllocatorLimit     error = allocationAccountSentinel("allocation exceeds allocator size limit")
	ErrAllocationAccountInvariant   error = allocationAccountSentinel("allocation account invariant failure")
	ErrAllocationAdmissionSuspended error = allocationAccountSentinel("allocation account admission is suspended")
	ErrAllocationMetadataSlots      error = allocationAccountSentinel("allocation metadata slots exhausted")
	ErrAllocationGenerationSlots    error = allocationAccountSentinel("allocation account generation slots exhausted")
	ErrAllocationAccountLive        error = allocationAccountSentinel("allocation account still owns memory")
)

type allocationAccountSentinel string

func (e allocationAccountSentinel) Error() string { return string(e) }

type allocationAccountDetailError struct {
	cause       error
	detail      string
	detailFirst bool
}

func (e *allocationAccountDetailError) Error() string {
	if e == nil || e.cause == nil {
		return "allocation account error"
	}
	if e.detail == "" {
		return e.cause.Error()
	}
	if e.detailFirst {
		return e.detail + ": " + e.cause.Error()
	}
	return e.cause.Error() + ": " + e.detail
}

func prefixAllocationAccountError(
	cause error,
	format string,
	args ...any,
) error {
	return &allocationAccountDetailError{
		cause:       cause,
		detail:      fmt.Sprintf(format, args...),
		detailFirst: true,
	}
}

func (e *allocationAccountDetailError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func wrapAllocationAccountError(
	cause error,
	format string,
	args ...any,
) error {
	return &allocationAccountDetailError{
		cause:  cause,
		detail: fmt.Sprintf(format, args...),
	}
}

const (
	allocationAccountSealedBit = uint64(1) << 63
	allocationAccountUsedMask  = allocationAccountSealedBit - 1
)

// AllocationAccountHandle identifies one use of a reusable registry slot.
// The upper 32 bits are the slot generation and the lower 32 bits are the slot.
type AllocationAccountHandle uint64

func newAllocationAccountHandle(slot, generation uint32) AllocationAccountHandle {
	return AllocationAccountHandle(uint64(generation)<<32 | uint64(slot))
}

func (h AllocationAccountHandle) slot() uint32 {
	return uint32(h)
}

func (h AllocationAccountHandle) generation() uint32 {
	return uint32(uint64(h) >> 32)
}

// AllocationAccountSnapshot is immutable observation state. A terminal owner
// may publish it after Seal and exact zero; taking a snapshot does not mutate
// account lifecycle.
type AllocationAccountSnapshot struct {
	Handle AllocationAccountHandle
	Limit  uint64
	Used   uint64
	Peak   uint64
	Sealed bool
}

// AllocationAccountTerminalState classifies the one immutable snapshot
// exported by an execution generation. A nonzero account at terminal cleanup
// is an ownership invariant failure, not recoverable capacity pressure.
type AllocationAccountTerminalState uint8

const (
	AllocationAccountTerminalValid AllocationAccountTerminalState = iota + 1
	AllocationAccountTerminalInvariantFailure
)

// AllocationFailureReason is the non-overlapping control-flow reason exposed
// to later pressure handling. Only Capacity is eligible for reclaim/spill or
// a smaller operation retry; every other reason is terminal for that logical
// operation or generation.
type AllocationFailureReason uint8

const (
	AllocationFailureNone AllocationFailureReason = iota
	AllocationFailureCapacity
	AllocationFailureSealed
	AllocationFailureMismatch
	AllocationFailureAllocatorLimit
	AllocationFailureInvariant
	AllocationFailureSuspended
)

func AllocationFailureReasonOf(err error) AllocationFailureReason {
	switch {
	case errors.Is(err, ErrAllocationAccountInvariant):
		return AllocationFailureInvariant
	case errors.Is(err, ErrAllocationAccountMismatch):
		return AllocationFailureMismatch
	case errors.Is(err, ErrAllocationAccountSealed):
		return AllocationFailureSealed
	case errors.Is(err, ErrAllocationAllocatorLimit):
		return AllocationFailureAllocatorLimit
	case errors.Is(err, ErrAllocationAdmissionSuspended):
		return AllocationFailureSuspended
	case errors.Is(err, ErrAllocationAccountCapacity),
		errors.Is(err, ErrAllocationMetadataSlots):
		return AllocationFailureCapacity
	default:
		return AllocationFailureNone
	}
}

func IsRetryableAllocationCapacity(err error) bool {
	return AllocationFailureReasonOf(err) == AllocationFailureCapacity
}

// AllocationAccountTerminalSnapshot is the immutable terminal observation of
// one generation. Failure snapshots retain the live-byte value observed at
// the terminal boundary even if a later physical Free drains the tombstone.
type AllocationAccountTerminalSnapshot struct {
	AllocationAccountSnapshot
	State           AllocationAccountTerminalState
	LiveOwner       AllocationOwner
	LiveSite        AllocationSite
	LiveAllocations uint64
}

// AllocationAccountCheckpoint records the physical live-byte boundary before
// a retryable logical operation. The owner performs its own private-allocation
// rollback, then ValidateRollback proves that the same generation returned to
// this exact boundary before a retry can begin.
type AllocationAccountCheckpoint struct {
	Handle AllocationAccountHandle
	Used   uint64
}

// AllocationCapacityController lets an account share a higher-level aggregate
// cap. The controller owns cap policy only; physical MPool metadata remains
// the sole release owner.
type AllocationCapacityController interface {
	AcquireAllocationCapacity(uint64) error
	ReleaseAllocationCapacity(uint64)
}

// AllocationAccount owns physical allocation capacity for one execution
// generation. state packs the sealed bit and used bytes into one atomic word,
// so Acquire and Seal have one unambiguous linearization point.
type AllocationAccount struct {
	registry *AllocationAccountRegistry
	handle   AllocationAccountHandle
	limit    uint64
	control  AllocationCapacityController

	state    atomic.Uint64
	peak     atomic.Uint64
	inflight atomic.Int64
}

func (a *AllocationAccount) Handle() AllocationAccountHandle {
	if a == nil {
		return 0
	}
	return a.handle
}

func (a *AllocationAccount) Snapshot() AllocationAccountSnapshot {
	if a == nil {
		return AllocationAccountSnapshot{}
	}
	state := a.state.Load()
	return AllocationAccountSnapshot{
		Handle: a.handle,
		Limit:  a.limit,
		Used:   state & allocationAccountUsedMask,
		Peak:   a.peak.Load(),
		Sealed: state&allocationAccountSealedBit != 0,
	}
}

func (a *AllocationAccount) Checkpoint() (AllocationAccountCheckpoint, error) {
	if a == nil || a.registry == nil || a.handle == 0 {
		return AllocationAccountCheckpoint{}, ErrAllocationAccountInvalid
	}
	resolved, ok := a.registry.Resolve(a.handle)
	if !ok || resolved != a {
		return AllocationAccountCheckpoint{}, ErrAllocationAccountStale
	}
	snapshot := a.Snapshot()
	if snapshot.Sealed {
		return AllocationAccountCheckpoint{}, ErrAllocationAccountSealed
	}
	return AllocationAccountCheckpoint{
		Handle: snapshot.Handle,
		Used:   snapshot.Used,
	}, nil
}

// ValidateRollback proves that an owner restored its complete physical
// allocation boundary. It never mutates accounting: only physical Free owns a
// release, so a helper cannot hide a leaked allocation by decrementing usage.
func (a *AllocationAccount) ValidateRollback(
	checkpoint AllocationAccountCheckpoint,
) error {
	if a == nil || checkpoint.Handle == 0 {
		return ErrAllocationAccountInvalid
	}
	if checkpoint.Handle != a.handle {
		return wrapAllocationAccountError(
			ErrAllocationAccountMismatch,
			"checkpoint=%d account=%d",
			checkpoint.Handle,
			a.handle,
		)
	}
	snapshot := a.Snapshot()
	if snapshot.Sealed {
		return ErrAllocationAccountSealed
	}
	if snapshot.Used != checkpoint.Used {
		return wrapAllocationAccountError(
			ErrAllocationAccountInvariant,
			"checkpoint-used=%d current-used=%d",
			checkpoint.Used,
			snapshot.Used,
		)
	}
	return nil
}

// RollbackToCheckpoint runs the owner's physical cleanup and then proves the
// exact generation boundary. It deliberately does not own or synthesize any
// release: MPool allocation metadata remains the sole release authority.
func (a *AllocationAccount) RollbackToCheckpoint(
	checkpoint AllocationAccountCheckpoint,
	rollback func() error,
) error {
	if rollback == nil {
		return ErrAllocationAccountInvalid
	}
	if a == nil || checkpoint.Handle != a.handle {
		return ErrAllocationAccountMismatch
	}
	if a.Snapshot().Sealed {
		return ErrAllocationAccountSealed
	}
	if err := rollback(); err != nil {
		return err
	}
	return a.ValidateRollback(checkpoint)
}

func (a *AllocationAccount) acquire(capacity uint64) error {
	if a == nil || a.registry == nil || a.handle == 0 {
		return ErrAllocationAccountInvalid
	}
	if capacity == 0 {
		return nil
	}
	state := a.state.Load()
	if state&allocationAccountSealedBit != 0 {
		return ErrAllocationAccountSealed
	}
	used := state & allocationAccountUsedMask
	if used > a.limit || capacity > a.limit-used {
		return newAllocationAccountCapacityError(used, capacity, a.limit)
	}

	// Register before consulting the shared controller. Once Seal publishes the
	// sealed bit, it either observes this transaction or this transaction
	// observes sealed before acquiring controller capacity.
	a.inflight.Add(1)
	defer a.inflight.Add(-1)
	state = a.state.Load()
	if state&allocationAccountSealedBit != 0 {
		return ErrAllocationAccountSealed
	}
	used = state & allocationAccountUsedMask
	if used > a.limit || capacity > a.limit-used {
		return newAllocationAccountCapacityError(used, capacity, a.limit)
	}
	if a.control != nil {
		if err := a.control.AcquireAllocationCapacity(capacity); err != nil {
			return err
		}
	}
	acquired := false
	defer func() {
		if !acquired && a.control != nil {
			a.control.ReleaseAllocationCapacity(capacity)
		}
	}()

	for {
		state = a.state.Load()
		if state&allocationAccountSealedBit != 0 {
			return ErrAllocationAccountSealed
		}
		used = state & allocationAccountUsedMask
		if used > a.limit || capacity > a.limit-used {
			return newAllocationAccountCapacityError(used, capacity, a.limit)
		}
		next := used + capacity
		if a.state.CompareAndSwap(state, next) {
			for {
				peak := a.peak.Load()
				if next <= peak || a.peak.CompareAndSwap(peak, next) {
					acquired = true
					return nil
				}
			}
		}
	}
}

func newAllocationAccountCapacityError(
	used uint64,
	requested uint64,
	limit uint64,
) error {
	return wrapAllocationAccountError(
		ErrAllocationAccountCapacity,
		"used=%d requested=%d limit=%d",
		used,
		requested,
		limit,
	)
}

func (a *AllocationAccount) release(capacity uint64) {
	if capacity == 0 {
		return
	}
	// Keep the local charge until the higher-level policy charge is gone. With
	// metadata released by allocationLease first, exact local zero is therefore
	// also a complete-release boundary.
	if a.control != nil {
		state := a.state.Load()
		if capacity > state&allocationAccountUsedMask {
			panic("allocation account release underflow")
		}
		a.control.ReleaseAllocationCapacity(capacity)
	}
	for {
		state := a.state.Load()
		used := state & allocationAccountUsedMask
		if capacity > used {
			panic("allocation account release underflow")
		}
		next := state - capacity
		if a.state.CompareAndSwap(state, next) {
			if next&allocationAccountUsedMask == 0 {
				a.registry.tryDrainTombstone(a)
			}
			return
		}
	}
}

// Seal prevents every later acquisition. It waits only for acquisitions that
// linearized before the sealed bit was published to finish updating peak.
func (a *AllocationAccount) Seal() AllocationAccountSnapshot {
	if a == nil {
		return AllocationAccountSnapshot{}
	}
	for {
		state := a.state.Load()
		if state&allocationAccountSealedBit != 0 ||
			a.state.CompareAndSwap(state, state|allocationAccountSealedBit) {
			break
		}
	}
	for a.inflight.Load() != 0 {
		runtime.Gosched()
	}
	return a.Snapshot()
}

type allocationAccountRegistrySlot struct {
	account atomic.Pointer[AllocationAccount]
	// terminal and tombstone are protected by AllocationAccountRegistry.mu.
	terminal  *AllocationAccountTerminalSnapshot
	tombstone bool
}

// AllocationAccountRegistry bounds live generations and accounted-allocation
// metadata for one CN. Registry slots are reused only after Seal and exact
// zero. Their generation counters never wrap.
type AllocationAccountRegistry struct {
	mu sync.Mutex

	slots       []allocationAccountRegistrySlot
	generations []uint32
	free        []uint32
	suspended   bool
	tombstones  uint32

	maxAllocations  uint64
	liveAllocations atomic.Uint64
	peakAllocations atomic.Uint64
}

func NewAllocationAccountRegistry(
	generationSlots uint32,
	allocationSlots uint64,
) (*AllocationAccountRegistry, error) {
	if generationSlots == 0 || uint64(generationSlots) >= uint64(math.MaxInt) {
		return nil, ErrAllocationAccountInvalid
	}
	registry := &AllocationAccountRegistry{
		slots:          make([]allocationAccountRegistrySlot, uint64(generationSlots)+1),
		generations:    make([]uint32, uint64(generationSlots)+1),
		free:           make([]uint32, generationSlots),
		maxAllocations: allocationSlots,
	}
	for i := uint32(0); i < generationSlots; i++ {
		registry.free[i] = generationSlots - i
	}
	return registry, nil
}

func (r *AllocationAccountRegistry) Open(
	limit uint64,
) (*AllocationAccount, error) {
	return r.OpenWithController(limit, nil)
}

func (r *AllocationAccountRegistry) OpenWithController(
	limit uint64,
	control AllocationCapacityController,
) (*AllocationAccount, error) {
	if r == nil || limit > allocationAccountUsedMask {
		return nil, ErrAllocationAccountInvalid
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.suspended {
		return nil, ErrAllocationAdmissionSuspended
	}
	for len(r.free) > 0 {
		index := len(r.free) - 1
		slot := r.free[index]
		r.free = r.free[:index]
		generation := r.generations[slot]
		if generation == math.MaxUint32 {
			continue
		}
		generation++
		r.generations[slot] = generation
		account := &AllocationAccount{
			registry: r,
			handle:   newAllocationAccountHandle(slot, generation),
			limit:    limit,
			control:  control,
		}
		r.slots[slot].account.Store(account)
		return account, nil
	}
	return nil, ErrAllocationGenerationSlots
}

// CompleteTerminal seals one generation and publishes its immutable terminal
// state exactly once. A nonzero terminal state remains resolvable as a
// release-capable tombstone and suspends new generations on this registry.
// The tombstone is removed automatically after the last physical Free.
//
// first is true only for the call that created the immutable snapshot. A
// repeated call while a tombstone is live returns the same snapshot with
// first=false.
func (r *AllocationAccountRegistry) CompleteTerminal(
	account *AllocationAccount,
) (snapshot AllocationAccountTerminalSnapshot, first bool, err error) {
	return r.CompleteTerminalWithError(account, nil)
}

// CompleteTerminalWithError additionally records an owner-lifecycle invariant
// discovered after physical producers quiesced. A zero-live failure is removed
// immediately (there is no provenance to retain); a nonzero failure follows
// the same tombstone/suspension path as CompleteTerminal.
func (r *AllocationAccountRegistry) CompleteTerminalWithError(
	account *AllocationAccount,
	terminalCause error,
) (snapshot AllocationAccountTerminalSnapshot, first bool, err error) {
	if r == nil || account == nil || account.registry != r {
		return snapshot, false, ErrAllocationAccountInvalid
	}
	account.Seal()

	r.mu.Lock()
	defer r.mu.Unlock()
	slot := account.handle.slot()
	if slot == 0 || uint64(slot) >= uint64(len(r.slots)) ||
		r.slots[slot].account.Load() != account {
		return snapshot, false, ErrAllocationAccountStale
	}
	entry := &r.slots[slot]
	if entry.terminal != nil {
		snapshot = *entry.terminal
		if snapshot.State == AllocationAccountTerminalInvariantFailure {
			return snapshot, false, newAllocationTerminalInvariantError(snapshot)
		}
		return snapshot, false, nil
	}

	current := account.Snapshot()
	if !current.Sealed || account.inflight.Load() != 0 {
		return AllocationAccountTerminalSnapshot{
				AllocationAccountSnapshot: current,
				State:                     AllocationAccountTerminalInvariantFailure,
			}, false, wrapAllocationAccountError(
				ErrAllocationAccountInvariant,
				"terminal account is not quiescent",
			)
	}
	snapshot = AllocationAccountTerminalSnapshot{
		AllocationAccountSnapshot: current,
		State:                     AllocationAccountTerminalValid,
	}
	if terminalCause != nil {
		snapshot.State = AllocationAccountTerminalInvariantFailure
	}
	if current.Used == 0 {
		entry.terminal = &snapshot
		r.removeSlotLocked(slot, account)
		if snapshot.State == AllocationAccountTerminalInvariantFailure {
			return snapshot, true, errors.Join(
				terminalCause,
				newAllocationTerminalInvariantError(snapshot),
			)
		}
		return snapshot, true, nil
	}

	snapshot.State = AllocationAccountTerminalInvariantFailure
	snapshot.LiveOwner, snapshot.LiveSite, snapshot.LiveAllocations =
		allocationAccountLiveDiagnostic(account)
	entry.terminal = &snapshot
	entry.tombstone = true
	r.tombstones++
	r.suspended = true
	// A physical Free may have raced the terminal observation. The immutable
	// failure snapshot remains truthful at its linearization point, while a
	// now-empty tombstone can be removed immediately.
	if account.Snapshot().Used == 0 && account.inflight.Load() == 0 {
		r.removeTombstoneLocked(slot, account)
	}
	return snapshot, true, errors.Join(
		terminalCause,
		newAllocationTerminalInvariantError(snapshot),
	)
}

func newAllocationTerminalInvariantError(
	snapshot AllocationAccountTerminalSnapshot,
) error {
	return wrapAllocationAccountError(
		ErrAllocationAccountInvariant,
		"handle=%d used=%d peak=%d limit=%d owner=%d site=%d live-allocations=%d",
		snapshot.Handle,
		snapshot.Used,
		snapshot.Peak,
		snapshot.Limit,
		snapshot.LiveOwner,
		snapshot.LiveSite,
		snapshot.LiveAllocations,
	)
}

// allocationAccountLiveDiagnostic is a terminal-only scan. It does not add a
// per-allocation hot-path counter: provenance already lives in the pointer
// metadata required for physical Free. The first live owner/site plus the
// exact live allocation count makes a nonzero terminal snapshot actionable.
func allocationAccountLiveDiagnostic(
	account *AllocationAccount,
) (AllocationOwner, AllocationSite, uint64) {
	if account == nil {
		return 0, 0, 0
	}
	var owner AllocationOwner
	var site AllocationSite
	var count uint64
	record := func(lease allocationLease) {
		if lease.account != account {
			return
		}
		if count == 0 {
			owner = lease.owner
			site = lease.site
		}
		count++
	}
	for i := range globalPtrShards {
		shard := &globalPtrShards[i]
		shard.mu.Lock()
		for _, lease := range shard.leases {
			record(lease)
		}
		shard.mu.Unlock()
	}
	// noLock pools intentionally provide no synchronization for their local
	// maps. Do not race unrelated single-threaded pools merely to enrich a
	// terminal error; production query pools use the sharded metadata above.
	return owner, site, count
}

func (r *AllocationAccountRegistry) removeSlotLocked(
	slot uint32,
	account *AllocationAccount,
) {
	entry := &r.slots[slot]
	if entry.account.Load() != account {
		return
	}
	entry.account.Store(nil)
	entry.terminal = nil
	entry.tombstone = false
	if r.generations[slot] != math.MaxUint32 {
		r.free = append(r.free, slot)
	}
}

func (r *AllocationAccountRegistry) removeTombstoneLocked(
	slot uint32,
	account *AllocationAccount,
) {
	entry := &r.slots[slot]
	if entry.account.Load() != account || !entry.tombstone {
		return
	}
	if r.tombstones == 0 {
		panic("allocation account tombstone underflow")
	}
	r.tombstones--
	r.removeSlotLocked(slot, account)
	r.suspended = r.tombstones != 0
}

func (r *AllocationAccountRegistry) tryDrainTombstone(
	account *AllocationAccount,
) {
	if r == nil || account == nil || account.Snapshot().Used != 0 ||
		account.inflight.Load() != 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	slot := account.handle.slot()
	if slot == 0 || uint64(slot) >= uint64(len(r.slots)) {
		return
	}
	if r.slots[slot].account.Load() == account &&
		r.slots[slot].tombstone &&
		account.Snapshot().Used == 0 &&
		account.inflight.Load() == 0 {
		r.removeTombstoneLocked(slot, account)
	}
}

func (r *AllocationAccountRegistry) AdmissionSuspended() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.suspended
}

func (r *AllocationAccountRegistry) LiveTombstones() uint32 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tombstones
}

func (r *AllocationAccountRegistry) Resolve(
	handle AllocationAccountHandle,
) (*AllocationAccount, bool) {
	if r == nil {
		return nil, false
	}
	slot := handle.slot()
	if slot == 0 || uint64(slot) >= uint64(len(r.slots)) {
		return nil, false
	}
	account := r.slots[slot].account.Load()
	return account, account != nil && account.handle == handle
}

// Finalize removes a sealed, empty account and makes its slot reusable. A live
// account remains resolvable so physical Free can still release its charge.
func (r *AllocationAccountRegistry) Finalize(
	account *AllocationAccount,
) (AllocationAccountSnapshot, error) {
	if r == nil || account == nil || account.registry != r {
		return AllocationAccountSnapshot{}, ErrAllocationAccountInvalid
	}
	snapshot := account.Snapshot()
	if !snapshot.Sealed || snapshot.Used != 0 ||
		account.inflight.Load() != 0 {
		return snapshot, ErrAllocationAccountLive
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	slot := account.handle.slot()
	if slot == 0 || uint64(slot) >= uint64(len(r.slots)) ||
		r.slots[slot].account.Load() != account {
		return snapshot, ErrAllocationAccountStale
	}
	if current := account.Snapshot(); !current.Sealed || current.Used != 0 ||
		account.inflight.Load() != 0 {
		return current, ErrAllocationAccountLive
	}
	r.removeSlotLocked(slot, account)
	return account.Snapshot(), nil
}

func (r *AllocationAccountRegistry) reserveMetadata() error {
	if r == nil {
		return ErrAllocationAccountInvalid
	}
	for {
		live := r.liveAllocations.Load()
		if live >= r.maxAllocations {
			return ErrAllocationMetadataSlots
		}
		if r.liveAllocations.CompareAndSwap(live, live+1) {
			next := live + 1
			for {
				peak := r.peakAllocations.Load()
				if next <= peak ||
					r.peakAllocations.CompareAndSwap(peak, next) {
					break
				}
			}
			return nil
		}
	}
}

func (r *AllocationAccountRegistry) releaseMetadata() {
	for {
		live := r.liveAllocations.Load()
		if live == 0 {
			panic("allocation metadata slot release underflow")
		}
		if r.liveAllocations.CompareAndSwap(live, live-1) {
			return
		}
	}
}

// LiveAllocationMetadata returns published and currently in-flight metadata
// slots. A failed unpublished transaction returns its slot before returning.
func (r *AllocationAccountRegistry) LiveAllocationMetadata() uint64 {
	if r == nil {
		return 0
	}
	return r.liveAllocations.Load()
}

// PeakAllocationMetadata returns the exact high-water slot count.
func (r *AllocationAccountRegistry) PeakAllocationMetadata() uint64 {
	if r == nil {
		return 0
	}
	return r.peakAllocations.Load()
}

func (r *AllocationAccountRegistry) MaxAllocationMetadata() uint64 {
	if r == nil {
		return 0
	}
	return r.maxAllocations
}

func (r *AllocationAccountRegistry) GenerationCapacity() uint32 {
	if r == nil || len(r.slots) == 0 {
		return 0
	}
	return uint32(len(r.slots) - 1)
}

type allocationAccountRequest struct {
	account *AllocationAccount
	owner   AllocationOwner
	site    AllocationSite
	// checkpoint is nil for every public caller. Same-package fault tests use
	// it to prove rollback at each unpublished transaction boundary.
	checkpoint func(allocationCheckpoint) error
}

type allocationCheckpoint uint8

const (
	allocationAfterAccount allocationCheckpoint = iota + 1
	allocationAfterMetadata
	allocationAfterGlobalStats
	allocationAfterPoolStats
	allocationAfterPhysical
	allocationAfterHeader
)

func (r allocationAccountRequest) validate() error {
	if r.account == nil || r.account.registry == nil ||
		r.owner < AllocationOwnerMin || r.owner > AllocationOwnerMax ||
		r.site < AllocationSiteMin {
		return ErrAllocationAccountInvalid
	}
	resolved, ok := r.account.registry.Resolve(r.account.handle)
	if !ok || resolved != r.account {
		return ErrAllocationAccountStale
	}
	return nil
}

func (r allocationAccountRequest) reach(
	checkpoint allocationCheckpoint,
) error {
	if r.checkpoint == nil {
		return nil
	}
	return r.checkpoint(checkpoint)
}

type allocationLease struct {
	account *AllocationAccount
	owner   AllocationOwner
	site    AllocationSite
	_       [6]byte
}

func (l allocationLease) release(capacity uint64) {
	if l.account == nil || l.account.registry == nil {
		panic("invalid allocation account lease")
	}
	// Return finite metadata first. account.release retains the local charge
	// until controller cleanup completes, so exact zero is a complete-release
	// boundary.
	l.account.registry.releaseMetadata()
	l.account.release(capacity)
}
