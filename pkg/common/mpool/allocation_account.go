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
	"slices"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// AllocationOwner and AllocationSite are bounded diagnostic dimensions.
// Owners come from the repository catalog; packages assign stable sites in
// the range reserved for that owner. Zero is reserved so an accounted
// allocation can never be published without explicit provenance.
type AllocationOwner uint8
type AllocationSite uint8

const (
	AllocationOwnerMin AllocationOwner = 1
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

// AllocationAccountOwnerSnapshot is exact, bounded observation state for one
// repository owner class. Current is live capacity at the snapshot boundary;
// Peak is that owner's high-water capacity within the generation.
type AllocationAccountOwnerSnapshot struct {
	Owner   AllocationOwner
	Current uint64
	Peak    uint64
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
	case errors.Is(err, ErrAllocationAccountInvalid),
		errors.Is(err, ErrAllocationAccountStale),
		errors.Is(err, ErrAllocationAccountLive),
		errors.Is(err, ErrAllocationGenerationSlots):
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
		errors.Is(err, ErrAllocationMetadataSlots),
		IsMPoolCapacityFailure(err):
		return AllocationFailureCapacity
	default:
		return AllocationFailureNone
	}
}

// IsMPoolCapacityFailure recognizes both a direct MO error and a contextual
// wrapper retained by an intermediate owner.
func IsMPoolCapacityFailure(err error) bool {
	var moErr *moerr.Error
	return errors.As(err, &moErr) && moErr.ErrorCode() == moerr.ErrMPoolCapacity
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
	Owners          []AllocationAccountOwnerSnapshot
	LiveOwner       AllocationOwner
	LiveSite        AllocationSite
	LiveAllocations uint64
}

// AllocationCapacityController lets an account share a higher-level aggregate
// cap. The controller owns cap policy only; physical MPool metadata remains
// the sole release owner.
type AllocationCapacityController interface {
	AcquireAllocationCapacity(uint64) error
	ReleaseAllocationCapacity(uint64)
}

// AllocationCapacityClass selects a capacity controller for one physical
// allocation. Class zero uses the account's statement controller. Non-zero
// classes are registered by execution owners whose future recovery storage
// must borrow pre-admitted headroom without charging that headroom twice.
// The namespace must accommodate every parallel HashBuild in one statement.
type AllocationCapacityClass uint32

// AllocationCapacityClassDefault uses the statement's ordinary controller.
const AllocationCapacityClassDefault AllocationCapacityClass = 0

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

	ownerUsage [AllocationOwnerCatalogMax + 1]allocationAccountOwnerUsage

	capacityMu          sync.Mutex
	capacityControllers map[AllocationCapacityClass]*allocationCapacityRegistration
	nextCapacityClass   AllocationCapacityClass
}

type allocationCapacityRegistration struct {
	control AllocationCapacityController
	used    uint64
}

type allocationAccountOwnerUsage struct {
	current atomic.Uint64
	peak    atomic.Uint64
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

// OwnerUsage returns one owner's current and peak capacities. The two fields
// are an observational pair; callers requiring a generation-wide invariant
// must use the immutable terminal snapshot after producers quiesce.
func (a *AllocationAccount) OwnerUsage(
	owner AllocationOwner,
) (AllocationAccountOwnerSnapshot, bool) {
	if a == nil || owner < AllocationOwnerMin || owner > AllocationOwnerCatalogMax {
		return AllocationAccountOwnerSnapshot{}, false
	}
	return AllocationAccountOwnerSnapshot{
		Owner:   owner,
		Current: a.ownerUsage[owner].current.Load(),
		Peak:    a.ownerUsage[owner].peak.Load(),
	}, true
}

// RegisterCapacityController installs one execution-local capacity class. The
// owner must unregister it only after every allocation in the class is freed.
func (a *AllocationAccount) RegisterCapacityController(
	control AllocationCapacityController,
) (AllocationCapacityClass, error) {
	if a == nil || control == nil || a.registry == nil || a.handle == 0 {
		return AllocationCapacityClassDefault, ErrAllocationAccountInvalid
	}
	a.inflight.Add(1)
	defer a.inflight.Add(-1)
	if a.state.Load()&allocationAccountSealedBit != 0 {
		return AllocationCapacityClassDefault, ErrAllocationAccountSealed
	}
	a.capacityMu.Lock()
	defer a.capacityMu.Unlock()
	if a.state.Load()&allocationAccountSealedBit != 0 {
		return AllocationCapacityClassDefault, ErrAllocationAccountSealed
	}
	if a.capacityControllers == nil {
		a.capacityControllers = make(
			map[AllocationCapacityClass]*allocationCapacityRegistration,
		)
	}
	for range uint64(math.MaxUint32) {
		a.nextCapacityClass++
		if a.nextCapacityClass == AllocationCapacityClassDefault {
			a.nextCapacityClass++
		}
		if _, exists := a.capacityControllers[a.nextCapacityClass]; !exists {
			a.capacityControllers[a.nextCapacityClass] =
				&allocationCapacityRegistration{control: control}
			return a.nextCapacityClass, nil
		}
	}
	return AllocationCapacityClassDefault, ErrAllocationAccountInvariant
}

// UnregisterCapacityController removes a quiescent execution-local class.
func (a *AllocationAccount) UnregisterCapacityController(
	class AllocationCapacityClass,
	control AllocationCapacityController,
) error {
	if a == nil || class == AllocationCapacityClassDefault || control == nil {
		return ErrAllocationAccountInvalid
	}
	a.capacityMu.Lock()
	defer a.capacityMu.Unlock()
	registration := a.capacityControllers[class]
	if registration == nil || registration.control != control {
		return ErrAllocationAccountMismatch
	}
	if registration.used != 0 {
		return ErrAllocationAccountLive
	}
	delete(a.capacityControllers, class)
	if len(a.capacityControllers) == 0 {
		a.capacityControllers = nil
	}
	return nil
}

// capacityControllerLocked requires capacityMu to be held, keeping a
// non-default registration stable through the complete acquire/release.
func (a *AllocationAccount) capacityControllerLocked(
	class AllocationCapacityClass,
) (*allocationCapacityRegistration, error) {
	if class == AllocationCapacityClassDefault {
		return nil, ErrAllocationAccountInvalid
	}
	registration := a.capacityControllers[class]
	if registration == nil {
		return nil, ErrAllocationAccountInvalid
	}
	return registration, nil
}

func (a *AllocationAccount) acquire(
	capacity uint64,
	owner AllocationOwner,
) error {
	return a.acquireWithCapacityClass(
		capacity,
		AllocationCapacityClassDefault,
		owner,
	)
}

func (a *AllocationAccount) acquireWithCapacityClass(
	capacity uint64,
	class AllocationCapacityClass,
	owner AllocationOwner,
) error {
	if a == nil || a.registry == nil || a.handle == 0 ||
		owner < AllocationOwnerMin || owner > AllocationOwnerCatalogMax {
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
	control := a.control
	var registration *allocationCapacityRegistration
	if class != AllocationCapacityClassDefault {
		a.capacityMu.Lock()
		var err error
		registration, err = a.capacityControllerLocked(class)
		if err != nil {
			a.capacityMu.Unlock()
			return err
		}
		control = registration.control
		defer a.capacityMu.Unlock()
	}
	if control != nil {
		if err := control.AcquireAllocationCapacity(capacity); err != nil {
			return err
		}
	}
	acquired := false
	defer func() {
		if !acquired && control != nil {
			control.ReleaseAllocationCapacity(capacity)
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
			if registration != nil {
				registration.used += capacity
			}
			ownerUsage := &a.ownerUsage[owner]
			ownerCurrent := ownerUsage.current.Add(capacity)
			raiseAllocationAccountPeak(&ownerUsage.peak, ownerCurrent)
			raiseAllocationAccountPeak(&a.peak, next)
			acquired = true
			return nil
		}
	}
}

func raiseAllocationAccountPeak(peak *atomic.Uint64, current uint64) {
	for {
		value := peak.Load()
		if current <= value {
			return
		}
		if peak.CompareAndSwap(value, current) {
			return
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

func (a *AllocationAccount) release(
	capacity uint64,
	owner AllocationOwner,
) {
	a.releaseWithCapacityClass(
		capacity,
		AllocationCapacityClassDefault,
		owner,
	)
}

func (a *AllocationAccount) releaseWithCapacityClass(
	capacity uint64,
	class AllocationCapacityClass,
	owner AllocationOwner,
) {
	if capacity == 0 {
		return
	}
	if a == nil || owner < AllocationOwnerMin || owner > AllocationOwnerCatalogMax {
		panic("invalid allocation account owner")
	}
	// Keep the local charge until the higher-level policy charge is gone. With
	// metadata released by allocationLease first, exact local zero is therefore
	// also a complete-release boundary.
	control := a.control
	var registration *allocationCapacityRegistration
	capacityLocked := false
	if class != AllocationCapacityClassDefault {
		a.capacityMu.Lock()
		capacityLocked = true
		var err error
		registration, err = a.capacityControllerLocked(class)
		if err != nil {
			a.capacityMu.Unlock()
			capacityLocked = false
			panic(err)
		}
		control = registration.control
		defer func() {
			if capacityLocked {
				a.capacityMu.Unlock()
			}
		}()
	}
	state := a.state.Load()
	ownerUsage := &a.ownerUsage[owner]
	ownerCurrent := ownerUsage.current.Load()
	if capacity > state&allocationAccountUsedMask || capacity > ownerCurrent {
		panic("allocation account release underflow")
	}
	if registration != nil && capacity > registration.used {
		panic("allocation capacity class release underflow")
	}
	// Validation must precede inflight registration: callers deliberately
	// recover release-underflow panics in invariant tests, and a rejected
	// release must not leave Seal waiting for work that never started.
	a.inflight.Add(1)
	if control != nil {
		releaseAllocationCapacity(control, capacity, &a.inflight)
	}
	for {
		if capacity > ownerCurrent {
			a.inflight.Add(-1)
			panic("allocation account owner release underflow")
		}
		if ownerUsage.current.CompareAndSwap(
			ownerCurrent,
			ownerCurrent-capacity,
		) {
			break
		}
		ownerCurrent = ownerUsage.current.Load()
	}
	becameZero := false
	for {
		used := state & allocationAccountUsedMask
		if capacity > used {
			a.inflight.Add(-1)
			panic("allocation account release underflow")
		}
		next := state - capacity
		if a.state.CompareAndSwap(state, next) {
			if registration != nil {
				registration.used -= capacity
			}
			becameZero = next&allocationAccountUsedMask == 0
			break
		}
		state = a.state.Load()
	}
	a.inflight.Add(-1)
	// Keep registry and capacity-controller locking independent. In particular,
	// tombstone draining must never wait for the registry while holding
	// capacityMu needed by unregister and terminal inspection.
	if capacityLocked {
		a.capacityMu.Unlock()
		capacityLocked = false
	}
	if becameZero {
		a.registry.tryDrainTombstone(a)
	}
}

func releaseAllocationCapacity(
	control AllocationCapacityController,
	capacity uint64,
	inflight *atomic.Int64,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			inflight.Add(-1)
			panic(recovered)
		}
	}()
	control.ReleaseAllocationCapacity(capacity)
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

// terminalOwnerSnapshot captures total and per-owner state at one quiescent
// point. Seal prevents new acquisitions; releases register in inflight, so a
// zero-before/zero-after observation is coherent even when a late physical
// Free is draining an invariant-failure tombstone.
func (a *AllocationAccount) terminalOwnerSnapshot() (
	AllocationAccountSnapshot,
	[]AllocationAccountOwnerSnapshot,
	error,
) {
	if a == nil {
		return AllocationAccountSnapshot{}, nil, ErrAllocationAccountInvalid
	}
	var fixed [AllocationOwnerCatalogMax]AllocationAccountOwnerSnapshot
	for {
		for a.inflight.Load() != 0 {
			runtime.Gosched()
		}
		state := a.state.Load()
		peak := a.peak.Load()
		count := 0
		var ownerCurrentTotal uint64
		var ownerInvariant bool
		for owner := AllocationOwnerMin; owner <= AllocationOwnerCatalogMax; owner++ {
			ownerUsage := &a.ownerUsage[owner]
			current := ownerUsage.current.Load()
			ownerPeak := ownerUsage.peak.Load()
			if current > ownerPeak || ownerPeak > peak ||
				ownerCurrentTotal > math.MaxUint64-current {
				ownerInvariant = true
			} else {
				ownerCurrentTotal += current
			}
			if current != 0 || ownerPeak != 0 {
				fixed[count] = AllocationAccountOwnerSnapshot{
					Owner:   owner,
					Current: current,
					Peak:    ownerPeak,
				}
				count++
			}
		}
		if peak != 0 && count == 0 {
			ownerInvariant = true
		}
		if a.inflight.Load() != 0 || a.state.Load() != state ||
			a.peak.Load() != peak {
			continue
		}
		current := AllocationAccountSnapshot{
			Handle: a.handle,
			Limit:  a.limit,
			Used:   state & allocationAccountUsedMask,
			Peak:   peak,
			Sealed: state&allocationAccountSealedBit != 0,
		}
		var owners []AllocationAccountOwnerSnapshot
		if count != 0 {
			owners = make([]AllocationAccountOwnerSnapshot, count)
			copy(owners, fixed[:count])
		}
		if ownerInvariant || ownerCurrentTotal != current.Used {
			return current, owners, wrapAllocationAccountError(
				ErrAllocationAccountInvariant,
				"owner current total=%d account used=%d",
				ownerCurrentTotal,
				current.Used,
			)
		}
		return current, owners, nil
	}
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
	// Owner capture is bounded but intentionally outside the registry's shared
	// lock. A concurrent terminal publisher can make this work redundant, but
	// cannot mutate peak after Seal; releases already participate in the
	// snapshot's inflight handshake.
	current, owners, ownerErr := account.terminalOwnerSnapshot()
	account.capacityMu.Lock()
	liveCapacityControllers := len(account.capacityControllers)
	account.capacityMu.Unlock()
	return r.publishTerminalSnapshot(
		account,
		current,
		owners,
		ownerErr,
		liveCapacityControllers,
		terminalCause,
	)
}

// publishTerminalSnapshot commits a coherent snapshot captured after Seal.
// A physical Free may start after that capture. The snapshot remains a valid
// terminal linearization point; publishing its nonzero state as a tombstone
// lets that Free drain the generation instead of stranding a sealed slot.
func (r *AllocationAccountRegistry) publishTerminalSnapshot(
	account *AllocationAccount,
	current AllocationAccountSnapshot,
	owners []AllocationAccountOwnerSnapshot,
	ownerErr error,
	liveCapacityControllers int,
	terminalCause error,
) (snapshot AllocationAccountTerminalSnapshot, first bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	slot := account.handle.slot()
	if slot == 0 || uint64(slot) >= uint64(len(r.slots)) ||
		r.slots[slot].account.Load() != account {
		return snapshot, false, ErrAllocationAccountStale
	}
	entry := &r.slots[slot]
	if entry.terminal != nil {
		snapshot = cloneAllocationAccountTerminalSnapshot(*entry.terminal)
		if snapshot.State == AllocationAccountTerminalInvariantFailure {
			return snapshot, false, newAllocationTerminalInvariantError(snapshot)
		}
		return snapshot, false, nil
	}

	terminalCause = errors.Join(terminalCause, ownerErr)
	if !current.Sealed {
		return AllocationAccountTerminalSnapshot{
				AllocationAccountSnapshot: current,
				State:                     AllocationAccountTerminalInvariantFailure,
				Owners:                    owners,
			}, false, wrapAllocationAccountError(
				ErrAllocationAccountInvariant,
				"terminal account is not quiescent",
			)
	}
	if liveCapacityControllers != 0 {
		terminalCause = errors.Join(
			terminalCause,
			wrapAllocationAccountError(
				ErrAllocationAccountInvariant,
				"terminal account retains %d capacity controllers",
				liveCapacityControllers,
			),
		)
	}
	snapshot = AllocationAccountTerminalSnapshot{
		AllocationAccountSnapshot: current,
		State:                     AllocationAccountTerminalValid,
		Owners:                    owners,
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
	stored := cloneAllocationAccountTerminalSnapshot(snapshot)
	entry.terminal = &stored
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

func cloneAllocationAccountTerminalSnapshot(
	snapshot AllocationAccountTerminalSnapshot,
) AllocationAccountTerminalSnapshot {
	snapshot.Owners = slices.Clone(snapshot.Owners)
	return snapshot
}

func newAllocationTerminalInvariantError(
	snapshot AllocationAccountTerminalSnapshot,
) error {
	return wrapAllocationAccountError(
		ErrAllocationAccountInvariant,
		"handle=%d used=%d peak=%d limit=%d owner=%d site=%d live-allocations=%d owner-name=%s",
		snapshot.Handle,
		snapshot.Used,
		snapshot.Peak,
		snapshot.Limit,
		snapshot.LiveOwner,
		snapshot.LiveSite,
		snapshot.LiveAllocations,
		snapshot.LiveOwner,
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
	account       *AllocationAccount
	owner         AllocationOwner
	site          AllocationSite
	capacityClass AllocationCapacityClass
}

func (r allocationAccountRequest) validate() error {
	if r.account == nil || r.account.registry == nil ||
		r.owner < AllocationOwnerMin || r.owner > AllocationOwnerCatalogMax ||
		r.site < AllocationSiteMin {
		return ErrAllocationAccountInvalid
	}
	resolved, ok := r.account.registry.Resolve(r.account.handle)
	if !ok || resolved != r.account {
		return ErrAllocationAccountStale
	}
	return nil
}

type allocationLease struct {
	account       *AllocationAccount
	owner         AllocationOwner
	site          AllocationSite
	profiled      bool
	capacityClass AllocationCapacityClass
}

func (l allocationLease) release(capacity uint64) {
	if l.account == nil || l.account.registry == nil {
		panic("invalid allocation account lease")
	}
	// Return finite metadata first. account.release retains the local charge
	// until controller cleanup completes, so exact zero is a complete-release
	// boundary.
	l.account.registry.releaseMetadata()
	l.account.releaseWithCapacityClass(capacity, l.capacityClass, l.owner)
}
