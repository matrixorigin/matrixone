// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const hashBuildMinimumReserve = uint64(4 << 30)

var (
	hashBuildGenerationSequence atomic.Uint64
	hashBuildCNBudgets          sync.Map // service ID -> *HashBuildBudget
)

// Errors returned by hash-build admission.  These errors intentionally live in
// process rather than the SQL layer so that operators and remote execution
// code can make an admission decision without importing frontend packages.
var (
	ErrHashBuildBudgetAdmission = errors.New("hash build budget admission rejected")
	// ErrHashBuildBudgetRejected is kept as a more discoverable spelling of the
	// admission sentinel.  It is the same value, so errors.Is works with either.
	ErrHashBuildBudgetRejected    = ErrHashBuildBudgetAdmission
	ErrHashBuildBudgetClosed      = errors.New("hash build budget is closed")
	ErrHashBuildBudgetInvalid     = errors.New("invalid hash build budget")
	ErrHashBuildCeilingMissing    = errors.New("hash build budget ceiling unavailable")
	ErrHashBuildBudgetUnavailable = ErrHashBuildCeilingMissing
)

// HashBuildBudgetErrorKind identifies the class of a budget error.
type HashBuildBudgetErrorKind uint8

const (
	HashBuildBudgetErrorAdmission HashBuildBudgetErrorKind = iota + 1
	HashBuildBudgetErrorClosed
	HashBuildBudgetErrorInvalid
	HashBuildBudgetErrorCeilingMissing
)

// HashBuildBudgetError carries bounded, observational details for an
// admission failure.  Requested, Used and Cap are bytes and are always safe
// to inspect (they are never produced by overflowing arithmetic).
type HashBuildBudgetError struct {
	Kind      HashBuildBudgetErrorKind
	Requested uint64
	Used      uint64
	Cap       uint64
	Message   string
}

func (e *HashBuildBudgetError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	switch e.Kind {
	case HashBuildBudgetErrorClosed:
		return ErrHashBuildBudgetClosed.Error()
	case HashBuildBudgetErrorInvalid:
		return ErrHashBuildBudgetInvalid.Error()
	case HashBuildBudgetErrorCeilingMissing:
		return ErrHashBuildCeilingMissing.Error()
	default:
		return fmt.Sprintf("%s: requested=%d used=%d cap=%d", ErrHashBuildBudgetAdmission, e.Requested, e.Used, e.Cap)
	}
}

func (e *HashBuildBudgetError) Unwrap() error {
	if e == nil {
		return nil
	}
	switch e.Kind {
	case HashBuildBudgetErrorClosed:
		return ErrHashBuildBudgetClosed
	case HashBuildBudgetErrorInvalid:
		return ErrHashBuildBudgetInvalid
	case HashBuildBudgetErrorCeilingMissing:
		return ErrHashBuildCeilingMissing
	default:
		return ErrHashBuildBudgetAdmission
	}
}

// Is lets admission handling classify both a cap rejection and a closed
// budget without depending on the concrete error type, while retaining the
// more specific closed sentinel for lifecycle diagnostics.
func (e *HashBuildBudgetError) Is(target error) bool {
	if e == nil {
		return false
	}
	if target == ErrHashBuildBudgetAdmission || target == ErrHashBuildBudgetRejected {
		return e.Kind == HashBuildBudgetErrorAdmission || e.Kind == HashBuildBudgetErrorClosed
	}
	switch e.Kind {
	case HashBuildBudgetErrorClosed:
		return target == ErrHashBuildBudgetClosed
	case HashBuildBudgetErrorInvalid:
		return target == ErrHashBuildBudgetInvalid
	case HashBuildBudgetErrorCeilingMissing:
		return target == ErrHashBuildCeilingMissing
	}
	return false
}

// HashBuildBudget is a local-CN aggregate budget.  Each opened generation has
// its own query-CN cap, while all generations charge this aggregate cap.  The
// mutex is deliberately held over the two-level reservation sequence and
// closure transitions.  This gives the operation a simple linearization point
// and, importantly, makes a query rejection roll back its complete CN charge.
type HashBuildBudget struct {
	mu        sync.Mutex
	refreshMu sync.Mutex

	aggregateCap  uint64
	aggregateUsed uint64
	queryCap      uint64
	capProvider   func() (uint64, error)
	closed        bool
}

// NewHashBuildBudget creates a local-CN budget.  Both caps are finite and
// positive; queryCap is the cap for each statement execution generation and
// aggregateCap is shared by all generations on this CN.
func NewHashBuildBudget(aggregateCap, queryCap uint64) (*HashBuildBudget, error) {
	if aggregateCap == 0 || queryCap == 0 || queryCap > aggregateCap {
		return nil, &HashBuildBudgetError{
			Kind:      HashBuildBudgetErrorInvalid,
			Requested: queryCap,
			Cap:       aggregateCap,
			Message:   fmt.Sprintf("%s: aggregate=%d query=%d", ErrHashBuildBudgetInvalid, aggregateCap, queryCap),
		}
	}
	return &HashBuildBudget{aggregateCap: aggregateCap, queryCap: queryCap}, nil
}

// MustNewHashBuildBudget is a convenience for initialization code with
// statically validated limits.
func MustNewHashBuildBudget(aggregateCap, queryCap uint64) *HashBuildBudget {
	b, err := NewHashBuildBudget(aggregateCap, queryCap)
	if err != nil {
		panic(err)
	}
	return b
}

// AggregateCap returns the configured local-CN cap.
func (b *HashBuildBudget) AggregateCap() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.aggregateCap
}

// CNHashCap is an alias useful to callers that describe the aggregate as the
// CN hash cap.
func (b *HashBuildBudget) CNHashCap() uint64 { return b.AggregateCap() }

// QueryCap returns the per-generation, per-target-CN cap.
func (b *HashBuildBudget) QueryCap() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.queryCap
}

// AggregateUsed reports bytes currently charged by all live generations.
func (b *HashBuildBudget) AggregateUsed() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.aggregateUsed
}

// CNHashUsed is an alias for AggregateUsed.
func (b *HashBuildBudget) CNHashUsed() uint64 { return b.AggregateUsed() }

// Current is a concise alias for AggregateUsed.
func (b *HashBuildBudget) Current() uint64 { return b.AggregateUsed() }

// Capacity is a concise alias for AggregateCap.
func (b *HashBuildBudget) Capacity() uint64 { return b.AggregateCap() }

// Closed reports whether no new generation or reservation may be opened.
func (b *HashBuildBudget) Closed() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// Close prevents future generations and reservations.  It does not refund
// live tokens; those tokens retain ownership of this budget and release their
// original generation charge normally.
func (b *HashBuildBudget) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
}

// UpdateAggregateCap applies a refreshed physical ceiling. If current usage
// is above a reduced cap, all new reservations fail until releases bring it
// back under the new limit.
func (b *HashBuildBudget) UpdateAggregateCap(cap uint64) error {
	if b == nil || cap == 0 {
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Requested: cap}
	}
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	b.aggregateCap = cap
	if b.queryCap > cap {
		b.queryCap = cap
	}
	b.mu.Unlock()
	return nil
}

// SetAggregateCapProvider installs the live physical-ceiling source consulted
// before every reservation. The provider must not call back into this budget.
// It lets an already-open generation observe cgroup/cache ceiling changes
// without waiting for another statement to create a generation.
func (b *HashBuildBudget) SetAggregateCapProvider(provider func() (uint64, error)) {
	if b == nil {
		return
	}
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	b.capProvider = provider
	b.mu.Unlock()
}

// HashBuildBudgetGeneration is a statement execution generation on one CN.
// A generation's charge is independent from every other generation, even if a
// caller happens to reuse its numeric ID after the old generation has closed.
type HashBuildBudgetGeneration struct {
	budget *HashBuildBudget
	id     uint64
	cap    uint64
	used   uint64
	closed bool
}

// HashBuildGeneration is a shorter spelling retained for call sites.
type HashBuildGeneration = HashBuildBudgetGeneration

// HashBuildQueryBudget makes the per-generation/per-target-CN scope explicit
// at call sites.  It is an alias, so tokens and methods retain one ownership
// implementation.
type HashBuildQueryBudget = HashBuildBudgetGeneration

// OpenGeneration opens a per-statement execution generation.  The budget's
// query cap is copied by reference (and remains immutable), while used bytes
// belong solely to the returned generation.
func (b *HashBuildBudget) OpenGeneration(id uint64) (*HashBuildBudgetGeneration, error) {
	if b == nil {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Message: "nil hash build budget"}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Message: ErrHashBuildBudgetClosed.Error()}
	}
	return &HashBuildBudgetGeneration{budget: b, id: id, cap: b.queryCap}, nil
}

// OpenGenerationWithCap opens a generation with a query-specific cap while
// retaining the same CN aggregate. This is used when process.Limitation.Size
// narrows one statement below the CN default.
func (b *HashBuildBudget) OpenGenerationWithCap(id, cap uint64) (*HashBuildBudgetGeneration, error) {
	if b == nil || cap == 0 {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Requested: cap, Message: "invalid hash build generation cap"}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Message: ErrHashBuildBudgetClosed.Error()}
	}
	if cap > b.aggregateCap {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Requested: cap, Cap: b.aggregateCap}
	}
	return &HashBuildBudgetGeneration{budget: b, id: id, cap: cap}, nil
}

// NewGeneration is an alias for OpenGeneration.
func (b *HashBuildBudget) NewGeneration(id uint64) (*HashBuildBudgetGeneration, error) {
	return b.OpenGeneration(id)
}

// OpenQueryBudget is the explicit per-query-CN spelling of OpenGeneration.
func (b *HashBuildBudget) OpenQueryBudget(id uint64) (*HashBuildQueryBudget, error) {
	return b.OpenGeneration(id)
}

// ID returns the execution generation identity.
func (g *HashBuildBudgetGeneration) ID() uint64 {
	if g == nil {
		return 0
	}
	return g.id
}

// Cap returns this generation's query-CN cap.
func (g *HashBuildBudgetGeneration) Cap() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	return g.cap
}

// QueryCap returns this generation's query-CN cap.
func (g *HashBuildBudgetGeneration) QueryCap() uint64 { return g.Cap() }

// Capacity is a concise alias for Cap.
func (g *HashBuildBudgetGeneration) Capacity() uint64 { return g.Cap() }

// Used reports bytes reserved by this generation.
func (g *HashBuildBudgetGeneration) Used() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.used
}

// Current is a concise alias for Used.
func (g *HashBuildBudgetGeneration) Current() uint64 { return g.Used() }

// Closed reports whether this generation rejects new reservations.
func (g *HashBuildBudgetGeneration) Closed() bool {
	if g == nil || g.budget == nil {
		return true
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.closed || g.budget.closed
}

// Close rejects future reservations for this generation while allowing all
// currently live tokens to release.  It is idempotent.
func (g *HashBuildBudgetGeneration) Close() {
	if g == nil || g.budget == nil {
		return
	}
	g.budget.mu.Lock()
	g.closed = true
	g.budget.mu.Unlock()
}

// Reserve performs the required two-level sequence: charge CN aggregate,
// then charge query-CN.  If query-CN rejects, aggregate is rolled back before
// returning, so callers never observe a partial reservation.
func (g *HashBuildBudgetGeneration) Reserve(size uint64) (*HashBuildReservation, error) {
	if g == nil || g.budget == nil {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Message: "nil hash build generation"}
	}
	b := g.budget
	// Serialize live-ceiling sampling through admission. Otherwise an older
	// concurrent provider result can overwrite a newer shrink immediately
	// before reserving and temporarily reopen unsafe headroom.
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	provider := b.capProvider
	b.mu.Unlock()
	if provider != nil {
		cap, err := provider()
		if err != nil {
			return nil, err
		}
		if cap == 0 {
			return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorCeilingMissing, Message: "live hash build budget ceiling is zero"}
		}
		b.mu.Lock()
		b.aggregateCap = cap
		if b.queryCap > cap {
			b.queryCap = cap
		}
		b.mu.Unlock()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || g.closed {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: size, Used: g.used, Cap: g.cap}
	}

	// Check by subtraction rather than used+size: this is safe for
	// math.MaxUint64 and rejects every overflow-sized request.
	if b.aggregateUsed > b.aggregateCap || size > b.aggregateCap-b.aggregateUsed {
		return nil, newAdmissionError(size, b.aggregateUsed, b.aggregateCap)
	}
	b.aggregateUsed += size

	if g.used > g.cap || size > g.cap-g.used {
		// Roll back the complete CN charge before returning the rejection.
		b.aggregateUsed -= size
		return nil, newAdmissionError(size, g.used, g.cap)
	}
	g.used += size
	return &HashBuildReservation{budget: b, generation: g, size: size, state: &atomic.Uint32{}}, nil
}

// TryReserve is a boolean convenience for admission-only call sites.
func (g *HashBuildBudgetGeneration) TryReserve(size uint64) bool {
	t, err := g.Reserve(size)
	if err != nil {
		return false
	}
	// A TryReserve caller has no token to retain; immediately release it.  Use
	// Release rather than manually decrementing to preserve exactly-once state.
	t.Release()
	return true
}

func newAdmissionError(requested, used, cap uint64) error {
	return &HashBuildBudgetError{
		Kind:      HashBuildBudgetErrorAdmission,
		Requested: requested,
		Used:      used,
		Cap:       cap,
		Message:   fmt.Sprintf("%s: requested=%d used=%d cap=%d", ErrHashBuildBudgetAdmission, requested, used, cap),
	}
}

// HashBuildReservation is an exactly-once ownership token for one charge in
// both the CN aggregate and its generation.  State transitions are atomic:
// active -> released or active -> transferred.  A late release therefore
// always affects the original generation and can never decrement a newer one.
type HashBuildReservation struct {
	budget     *HashBuildBudget
	generation *HashBuildBudgetGeneration
	size       uint64
	// state is a pointer rather than an embedded atomic so accidental copies of
	// the token still share exactly-once ownership state.
	state *atomic.Uint32
}

const (
	hashBuildReservationActive uint32 = iota
	hashBuildReservationReleased
	hashBuildReservationTransferred
)

// Size returns the reservation's immutable charge.
func (r *HashBuildReservation) Size() uint64 {
	if r == nil {
		return 0
	}
	return r.size
}

// GenerationID returns the generation charged by this token.
func (r *HashBuildReservation) GenerationID() uint64 {
	if r == nil || r.generation == nil {
		return 0
	}
	return r.generation.id
}

// Released reports whether this token has relinquished its ownership.  A
// transferred token is not released, but no longer owns the charge.
func (r *HashBuildReservation) Released() bool {
	return r == nil || r.state == nil || r.state.Load() != hashBuildReservationActive
}

// Release relinquishes this token once.  It returns true only for the caller
// that won the active -> released transition.
func (r *HashBuildReservation) Release() bool {
	if r == nil || r.state == nil || !r.state.CompareAndSwap(hashBuildReservationActive, hashBuildReservationReleased) {
		return false
	}
	if r.budget != nil && r.generation != nil {
		r.budget.mu.Lock()
		// The subtraction is exact for a live token.  Keep a defensive branch so
		// corrupted state cannot underflow and turn into an apparent huge charge.
		if r.generation.used >= r.size {
			r.generation.used -= r.size
		} else {
			r.generation.used = 0
		}
		if r.budget.aggregateUsed >= r.size {
			r.budget.aggregateUsed -= r.size
		} else {
			r.budget.aggregateUsed = 0
		}
		r.budget.mu.Unlock()
	}
	return true
}

// Transfer moves ownership to a fresh token exactly once.  The original token
// becomes inert; releasing it after a successful transfer cannot decrement the
// budget.  If Release wins the race, Transfer returns nil.
func (r *HashBuildReservation) Transfer() *HashBuildReservation {
	if r == nil || r.state == nil || !r.state.CompareAndSwap(hashBuildReservationActive, hashBuildReservationTransferred) {
		return nil
	}
	return &HashBuildReservation{budget: r.budget, generation: r.generation, size: r.size, state: &atomic.Uint32{}}
}

// TransferOwnership is a descriptive alias for Transfer.
func (r *HashBuildReservation) TransferOwnership() *HashBuildReservation { return r.Transfer() }

// TransferTo is another descriptive spelling for ownership transfer.
func (r *HashBuildReservation) TransferTo() *HashBuildReservation { return r.Transfer() }

// HashBuildCeilingInputs are the finite resource sources used by
// ResolveHashBuildCeiling.  A zero or math.MaxUint64 source means unavailable
// or unlimited and is excluded from the minimum.  No OS probing occurs here;
// callers provide values obtained from their environment.
type HashBuildCeilingInputs struct {
	CgroupMemoryMax uint64
	HostMemTotal    uint64
	GlobalMpoolCap  uint64
	FileCacheHint   uint64
	// A positive process limitation narrows QueryCap.  Zero means no narrower
	// override (the resolved CN cap remains the query cap).
	ProcessLimitationSize uint64
}

// HashBuildCeiling is the resolved local-CN and per-generation budget.
type HashBuildCeiling struct {
	EffectiveCN      uint64
	RequestedReserve uint64
	Reserve          uint64
	CNHashCap        uint64
	QueryCap         uint64
}

// ResolveHashBuildCeiling computes the budget ceiling without touching the OS.
// At least one finite, positive source is required, and every resulting cap
// must remain positive (fail closed otherwise).
func ResolveHashBuildCeiling(in HashBuildCeilingInputs) (HashBuildCeiling, error) {
	effective := uint64(0)
	for _, candidate := range []uint64{in.CgroupMemoryMax, in.HostMemTotal, in.GlobalMpoolCap} {
		if candidate == 0 || candidate == math.MaxUint64 {
			continue
		}
		if effective == 0 || candidate < effective {
			effective = candidate
		}
	}
	if effective == 0 {
		return HashBuildCeiling{}, &HashBuildBudgetError{Kind: HashBuildBudgetErrorCeilingMissing, Message: ErrHashBuildCeilingMissing.Error()}
	}

	requested := hashBuildMinimumReserve
	if fifth := effective / 5; fifth > requested {
		requested = fifth
	}
	if in.FileCacheHint > requested {
		requested = in.FileCacheHint
	}
	reserve := requested
	// Small, tightly limited CNs still need a bounded HashBuild allowance for
	// bootstrap/internal SQL. Keep at least 5% (and normally 64 MiB) available
	// instead of turning every HashBuild into a startup-fatal ceiling error.
	minimumHashCap := effective / 20
	if minimumHashCap < 64*mpool.MB {
		minimumHashCap = 64 * mpool.MB
	}
	if minimumHashCap >= effective {
		minimumHashCap = effective / 5
	}
	maxReserve := effective - minimumHashCap
	if reserve > maxReserve {
		reserve = maxReserve
	}
	cnCap := effective - reserve
	if cnCap == 0 {
		return HashBuildCeiling{}, &HashBuildBudgetError{Kind: HashBuildBudgetErrorCeilingMissing, Message: "hash build budget ceiling is zero"}
	}
	queryCap := cnCap
	if in.ProcessLimitationSize > 0 && in.ProcessLimitationSize < queryCap {
		queryCap = in.ProcessLimitationSize
	}
	if queryCap == 0 {
		return HashBuildCeiling{}, &HashBuildBudgetError{Kind: HashBuildBudgetErrorCeilingMissing, Message: "hash build query cap is zero"}
	}
	return HashBuildCeiling{
		EffectiveCN:      effective,
		RequestedReserve: requested,
		Reserve:          reserve,
		CNHashCap:        cnCap,
		QueryCap:         queryCap,
	}, nil
}

// ResolveHashBuildBudget is a semantic alias used by budget initialization
// callers.
func ResolveHashBuildBudget(in HashBuildCeilingInputs) (HashBuildCeiling, error) {
	return ResolveHashBuildCeiling(in)
}

// NewHashBuildBudgetFromCeiling wires a resolved ceiling into the local-CN
// aggregate/generation budget.
func NewHashBuildBudgetFromCeiling(ceiling HashBuildCeiling) (*HashBuildBudget, error) {
	return NewHashBuildBudget(ceiling.CNHashCap, ceiling.QueryCap)
}

// GetHashBuildBudget returns the statement generation shared by every child
// process in this BaseProcess. Different top-level processes on the same CN
// charge a shared aggregate budget.
func (proc *Process) GetHashBuildBudget() (*HashBuildBudgetGeneration, error) {
	if proc == nil || proc.Base == nil {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Message: "nil process for hash build budget"}
	}
	proc.Base.hashBuildBudgetMu.Lock()
	defer proc.Base.hashBuildBudgetMu.Unlock()
	if proc.Base.hashBuildBudget != nil {
		return proc.Base.hashBuildBudget, nil
	}

	globalCap := uint64(0)
	if cap := mpool.GlobalCap(); cap > 0 && cap < mpool.PB {
		globalCap = uint64(cap)
	}
	queryLimit := uint64(0)
	if proc.Base.Lim.Size > 0 {
		queryLimit = uint64(proc.Base.Lim.Size)
	}
	fileCacheHint := uint64(0)
	if hint := fileservice.GlobalMemoryCacheSizeHint.Load(); hint > 0 {
		fileCacheHint = uint64(hint)
	}
	ceiling, err := ResolveHashBuildCeiling(HashBuildCeilingInputs{
		CgroupMemoryMax:       system.CgroupMemoryLimit(),
		HostMemTotal:          system.MemoryTotal(),
		GlobalMpoolCap:        globalCap,
		FileCacheHint:         fileCacheHint,
		ProcessLimitationSize: queryLimit,
	})
	if err != nil {
		return nil, err
	}

	liveCNCap := func() (uint64, error) {
		globalCap := uint64(0)
		if cap := mpool.GlobalCap(); cap > 0 && cap < mpool.PB {
			globalCap = uint64(cap)
		}
		fileCacheHint := uint64(0)
		if hint := fileservice.GlobalMemoryCacheSizeHint.Load(); hint > 0 {
			fileCacheHint = uint64(hint)
		}
		current, resolveErr := ResolveHashBuildCeiling(HashBuildCeilingInputs{
			CgroupMemoryMax: system.CgroupMemoryLimit(),
			HostMemTotal:    system.MemoryTotal(),
			GlobalMpoolCap:  globalCap,
			FileCacheHint:   fileCacheHint,
		})
		if resolveErr != nil {
			return 0, resolveErr
		}
		return current.CNHashCap, nil
	}

	var aggregate *HashBuildBudget
	service := proc.GetService()
	if service == "" {
		service = "__process_local_cn__"
	}
	value, _ := hashBuildCNBudgets.LoadOrStore(service, func() *HashBuildBudget {
		b, createErr := NewHashBuildBudget(ceiling.CNHashCap, ceiling.CNHashCap)
		if createErr != nil {
			return nil
		}
		return b
	}())
	aggregate, _ = value.(*HashBuildBudget)
	if aggregate == nil {
		err = &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Message: "failed to initialize CN hash build budget"}
	}
	if err != nil {
		return nil, err
	}
	if err = aggregate.UpdateAggregateCap(ceiling.CNHashCap); err != nil {
		return nil, err
	}
	aggregate.SetAggregateCapProvider(liveCNCap)
	queryCap := ceiling.QueryCap
	if aggregate.AggregateCap() < queryCap {
		queryCap = aggregate.AggregateCap()
	}
	generation, err := aggregate.OpenGenerationWithCap(hashBuildGenerationSequence.Add(1), queryCap)
	if err != nil {
		return nil, err
	}
	proc.Base.hashBuildBudget = generation
	return generation, nil
}
