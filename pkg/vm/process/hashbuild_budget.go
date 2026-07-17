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
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const hashBuildMinimumReserve = uint64(4 << 30)

var (
	hashBuildGenerationSequence atomic.Uint64
	hashBuildCNBudgets          sync.Map // service ID -> *HashBuildBudget
)

func observeHashBuildBudget(component, event, scope string, bytes uint64) {
	metricv2.HashBuildBudgetEventCounter.WithLabelValues(component, event, scope).Inc()
	metricv2.HashBuildBudgetBytesCounter.WithLabelValues(component, event, scope).Add(float64(bytes))
}

// Errors returned by hash-build admission.  These errors intentionally live in
// process rather than the SQL layer so that operators and remote execution
// code can make an admission decision without importing frontend packages.
var (
	ErrHashBuildBudgetAdmission = errors.New("hash build budget admission rejected")
	// ErrHashBuildBudgetRejected is kept as a more discoverable spelling of the
	// admission sentinel.  It is the same value, so errors.Is works with either.
	ErrHashBuildBudgetRejected             = ErrHashBuildBudgetAdmission
	ErrHashBuildBudgetClosed               = errors.New("hash build budget is closed")
	ErrHashBuildBudgetInvalid              = errors.New("invalid hash build budget")
	ErrHashBuildCeilingMissing             = errors.New("hash build budget ceiling unavailable")
	ErrHashBuildBudgetUnavailable          = ErrHashBuildCeilingMissing
	ErrHashBuildReservationInactive        = errors.New("hash build reservation is inactive")
	ErrHashBuildReservationUpward          = errors.New("hash build reservation reconciliation would increase charge")
	ErrHashBuildReservationReconcileUpward = ErrHashBuildReservationUpward
	ErrHashBuildReservationClosed          = ErrHashBuildReservationInactive
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
	spillDiskCap  uint64
	spillDiskUsed uint64
	spillFDCap    uint64
	spillFDUsed   uint64
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
	return &HashBuildBudget{
		aggregateCap: aggregateCap, queryCap: queryCap,
		spillDiskCap: defaultSpillCap(aggregateCap), spillFDCap: defaultSpillFDCap(aggregateCap),
	}, nil
}

func defaultSpillCap(memoryCap uint64) uint64 {
	const maxSpill = uint64(1 << 40)
	if memoryCap > maxSpill/8 {
		return maxSpill
	}
	return memoryCap * 8
}

func defaultSpillFDCap(memoryCap uint64) uint64 {
	// A finite cap derived from memory keeps FD admission bounded while
	// retaining enough fanout for normal (32-way) spill partitions.
	// A 16-way shuffle query can have 16 concurrent build partitions, each
	// owning 32 first-pass files and up to 64 transactional build/probe child
	// files during re-spill. Keep that normal peak admissible while retaining a
	// finite query/CN ledger that rejects runaway fanout.
	const minFD = uint64(2048)
	const bytesPerFD = uint64(4 << 20)
	cap := memoryCap / bytesPerFD
	if cap < minFD {
		cap = minFD
	}
	return cap
}

// HashBuildBudgetSnapshot is an immutable observational view of CN charges.
type HashBuildBudgetSnapshot struct {
	AggregateCap, AggregateUsed uint64
	SpillDiskCap, SpillDiskUsed uint64
	SpillFDCap, SpillFDUsed     uint64
	Closed                      bool
}

func (b *HashBuildBudget) Snapshot() HashBuildBudgetSnapshot {
	if b == nil {
		return HashBuildBudgetSnapshot{Closed: true}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return HashBuildBudgetSnapshot{b.aggregateCap, b.aggregateUsed, b.spillDiskCap, b.spillDiskUsed, b.spillFDCap, b.spillFDUsed, b.closed}
}

func (b *HashBuildBudget) SpillDiskCap() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spillDiskCap
}
func (b *HashBuildBudget) SpillDiskUsed() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spillDiskUsed
}
func (b *HashBuildBudget) SpillFDCap() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spillFDCap
}
func (b *HashBuildBudget) SpillFDUsed() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spillFDUsed
}

// SetSpillCaps configures finite CN spill caps. Zero values restore defaults.
func (b *HashBuildBudget) SetSpillCaps(diskBytes, fds uint64) error {
	if b == nil {
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if diskBytes == 0 {
		diskBytes = defaultSpillCap(b.aggregateCap)
	}
	if fds == 0 {
		fds = defaultSpillFDCap(b.aggregateCap)
	}
	if b.spillDiskUsed > diskBytes || b.spillFDUsed > fds {
		return newAdmissionError(0, b.spillDiskUsed, diskBytes)
	}
	b.spillDiskCap, b.spillFDCap = diskBytes, fds
	return nil
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

func NewHashBuildBudgetWithSpillCaps(aggregateCap, queryCap, spillDiskCap, spillFDCap uint64) (*HashBuildBudget, error) {
	b, err := NewHashBuildBudget(aggregateCap, queryCap)
	if err != nil {
		return nil, err
	}
	if err = b.SetSpillCaps(spillDiskCap, spillFDCap); err != nil {
		return nil, err
	}
	return b, nil
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
	budget                                                  *HashBuildBudget
	id                                                      uint64
	cap                                                     uint64
	used                                                    uint64
	closed                                                  bool
	spillDiskCap, spillDiskUsed                             uint64
	spillFDCap, spillFDUsed                                 uint64
	reserveCount, rejectCount, reconcileCount, releaseCount uint64
	peakUsed                                                uint64
}

// HashBuildBudgetGenerationSnapshot is an immutable fixed-cardinality view.
type HashBuildBudgetGenerationSnapshot struct {
	ID, Cap, Used, PeakUsed                                 uint64
	ReserveCount, RejectCount, ReconcileCount, ReleaseCount uint64
	SpillDiskCap, SpillDiskUsed, SpillFDCap                 uint64
	SpillFDUsed                                             uint64
	Closed                                                  bool
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
	return &HashBuildBudgetGeneration{budget: b, id: id, cap: b.queryCap,
		spillDiskCap: defaultSpillCap(b.queryCap), spillFDCap: defaultSpillFDCap(b.queryCap)}, nil
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
	return &HashBuildBudgetGeneration{budget: b, id: id, cap: cap,
		spillDiskCap: defaultSpillCap(cap), spillFDCap: defaultSpillFDCap(cap)}, nil
}

// OpenGenerationWithSpillCaps opens a generation with explicit memory, disk,
// and file-descriptor ceilings. Zero spill values use the documented defaults.
func (b *HashBuildBudget) OpenGenerationWithSpillCaps(id, memoryCap, spillDiskCap, spillFDCap uint64) (*HashBuildBudgetGeneration, error) {
	g, err := b.OpenGenerationWithCap(id, memoryCap)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if spillDiskCap == 0 {
		spillDiskCap = defaultSpillCap(memoryCap)
	}
	if spillFDCap == 0 {
		spillFDCap = defaultSpillFDCap(memoryCap)
	}
	if spillDiskCap > b.spillDiskCap {
		spillDiskCap = b.spillDiskCap
	}
	if spillFDCap > b.spillFDCap {
		spillFDCap = b.spillFDCap
	}
	g.spillDiskCap, g.spillFDCap = spillDiskCap, spillFDCap
	b.mu.Unlock()
	return g, nil
}

// OpenGenerationWithLimits is a compatibility spelling for explicit spill caps.
func (b *HashBuildBudget) OpenGenerationWithLimits(id, memoryCap, spillDiskCap, spillFDCap uint64) (*HashBuildBudgetGeneration, error) {
	return b.OpenGenerationWithSpillCaps(id, memoryCap, spillDiskCap, spillFDCap)
}

func (b *HashBuildBudget) OpenGenerationWithCapAndSpill(id, memoryCap, spillDiskCap, spillFDCap uint64) (*HashBuildBudgetGeneration, error) {
	return b.OpenGenerationWithSpillCaps(id, memoryCap, spillDiskCap, spillFDCap)
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

func (g *HashBuildBudgetGeneration) SpillDiskCap() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.spillDiskCap
}
func (g *HashBuildBudgetGeneration) SpillDiskUsed() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.spillDiskUsed
}
func (g *HashBuildBudgetGeneration) SpillFDCap() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.spillFDCap
}
func (g *HashBuildBudgetGeneration) SpillFDUsed() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.spillFDUsed
}

func (g *HashBuildBudgetGeneration) Snapshot() HashBuildBudgetGenerationSnapshot {
	if g == nil || g.budget == nil {
		return HashBuildBudgetGenerationSnapshot{Closed: true}
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return HashBuildBudgetGenerationSnapshot{
		ID: g.id, Cap: g.cap, Used: g.used, PeakUsed: g.peakUsed,
		ReserveCount: g.reserveCount, RejectCount: g.rejectCount, ReconcileCount: g.reconcileCount, ReleaseCount: g.releaseCount,
		SpillDiskCap: g.spillDiskCap, SpillDiskUsed: g.spillDiskUsed, SpillFDCap: g.spillFDCap, SpillFDUsed: g.spillFDUsed,
		Closed: g.closed || g.budget.closed,
	}
}

// Stats is an alias retained for observability call sites.
func (g *HashBuildBudgetGeneration) Stats() HashBuildBudgetGenerationSnapshot { return g.Snapshot() }
func (g *HashBuildBudgetGeneration) Peak() uint64                             { return g.Snapshot().PeakUsed }
func (g *HashBuildBudgetGeneration) ReserveCount() uint64                     { return g.Snapshot().ReserveCount }
func (g *HashBuildBudgetGeneration) RejectCount() uint64                      { return g.Snapshot().RejectCount }
func (g *HashBuildBudgetGeneration) ReconcileCount() uint64                   { return g.Snapshot().ReconcileCount }
func (g *HashBuildBudgetGeneration) ReleaseCount() uint64                     { return g.Snapshot().ReleaseCount }

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
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "query", size)
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: size, Used: g.used, Cap: g.cap}
	}

	// Check by subtraction rather than used+size: this is safe for
	// math.MaxUint64 and rejects every overflow-sized request.
	if b.aggregateUsed > b.aggregateCap || size > b.aggregateCap-b.aggregateUsed {
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "cn", size)
		return nil, newAdmissionError(size, b.aggregateUsed, b.aggregateCap)
	}
	b.aggregateUsed += size

	if g.used > g.cap || size > g.cap-g.used {
		// Roll back the complete CN charge before returning the rejection.
		b.aggregateUsed -= size
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "query", size)
		return nil, newAdmissionError(size, g.used, g.cap)
	}
	g.used += size
	observeHashBuildBudget("memory", "reserve", "query", size)
	observeHashBuildBudget("memory", "reserve", "cn", size)
	g.reserveCount++
	if g.used > g.peakUsed {
		g.peakUsed = g.used
	}
	return &HashBuildReservation{budget: b, generation: g, core: &hashBuildReservationCore{size: size}}, nil
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

// Grow increases a live memory reservation atomically. It is used for the
// Shuffle emergency spill-scratch lease so retained copies cannot consume the
// memory required to recover from a later admission rejection.
func (r *HashBuildReservation) Grow(additional uint64) error {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return ErrHashBuildReservationInactive
	}
	if additional == 0 {
		return nil
	}
	b := r.budget
	g := r.generation
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	provider := b.capProvider
	b.mu.Unlock()
	if provider != nil {
		cap, err := provider()
		if err != nil {
			return err
		}
		if cap == 0 {
			return &HashBuildBudgetError{Kind: HashBuildBudgetErrorCeilingMissing, Message: "live hash build budget ceiling is zero"}
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
	if r.core.state.Load() != hashBuildReservationActive {
		return ErrHashBuildReservationInactive
	}
	if b.closed || g.closed {
		g.rejectCount++
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: additional, Used: g.used, Cap: g.cap}
	}
	if b.aggregateUsed > b.aggregateCap || additional > b.aggregateCap-b.aggregateUsed {
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "cn", additional)
		return newAdmissionError(additional, b.aggregateUsed, b.aggregateCap)
	}
	if g.used > g.cap || additional > g.cap-g.used {
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "query", additional)
		return newAdmissionError(additional, g.used, g.cap)
	}
	if r.core.size > math.MaxUint64-additional {
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Requested: additional, Message: "hash build reservation size overflow"}
	}
	b.aggregateUsed += additional
	g.used += additional
	r.core.size += additional
	if g.used > g.peakUsed {
		g.peakUsed = g.used
	}
	g.reserveCount++
	observeHashBuildBudget("memory", "reserve", "query", additional)
	observeHashBuildBudget("memory", "reserve", "cn", additional)
	return nil
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
	// core is shared by accidental token copies, keeping mutable charge and
	// exactly-once state together under the budget mutex.
	core *hashBuildReservationCore
}

type hashBuildReservationCore struct {
	size  uint64
	state atomic.Uint32
}

const (
	hashBuildReservationActive uint32 = iota
	hashBuildReservationReleased
	hashBuildReservationTransferred
)

// Size returns the reservation's current reconciled charge.
func (r *HashBuildReservation) Size() uint64 {
	if r == nil || r.budget == nil || r.core == nil {
		return 0
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	return r.core.size
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
	if r == nil || r.core == nil {
		return true
	}
	if r.budget == nil {
		return r.core.state.Load() != hashBuildReservationActive
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	return r.core.state.Load() != hashBuildReservationActive
}

// Release relinquishes this token once.  It returns true only for the caller
// that won the active -> released transition.
func (r *HashBuildReservation) Release() bool {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return false
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if !r.core.state.CompareAndSwap(hashBuildReservationActive, hashBuildReservationReleased) {
		return false
	}
	// The subtraction is exact for a live token.  Keep a defensive branch so
	// corrupted state cannot underflow and turn into an apparent huge charge.
	if r.generation.used >= r.core.size {
		r.generation.used -= r.core.size
	} else {
		r.generation.used = 0
	}
	if r.budget.aggregateUsed >= r.core.size {
		r.budget.aggregateUsed -= r.core.size
	} else {
		r.budget.aggregateUsed = 0
	}
	r.generation.releaseCount++
	observeHashBuildBudget("memory", "release", "query", r.core.size)
	observeHashBuildBudget("memory", "release", "cn", r.core.size)
	return true
}

// ReconcileDown shrinks a live charge to actual bytes. It is linearized with
// reserve/release/transfer under the owning budget mutex. Upward reconciliation
// is rejected and inactive tokens never mutate counters.
func (r *HashBuildReservation) ReconcileDown(actual uint64) (bool, error) {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return false, ErrHashBuildReservationInactive
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if r.core.state.Load() != hashBuildReservationActive {
		return false, ErrHashBuildReservationInactive
	}
	if actual > r.core.size {
		return false, ErrHashBuildReservationUpward
	}
	delta := r.core.size - actual
	if delta > 0 {
		if r.generation.used < delta || r.budget.aggregateUsed < delta {
			return false, ErrHashBuildReservationInactive
		}
		r.generation.used -= delta
		r.budget.aggregateUsed -= delta
		r.core.size = actual
		observeHashBuildBudget("memory", "reconcile", "query", delta)
		observeHashBuildBudget("memory", "reconcile", "cn", delta)
	}
	r.generation.reconcileCount++
	return true, nil
}

// Reconcile is a compatibility alias.
func (r *HashBuildReservation) Reconcile(actual uint64) (bool, error) { return r.ReconcileDown(actual) }

// Transfer moves ownership to a fresh token exactly once.  The original token
// becomes inert; releasing it after a successful transfer cannot decrement the
// budget.  If Release wins the race, Transfer returns nil.
func (r *HashBuildReservation) Transfer() *HashBuildReservation {
	if r == nil || r.core == nil || r.budget == nil {
		return nil
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if !r.core.state.CompareAndSwap(hashBuildReservationActive, hashBuildReservationTransferred) {
		return nil
	}
	return &HashBuildReservation{budget: r.budget, generation: r.generation, core: &hashBuildReservationCore{size: r.core.size}}
}

// TransferOwnership is a descriptive alias for Transfer.
func (r *HashBuildReservation) TransferOwnership() *HashBuildReservation { return r.Transfer() }

// TransferTo is another descriptive spelling for ownership transfer.
func (r *HashBuildReservation) TransferTo() *HashBuildReservation { return r.Transfer() }

// HashBuildSpillDiskReservation owns query and CN spill-disk bytes.
type HashBuildSpillDiskReservation struct {
	budget     *HashBuildBudget
	generation *HashBuildBudgetGeneration
	core       *hashBuildReservationCore
}

// HashBuildSpillFDReservation owns query and CN spill file descriptors.
type HashBuildSpillFDReservation struct {
	budget     *HashBuildBudget
	generation *HashBuildBudgetGeneration
	core       *hashBuildReservationCore
}

type SpillDiskReservation = HashBuildSpillDiskReservation
type SpillFDReservation = HashBuildSpillFDReservation

func (r *HashBuildSpillDiskReservation) Size() uint64 {
	if r == nil || r.budget == nil || r.core == nil {
		return 0
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	return r.core.size
}
func (r *HashBuildSpillFDReservation) Size() uint64 {
	if r == nil || r.budget == nil || r.core == nil {
		return 0
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	return r.core.size
}
func (r *HashBuildSpillDiskReservation) Released() bool {
	return r == nil || r.core == nil || r.core.state.Load() != hashBuildReservationActive
}
func (r *HashBuildSpillFDReservation) Released() bool {
	return r == nil || r.core == nil || r.core.state.Load() != hashBuildReservationActive
}

func (g *HashBuildBudgetGeneration) ReserveSpillDisk(size uint64) (*HashBuildSpillDiskReservation, error) {
	if g == nil || g.budget == nil {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid}
	}
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || g.closed {
		g.rejectCount++
		observeHashBuildBudget("spill_disk", "reject", "query", size)
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: size}
	}
	if b.spillDiskUsed > b.spillDiskCap || size > b.spillDiskCap-b.spillDiskUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_disk", "reject", "cn", size)
		return nil, newAdmissionError(size, b.spillDiskUsed, b.spillDiskCap)
	}
	if g.spillDiskUsed > g.spillDiskCap || size > g.spillDiskCap-g.spillDiskUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_disk", "reject", "query", size)
		return nil, newAdmissionError(size, g.spillDiskUsed, g.spillDiskCap)
	}
	b.spillDiskUsed += size
	g.spillDiskUsed += size
	observeHashBuildBudget("spill_disk", "reserve", "query", size)
	observeHashBuildBudget("spill_disk", "reserve", "cn", size)
	return &HashBuildSpillDiskReservation{budget: b, generation: g, core: &hashBuildReservationCore{size: size}}, nil
}

func (g *HashBuildBudgetGeneration) ReserveSpillDiskBytes(size uint64) (*HashBuildSpillDiskReservation, error) {
	return g.ReserveSpillDisk(size)
}

// Grow increases one live per-file disk reservation without allocating a new
// bookkeeping token. This keeps metadata proportional to open spill files,
// rather than to the number of tiny batch records written to those files.
func (r *HashBuildSpillDiskReservation) Grow(additional uint64) error {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return ErrHashBuildReservationInactive
	}
	if additional == 0 {
		return nil
	}
	b := r.budget
	g := r.generation
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.core.state.Load() != hashBuildReservationActive {
		return ErrHashBuildReservationInactive
	}
	if b.closed || g.closed {
		g.rejectCount++
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: additional}
	}
	if b.spillDiskUsed > b.spillDiskCap || additional > b.spillDiskCap-b.spillDiskUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_disk", "reject", "cn", additional)
		return newAdmissionError(additional, b.spillDiskUsed, b.spillDiskCap)
	}
	if g.spillDiskUsed > g.spillDiskCap || additional > g.spillDiskCap-g.spillDiskUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_disk", "reject", "query", additional)
		return newAdmissionError(additional, g.spillDiskUsed, g.spillDiskCap)
	}
	if r.core.size > math.MaxUint64-additional {
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Requested: additional, Message: "spill disk reservation size overflow"}
	}
	b.spillDiskUsed += additional
	g.spillDiskUsed += additional
	r.core.size += additional
	observeHashBuildBudget("spill_disk", "reserve", "query", additional)
	observeHashBuildBudget("spill_disk", "reserve", "cn", additional)
	g.reserveCount++
	return nil
}

func (g *HashBuildBudgetGeneration) ReserveSpillFD(size uint64) (*HashBuildSpillFDReservation, error) {
	if g == nil || g.budget == nil {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid}
	}
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || g.closed {
		g.rejectCount++
		observeHashBuildBudget("spill_fd", "reject", "query", size)
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: size}
	}
	if b.spillFDUsed > b.spillFDCap || size > b.spillFDCap-b.spillFDUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_fd", "reject", "cn", size)
		return nil, newAdmissionError(size, b.spillFDUsed, b.spillFDCap)
	}
	if g.spillFDUsed > g.spillFDCap || size > g.spillFDCap-g.spillFDUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_fd", "reject", "query", size)
		return nil, newAdmissionError(size, g.spillFDUsed, g.spillFDCap)
	}
	b.spillFDUsed += size
	g.spillFDUsed += size
	observeHashBuildBudget("spill_fd", "reserve", "query", size)
	observeHashBuildBudget("spill_fd", "reserve", "cn", size)
	return &HashBuildSpillFDReservation{budget: b, generation: g, core: &hashBuildReservationCore{size: size}}, nil
}

func (g *HashBuildBudgetGeneration) ReserveSpillFileDescriptors(size uint64) (*HashBuildSpillFDReservation, error) {
	return g.ReserveSpillFD(size)
}

func (r *HashBuildSpillDiskReservation) Release() bool {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return false
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if !r.core.state.CompareAndSwap(hashBuildReservationActive, hashBuildReservationReleased) {
		return false
	}
	if r.generation.spillDiskUsed >= r.core.size {
		r.generation.spillDiskUsed -= r.core.size
	} else {
		r.generation.spillDiskUsed = 0
	}
	if r.budget.spillDiskUsed >= r.core.size {
		r.budget.spillDiskUsed -= r.core.size
	} else {
		r.budget.spillDiskUsed = 0
	}
	observeHashBuildBudget("spill_disk", "release", "query", r.core.size)
	observeHashBuildBudget("spill_disk", "release", "cn", r.core.size)
	r.generation.releaseCount++
	return true
}

func (r *HashBuildSpillFDReservation) Release() bool {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return false
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if !r.core.state.CompareAndSwap(hashBuildReservationActive, hashBuildReservationReleased) {
		return false
	}
	if r.generation.spillFDUsed >= r.core.size {
		r.generation.spillFDUsed -= r.core.size
	} else {
		r.generation.spillFDUsed = 0
	}
	if r.budget.spillFDUsed >= r.core.size {
		r.budget.spillFDUsed -= r.core.size
	} else {
		r.budget.spillFDUsed = 0
	}
	observeHashBuildBudget("spill_fd", "release", "query", r.core.size)
	observeHashBuildBudget("spill_fd", "release", "cn", r.core.size)
	r.generation.releaseCount++
	return true
}

func (r *HashBuildSpillDiskReservation) ReconcileDown(actual uint64) (bool, error) {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return false, ErrHashBuildReservationInactive
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if r.core.state.Load() != hashBuildReservationActive {
		return false, ErrHashBuildReservationInactive
	}
	if actual > r.core.size {
		return false, ErrHashBuildReservationUpward
	}
	delta := r.core.size - actual
	if delta > 0 {
		if r.generation.spillDiskUsed < delta || r.budget.spillDiskUsed < delta {
			return false, ErrHashBuildReservationInactive
		}
		r.generation.spillDiskUsed -= delta
		r.budget.spillDiskUsed -= delta
		r.core.size = actual
		observeHashBuildBudget("spill_disk", "reconcile", "query", delta)
		observeHashBuildBudget("spill_disk", "reconcile", "cn", delta)
	}
	r.generation.reconcileCount++
	return true, nil
}
func (r *HashBuildSpillFDReservation) ReconcileDown(actual uint64) (bool, error) {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return false, ErrHashBuildReservationInactive
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if r.core.state.Load() != hashBuildReservationActive {
		return false, ErrHashBuildReservationInactive
	}
	if actual > r.core.size {
		return false, ErrHashBuildReservationUpward
	}
	delta := r.core.size - actual
	if delta > 0 {
		if r.generation.spillFDUsed < delta || r.budget.spillFDUsed < delta {
			return false, ErrHashBuildReservationInactive
		}
		r.generation.spillFDUsed -= delta
		r.budget.spillFDUsed -= delta
		r.core.size = actual
		observeHashBuildBudget("spill_fd", "reconcile", "query", delta)
		observeHashBuildBudget("spill_fd", "reconcile", "cn", delta)
	}
	r.generation.reconcileCount++
	return true, nil
}
func (r *HashBuildSpillDiskReservation) Reconcile(actual uint64) (bool, error) {
	return r.ReconcileDown(actual)
}
func (r *HashBuildSpillFDReservation) Reconcile(actual uint64) (bool, error) {
	return r.ReconcileDown(actual)
}

func (r *HashBuildSpillDiskReservation) Transfer() *HashBuildSpillDiskReservation {
	if r == nil || r.core == nil || r.budget == nil {
		return nil
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if !r.core.state.CompareAndSwap(hashBuildReservationActive, hashBuildReservationTransferred) {
		return nil
	}
	return &HashBuildSpillDiskReservation{budget: r.budget, generation: r.generation, core: &hashBuildReservationCore{size: r.core.size}}
}
func (r *HashBuildSpillFDReservation) Transfer() *HashBuildSpillFDReservation {
	if r == nil || r.core == nil || r.budget == nil {
		return nil
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if !r.core.state.CompareAndSwap(hashBuildReservationActive, hashBuildReservationTransferred) {
		return nil
	}
	return &HashBuildSpillFDReservation{budget: r.budget, generation: r.generation, core: &hashBuildReservationCore{size: r.core.size}}
}
func (r *HashBuildSpillDiskReservation) TransferOwnership() *HashBuildSpillDiskReservation {
	return r.Transfer()
}
func (r *HashBuildSpillFDReservation) TransferOwnership() *HashBuildSpillFDReservation {
	return r.Transfer()
}
func (r *HashBuildSpillDiskReservation) TransferTo() *HashBuildSpillDiskReservation {
	return r.Transfer()
}
func (r *HashBuildSpillFDReservation) TransferTo() *HashBuildSpillFDReservation { return r.Transfer() }

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
	spillDiskCap := uint64(0)
	if proc.Base.Lim.SpillSize > 0 {
		spillDiskCap = uint64(proc.Base.Lim.SpillSize)
	}
	generation, err := aggregate.OpenGenerationWithSpillCaps(hashBuildGenerationSequence.Add(1), queryCap, spillDiskCap, 0)
	if err != nil {
		return nil, err
	}
	proc.Base.hashBuildBudget = generation
	return generation, nil
}
