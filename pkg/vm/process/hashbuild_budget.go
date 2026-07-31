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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const hashBuildMinimumReserve = uint64(4 << 30)

const (
	hashBuildAllocationGenerationSlots = uint32(131_072)
	// Account metadata lives outside the MPool payload cap. Bound its worst-case
	// Go-heap footprint to half of hashBuildMinimumReserve: 128 bytes covers one
	// sparse pointer/lease-map entry, and 16,777,216 live entries consume at most
	// 2 GiB by construction. Small aggregate caps use the tighter byte
	// conservation bound because every physical allocation owns at least one
	// byte. Slot exhaustion is real metadata capacity pressure, not an
	// estimator rejection.
	hashBuildAllocationMetadataBytesPerSlot = uint64(128)
	hashBuildAllocationMetadataHeadroom     = hashBuildMinimumReserve / 2
	hashBuildAllocationMetadataMaxSlots     = hashBuildAllocationMetadataHeadroom / hashBuildAllocationMetadataBytesPerSlot
)

const (
	// Keep a process-wide reserve for listeners, RPC connections, object
	// storage, logs, and other descriptors that are not represented by the
	// hash-build spill ledger. The proportional reserve keeps the spill
	// subsystem from consuming an otherwise healthy CN's whole RLIMIT, while
	// the absolute floor protects low-limit containers.
	hashBuildNonSpillFDHeadroom    = uint64(64)
	hashBuildNonSpillFDHeadroomDiv = uint64(4)
)

// hashBuildBudgetCapRefreshTTL bounds how long a sampled configuration ceiling
// may be reused. The inputs are limits/reservations, not current memory usage.
// Cgroup max and host total are process-start snapshots; changing either
// requires restarting the CN. Runtime changes to the mpool cap or file-cache
// hint become effective within this window. A rejection below a cached cap
// bypasses the window so legitimate cap growth does not cause avoidable
// spilling.
const hashBuildBudgetCapRefreshTTL = 100 * time.Millisecond

var (
	hashBuildGenerationSequence atomic.Uint64
	hashBuildCNBudgets          sync.Map // service ID -> *HashBuildBudget
	hashBuildBudgetObservers    = newHashBuildBudgetObservers()
	// HashBuild treats physical process memory as a startup contract. Keeping
	// this snapshot local to HashBuild avoids cgroup filesystem reads on query
	// admission without changing the live CgroupMemoryLimit API used by other
	// subsystems such as remote compile.
	hashBuildProcessMemoryInputs = HashBuildCeilingInputs{
		CgroupMemoryMax: system.CgroupMemoryLimit(),
		HostMemTotal:    system.MemoryTotal(),
	}
)

type hashBuildBudgetMetricKey struct {
	component string
	event     string
	scope     string
}

type hashBuildBudgetObserver func(bytes uint64)

func newHashBuildBudgetObservers() map[hashBuildBudgetMetricKey]hashBuildBudgetObserver {
	components := [...]string{"memory", "spill_disk", "spill_fd"}
	events := [...]string{"reserve", "release", "reconcile", "reject"}
	scopes := [...]string{"query", "cn"}
	observers := make(map[hashBuildBudgetMetricKey]hashBuildBudgetObserver, len(components)*len(events)*len(scopes))
	for _, component := range components {
		for _, event := range events {
			for _, scope := range scopes {
				key := hashBuildBudgetMetricKey{component: component, event: event, scope: scope}
				eventCounter := metricv2.HashBuildBudgetEventCounter.WithLabelValues(component, event, scope)
				bytesCounter := metricv2.HashBuildBudgetBytesCounter.WithLabelValues(component, event, scope)
				observers[key] = func(bytes uint64) {
					eventCounter.Inc()
					bytesCounter.Add(float64(bytes))
				}
			}
		}
	}
	return observers
}

func observeHashBuildBudget(component, event, scope string, bytes uint64) {
	if observer := hashBuildBudgetObservers[hashBuildBudgetMetricKey{
		component: component,
		event:     event,
		scope:     scope,
	}]; observer != nil {
		observer(bytes)
		return
	}
	// Preserve the helper's behavior for future labels that have not yet been
	// added to the fixed-cardinality fast path above.
	metricv2.HashBuildBudgetEventCounter.WithLabelValues(component, event, scope).Inc()
	metricv2.HashBuildBudgetBytesCounter.WithLabelValues(component, event, scope).Add(float64(bytes))
}

// Errors returned by hash-build admission.  These errors intentionally live in
// process rather than the SQL layer so that operators and remote execution
// code can make an admission decision without importing frontend packages.
var (
	ErrHashBuildBudgetAdmission = moerr.NewInternalErrorNoCtx("hash build budget admission rejected")
	// ErrHashBuildBudgetRejected is kept as a more discoverable spelling of the
	// admission sentinel.  It is the same value, so errors.Is works with either.
	ErrHashBuildBudgetRejected             = ErrHashBuildBudgetAdmission
	ErrHashBuildBudgetClosed               = moerr.NewInternalErrorNoCtx("hash build budget is closed")
	ErrHashBuildBudgetInvalid              = moerr.NewInternalErrorNoCtx("invalid hash build budget")
	ErrHashBuildCeilingMissing             = moerr.NewInternalErrorNoCtx("hash build budget ceiling unavailable")
	ErrHashBuildBudgetUnavailable          = ErrHashBuildCeilingMissing
	ErrHashBuildReservationInactive        = moerr.NewInternalErrorNoCtx("hash build reservation is inactive")
	ErrHashBuildReservationUpward          = moerr.NewInternalErrorNoCtx("hash build reservation reconciliation would increase charge")
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

// HashBuildBudgetComponent identifies the independently bounded resource that
// rejected an admission. The zero value remains the memory component for
// compatibility with older callers that construct HashBuildBudgetError
// directly. A spill-disk or spill-FD rejection must never enter the memory
// reclaim/reduce loop: reducing an in-memory batch cannot create either
// resource and may replay already-published spill records.
type HashBuildBudgetComponent uint8

const (
	HashBuildBudgetComponentMemory HashBuildBudgetComponent = iota
	HashBuildBudgetComponentSpillDisk
	HashBuildBudgetComponentSpillFD
)

// HashBuildBudgetError carries bounded, observational details for an
// admission failure. Requested, Used and Cap use the unit named by Resource
// (bytes for memory/spill disk, descriptors for spill FD) and are always safe
// to inspect; they are never produced by overflowing arithmetic.
type HashBuildBudgetError struct {
	Kind      HashBuildBudgetErrorKind
	Component HashBuildBudgetComponent
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
	case HashBuildBudgetErrorAdmission:
		return fmt.Sprintf("%s: requested=%d used=%d cap=%d", ErrHashBuildBudgetAdmission, e.Requested, e.Used, e.Cap)
	case HashBuildBudgetErrorClosed:
		return ErrHashBuildBudgetClosed.Error()
	case HashBuildBudgetErrorInvalid:
		return ErrHashBuildBudgetInvalid.Error()
	case HashBuildBudgetErrorCeilingMissing:
		return ErrHashBuildCeilingMissing.Error()
	default:
		return fmt.Sprintf("%s: unknown kind=%d", ErrHashBuildBudgetInvalid, e.Kind)
	}
}

func (e *HashBuildBudgetError) Unwrap() error {
	if e == nil {
		return nil
	}
	switch e.Kind {
	case HashBuildBudgetErrorAdmission:
		return ErrHashBuildBudgetAdmission
	case HashBuildBudgetErrorClosed:
		return ErrHashBuildBudgetClosed
	case HashBuildBudgetErrorInvalid:
		return ErrHashBuildBudgetInvalid
	case HashBuildBudgetErrorCeilingMissing:
		return ErrHashBuildCeilingMissing
	default:
		return ErrHashBuildBudgetInvalid
	}
}

// Is keeps capacity admission, lifecycle, and accounting failures disjoint.
// Callers may recover a capacity rejection through spill, while Closed and
// other lifecycle failures must remain fatal.
func (e *HashBuildBudgetError) Is(target error) bool {
	if e == nil {
		return false
	}
	if target == ErrHashBuildBudgetAdmission || target == ErrHashBuildBudgetRejected {
		return e.Kind == HashBuildBudgetErrorAdmission
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
	// cap cache fields are protected by b.mu. refreshMu serializes only cache
	// misses/provider calls. refreshEpoch lets a failed admission avoid a second
	// provider call when another goroutine has already refreshed.
	capCached       bool
	capRefreshAt    time.Time
	capRefreshErr   error
	capRefreshEpoch uint64
	capRefreshTTL   time.Duration
	capNow          func() time.Time
	// liveCapInputs belongs to the production CN provider and is protected by
	// refreshMu. Direct budgets using SetAggregateCapProvider do not use it.
	liveCapInputs         HashBuildCeilingInputs
	liveCapInputsSnapshot atomic.Pointer[HashBuildCeilingInputs]
	closed                bool
	spillDiskCap          uint64
	spillDiskUsed         uint64
	// spillFDConfiguredCap is the finite logical ledger limit. spillFDCap is
	// its current effective value after applying the process RLIMIT_NOFILE
	// ceiling. Keeping both lets a budget recover if an administrator raises
	// the process limit, while every file reservation still preflights a
	// runtime decrease.
	spillFDConfiguredCap uint64
	spillFDCap           uint64
	spillFDUsed          uint64

	allocationRegistryOnce sync.Once
	allocationRegistry     *mpool.AllocationAccountRegistry
	allocationRegistryErr  error
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
	configuredFDCap := configuredSpillFDCap(aggregateCap)
	return &HashBuildBudget{
		aggregateCap: aggregateCap, queryCap: queryCap,
		spillDiskCap:         defaultSpillCap(aggregateCap),
		spillFDConfiguredCap: configuredFDCap,
		spillFDCap:           clampSpillFDCapToProcess(configuredFDCap),
		// Keep direct callers' historical provider semantics (sample before
		// every reservation). CN budgets obtained through GetHashBuildBudget
		// opt into the short shared cache below.
		capRefreshTTL: 0, capNow: time.Now,
	}, nil
}

func defaultSpillCap(memoryCap uint64) uint64 {
	const maxSpill = uint64(1 << 40)
	if memoryCap > maxSpill/8 {
		return maxSpill
	}
	return memoryCap * 8
}

func configuredSpillFDCap(memoryCap uint64) uint64 {
	// A finite cap derived from memory keeps FD admission bounded while
	// cushioning the first 32-way repartition peak. One engine can retain 32
	// build plus 32 probe files and open up to 64 child writers while it
	// re-partitions a bucket; 16 concurrent engines therefore reach roughly
	// 2048 descriptors. Deeper skew can retain a larger bucket queue and is
	// intentionally allowed to fail controlled FD admission: this floor is a
	// bounded operating allowance, not a completion guarantee.
	const minFD = uint64(2048)
	const bytesPerFD = uint64(4 << 20)
	cap := memoryCap / bytesPerFD
	if cap < minFD {
		cap = minFD
	}
	return cap
}

// clampSpillFDCap is the pure policy boundary between the finite spill ledger
// and the process-wide open-file ceiling. A failed/unsupported RLIMIT sample
// fails closed: resident joins remain usable, while spill-file admission is
// disabled. The headroom reduces spill's contribution to EMFILE risk, but it
// cannot account for the process's actual non-spill descriptors and therefore
// cannot guarantee that a later physical open will succeed.
func clampSpillFDCap(configured, processLimit uint64, limitKnown bool) uint64 {
	if configured == 0 || !limitKnown {
		return 0
	}
	if processLimit == math.MaxUint64 {
		return configured
	}
	headroom := processLimit / hashBuildNonSpillFDHeadroomDiv
	if headroom < hashBuildNonSpillFDHeadroom {
		headroom = hashBuildNonSpillFDHeadroom
	}
	if headroom >= processLimit {
		return 0
	}
	processSpillCap := processLimit - headroom
	if configured < processSpillCap {
		return configured
	}
	return processSpillCap
}

func clampSpillFDCapToProcess(configured uint64) uint64 {
	limit, ok := processOpenFileLimit()
	return clampSpillFDCap(configured, limit, ok)
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
	processLimit, limitKnown := processOpenFileLimit()
	b.mu.Lock()
	defer b.mu.Unlock()
	if diskBytes == 0 {
		diskBytes = defaultSpillCap(b.aggregateCap)
	}
	if fds == 0 {
		fds = configuredSpillFDCap(b.aggregateCap)
	}
	effectiveFDCap := clampSpillFDCap(fds, processLimit, limitKnown)
	if b.spillDiskUsed > diskBytes {
		return newComponentAdmissionError(HashBuildBudgetComponentSpillDisk, 0, b.spillDiskUsed, diskBytes)
	}
	if b.spillFDUsed > effectiveFDCap {
		return newComponentAdmissionError(HashBuildBudgetComponentSpillFD, 0, b.spillFDUsed, effectiveFDCap)
	}
	b.spillDiskCap = diskBytes
	b.spillFDConfiguredCap = fds
	b.spillFDCap = effectiveFDCap
	return nil
}

// raiseSpillDiskCapToExplicitLimit honors the operator-configured process
// spill limit at the shared CN ledger. Zero keeps the bounded default, and a
// smaller explicit limit remains generation-local. Growing is monotonic so an
// active reservation admitted under an earlier configuration is never made
// invalid by a concurrent process.
func (b *HashBuildBudget) raiseSpillDiskCapToExplicitLimit(diskBytes uint64) error {
	if b == nil {
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid}
	}
	if diskBytes == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return &HashBuildBudgetError{
			Kind:    HashBuildBudgetErrorClosed,
			Message: ErrHashBuildBudgetClosed.Error(),
		}
	}
	if diskBytes > b.spillDiskCap {
		b.spillDiskCap = diskBytes
	}
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

// refreshAggregateCap samples the live physical ceiling at most once per
// refresh TTL. refreshMu is a single-flight gate: callers arriving while a
// provider is running observe that result rather than issuing another OS
// sample. expectedEpoch is used by an admission retry: if another goroutine
// already refreshed after the failed attempt, the retry reuses that result.
// Provider errors are cached for the same short window and returned to every
// caller, keeping a failing source fail-closed without creating a syscall
// storm.
func (b *HashBuildBudget) refreshAggregateCap(force bool, expectedEpoch uint64) (epoch uint64, hasProvider bool, refreshed bool, err error) {
	if b == nil {
		return 0, false, false, &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid}
	}

	// The common TTL-hit path only needs b.mu. Taking refreshMu before checking
	// the cache serializes every reservation even though no provider call is
	// needed. A miss is checked again after entering refreshMu so concurrent
	// callers still share one provider result.
	_, epoch, hasProvider, cached, err := b.aggregateCapRefreshDecision(force, expectedEpoch)
	if cached {
		return epoch, hasProvider, false, err
	}

	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()

	provider, epoch, hasProvider, cached, err := b.aggregateCapRefreshDecision(force, expectedEpoch)
	if cached {
		return epoch, hasProvider, false, err
	}

	cap, providerErr := provider()
	if providerErr == nil && cap == 0 {
		providerErr = &HashBuildBudgetError{Kind: HashBuildBudgetErrorCeilingMissing, Message: "live hash build budget ceiling is zero"}
	}

	b.mu.Lock()
	// SetAggregateCapProvider and UpdateAggregateCap both take refreshMu, so
	// the provider cannot be replaced while this result is in flight. Keeping
	// the assignment under b.mu also makes cap and epoch one atomic snapshot to
	// admission callers.
	// Start the TTL when the sample completes. A slow filesystem read must not
	// make the freshly published result immediately stale and cause queued
	// callers to repeat the same read.
	now := b.capNow
	if now == nil {
		now = time.Now
	}
	b.capRefreshAt = now()
	b.capCached = true
	b.capRefreshErr = providerErr
	b.capRefreshEpoch++
	epoch = b.capRefreshEpoch
	if providerErr == nil {
		b.aggregateCap = cap
		if b.queryCap > cap {
			b.queryCap = cap
		}
	}
	b.mu.Unlock()
	return epoch, true, true, providerErr
}

// aggregateCapRefreshDecision returns an immutable provider snapshot and
// whether the caller can reuse the current cache. The caller must re-run this
// decision after acquiring refreshMu before invoking a provider.
func (b *HashBuildBudget) aggregateCapRefreshDecision(
	force bool,
	expectedEpoch uint64,
) (
	provider func() (uint64, error),
	epoch uint64,
	hasProvider bool,
	cached bool,
	err error,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.aggregateCapRefreshDecisionLocked(force, expectedEpoch)
}

// aggregateCapRefreshDecisionLocked is the lock-aware form used by admission
// paths that already hold b.mu. Keeping the cache decision and the ledger
// update in one critical section avoids repeated handoffs on every reservation.
func (b *HashBuildBudget) aggregateCapRefreshDecisionLocked(
	force bool,
	expectedEpoch uint64,
) (
	provider func() (uint64, error),
	epoch uint64,
	hasProvider bool,
	cached bool,
	err error,
) {
	provider = b.capProvider
	epoch = b.capRefreshEpoch
	if provider == nil {
		return nil, epoch, false, true, nil
	}
	now := b.capNow
	if now == nil {
		now = time.Now
	}
	ttl := b.capRefreshTTL
	// A zero TTL is useful for tests and explicit callers that want every
	// reservation to sample; the production default is a positive 100ms TTL.
	valid := b.capCached && ttl > 0 && now().Sub(b.capRefreshAt) < ttl
	if !force && valid {
		return provider, epoch, true, true, b.capRefreshErr
	}
	if force && expectedEpoch != 0 && expectedEpoch != epoch && valid {
		// A concurrent admission already performed the forced refresh. Reuse
		// its result, including a cached provider error (fail closed).
		return provider, epoch, true, true, b.capRefreshErr
	}
	return provider, epoch, true, false, nil
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
	// GetHashBuildBudget installs its provider first, then calls this method
	// with the ceiling it just resolved. That explicit value is a fresh sample
	// and can seed the shared cache instead of repeating provider work on the
	// first reservation of every new query.
	now := b.capNow
	if now == nil {
		now = time.Now
	}
	b.capRefreshAt = now()
	b.capCached = b.capProvider != nil
	b.capRefreshErr = nil
	b.capRefreshEpoch++
	b.mu.Unlock()
	return nil
}

// SetAggregateCapProvider installs the live physical-ceiling source. The
// provider must not call back into this budget. An already-open generation
// observes runtime mpool/file-cache ceiling changes on the first reservation,
// after the refresh TTL, or immediately when a cached aggregate cap rejects a
// request. Cgroup and host-memory inputs remain fixed until process restart.
func (b *HashBuildBudget) SetAggregateCapProvider(provider func() (uint64, error)) {
	if b == nil {
		return
	}
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	b.capProvider = provider
	// Installing or replacing a provider starts a fresh epoch. Callers that
	// already sampled a ceiling can seed the cache with UpdateAggregateCap
	// immediately afterward (as GetHashBuildBudget does).
	b.capCached = false
	b.capRefreshErr = nil
	b.capRefreshEpoch++
	b.mu.Unlock()
}

// installCNCapProvider publishes a new CN aggregate with its initial source
// snapshot already attached. The candidate is not placed in the service map
// until this method returns.
func (b *HashBuildBudget) installCNCapProvider(inputs HashBuildCeilingInputs) {
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.liveCapInputs = inputs
	b.publishLiveCNCapInputs()
	b.mu.Lock()
	b.capProvider = b.sampleCNCap
	b.capRefreshTTL = hashBuildBudgetCapRefreshTTL
	now := b.capNow
	if now == nil {
		now = time.Now
	}
	b.capRefreshAt = now()
	b.capCached = true
	b.capRefreshErr = nil
	b.capRefreshEpoch++
	b.mu.Unlock()
}

// mergeObservedCNCap records finite sources observed by another process before
// it uses an existing shared aggregate. External samples can only become more
// conservative here: ceilings take the lower value and the file-cache reserve
// takes the higher value. Legitimate growth is published later by sampleCNCap,
// which runs serially under refreshMu and therefore has a total order.
func (b *HashBuildBudget) mergeObservedCNCap(inputs HashBuildCeilingInputs, cap uint64) {
	if b.observedCNCapInputsCurrent(inputs, cap) {
		return
	}
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	if b.observedCNCapInputsCurrent(inputs, cap) {
		return
	}
	mergeLower := func(current *uint64, observed uint64) {
		if observed > 0 && observed < math.MaxUint64 && (*current == 0 || *current == math.MaxUint64 || observed < *current) {
			*current = observed
		}
	}
	mergeLower(&b.liveCapInputs.CgroupMemoryMax, inputs.CgroupMemoryMax)
	mergeLower(&b.liveCapInputs.HostMemTotal, inputs.HostMemTotal)
	mergeLower(&b.liveCapInputs.GlobalMpoolCap, inputs.GlobalMpoolCap)
	if inputs.FileCacheHint > b.liveCapInputs.FileCacheHint {
		b.liveCapInputs.FileCacheHint = inputs.FileCacheHint
	}
	b.mu.Lock()
	if cap > 0 && cap < b.aggregateCap {
		b.aggregateCap = cap
		if b.queryCap > cap {
			b.queryCap = cap
		}
	}
	b.mu.Unlock()
	b.publishLiveCNCapInputs()
}

func (b *HashBuildBudget) observedCNCapInputsCurrent(inputs HashBuildCeilingInputs, cap uint64) bool {
	current := b.liveCapInputsSnapshot.Load()
	if current == nil ||
		current.CgroupMemoryMax != inputs.CgroupMemoryMax ||
		current.HostMemTotal != inputs.HostMemTotal ||
		current.GlobalMpoolCap != inputs.GlobalMpoolCap ||
		current.FileCacheHint != inputs.FileCacheHint {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return cap == 0 || cap >= b.aggregateCap
}

// publishLiveCNCapInputs stores an immutable snapshot for the steady-state
// GetHashBuildBudget path. The caller holds refreshMu while mutating and
// publishing liveCapInputs.
func (b *HashBuildBudget) publishLiveCNCapInputs() {
	snapshot := b.liveCapInputs
	b.liveCapInputsSnapshot.Store(&snapshot)
}

// sampleCNCap is invoked only while refreshMu is held. Physical process memory
// remains fixed at the package-start snapshot; only runtime mpool and
// file-cache inputs are sampled here. A runtime source reports unavailable as
// zero, so resolveCNCapSample retains its last finite value rather than
// interpreting a transient disappearance as extra memory.
func (b *HashBuildBudget) sampleCNCap() (uint64, error) {
	current := hashBuildProcessMemoryInputs
	if cap := mpool.GlobalCap(); cap > 0 && cap < mpool.PB {
		current.GlobalMpoolCap = uint64(cap)
	}
	if hint := fileservice.GlobalMemoryCacheSizeHint.Load(); hint > 0 {
		current.FileCacheHint = uint64(hint)
	}
	return b.resolveCNCapSample(current)
}

// resolveCNCapSample applies fail-closed fallback to one serialized source
// sample. It is split from OS probing so source-turnover behavior is directly
// testable without depending on the host's cgroup layout.
func (b *HashBuildBudget) resolveCNCapSample(current HashBuildCeilingInputs) (uint64, error) {
	if current.CgroupMemoryMax == 0 {
		current.CgroupMemoryMax = b.liveCapInputs.CgroupMemoryMax
	}
	if current.HostMemTotal == 0 {
		current.HostMemTotal = b.liveCapInputs.HostMemTotal
	}
	if current.GlobalMpoolCap == 0 {
		current.GlobalMpoolCap = b.liveCapInputs.GlobalMpoolCap
	}
	if current.FileCacheHint == 0 {
		current.FileCacheHint = b.liveCapInputs.FileCacheHint
	}
	ceiling, err := ResolveHashBuildCeiling(current)
	if err != nil {
		return 0, err
	}
	b.liveCapInputs = current
	b.publishLiveCNCapInputs()
	return ceiling.CNHashCap, nil
}

// HashBuildBudgetGeneration is a statement execution generation on one CN.
// A generation's charge is independent from every other generation, even if a
// caller happens to reuse its numeric ID after the old generation has closed.
type HashBuildBudgetGeneration struct {
	budget                                                  *HashBuildBudget
	id                                                      uint64
	cap                                                     uint64
	used                                                    uint64
	allocationUsed                                          uint64
	closed                                                  bool
	spillDiskCap, spillDiskUsed                             uint64
	spillFDConfiguredCap, spillFDCap, spillFDUsed           uint64
	reserveCount, rejectCount, reconcileCount, releaseCount uint64
	peakUsed                                                uint64
}

var _ mpool.AllocationCapacityController = (*HashBuildBudgetGeneration)(nil)

// HashBuildBudgetGenerationSnapshot is an immutable fixed-cardinality view.
type HashBuildBudgetGenerationSnapshot struct {
	ID, Cap, Used, PeakUsed                                 uint64
	AllocationUsed                                          uint64
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
	configuredFDCap := configuredSpillFDCap(b.queryCap)
	if configuredFDCap > b.spillFDConfiguredCap {
		configuredFDCap = b.spillFDConfiguredCap
	}
	effectiveFDCap := configuredFDCap
	if effectiveFDCap > b.spillFDCap {
		effectiveFDCap = b.spillFDCap
	}
	return &HashBuildBudgetGeneration{budget: b, id: id, cap: b.queryCap,
		spillDiskCap:         defaultSpillCap(b.queryCap),
		spillFDConfiguredCap: configuredFDCap,
		spillFDCap:           effectiveFDCap}, nil
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
	configuredFDCap := configuredSpillFDCap(cap)
	if configuredFDCap > b.spillFDConfiguredCap {
		configuredFDCap = b.spillFDConfiguredCap
	}
	effectiveFDCap := configuredFDCap
	if effectiveFDCap > b.spillFDCap {
		effectiveFDCap = b.spillFDCap
	}
	return &HashBuildBudgetGeneration{budget: b, id: id, cap: cap,
		spillDiskCap:         defaultSpillCap(cap),
		spillFDConfiguredCap: configuredFDCap,
		spillFDCap:           effectiveFDCap}, nil
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
		spillFDCap = configuredSpillFDCap(memoryCap)
	}
	if spillDiskCap > b.spillDiskCap {
		spillDiskCap = b.spillDiskCap
	}
	if spillFDCap > b.spillFDConfiguredCap {
		spillFDCap = b.spillFDConfiguredCap
	}
	effectiveFDCap := spillFDCap
	if effectiveFDCap > b.spillFDCap {
		effectiveFDCap = b.spillFDCap
	}
	g.spillDiskCap = spillDiskCap
	g.spillFDConfiguredCap = spillFDCap
	g.spillFDCap = effectiveFDCap
	b.mu.Unlock()
	return g, nil
}

// openProcessGeneration opens the generation selected by GetHashBuildBudget.
// The resolved query cap can become stale before another statement finishes
// lowering the shared CN aggregate. Clamp and construct under one aggregate
// lock so an ordinary process cannot observe that race as an invalid budget.
func (b *HashBuildBudget) openProcessGeneration(
	id, requestedMemoryCap, spillDiskCap uint64,
) (*HashBuildBudgetGeneration, error) {
	if b == nil || requestedMemoryCap == 0 {
		return nil, &HashBuildBudgetError{
			Kind:      HashBuildBudgetErrorInvalid,
			Requested: requestedMemoryCap,
			Message:   "invalid process hash build generation cap",
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, &HashBuildBudgetError{
			Kind:    HashBuildBudgetErrorClosed,
			Message: ErrHashBuildBudgetClosed.Error(),
		}
	}

	memoryCap := requestedMemoryCap
	if memoryCap > b.aggregateCap {
		memoryCap = b.aggregateCap
	}
	if memoryCap == 0 {
		return nil, &HashBuildBudgetError{
			Kind:      HashBuildBudgetErrorInvalid,
			Requested: requestedMemoryCap,
			Cap:       b.aggregateCap,
			Message:   "zero live process hash build generation cap",
		}
	}

	if spillDiskCap == 0 {
		spillDiskCap = defaultSpillCap(memoryCap)
	}
	if spillDiskCap > b.spillDiskCap {
		spillDiskCap = b.spillDiskCap
	}
	configuredFDCap := configuredSpillFDCap(memoryCap)
	if configuredFDCap > b.spillFDConfiguredCap {
		configuredFDCap = b.spillFDConfiguredCap
	}
	effectiveFDCap := configuredFDCap
	if effectiveFDCap > b.spillFDCap {
		effectiveFDCap = b.spillFDCap
	}

	return &HashBuildBudgetGeneration{
		budget:               b,
		id:                   id,
		cap:                  memoryCap,
		spillDiskCap:         spillDiskCap,
		spillFDConfiguredCap: configuredFDCap,
		spillFDCap:           effectiveFDCap,
	}, nil
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
		AllocationUsed: g.allocationUsed,
		ReserveCount:   g.reserveCount, RejectCount: g.rejectCount, ReconcileCount: g.reconcileCount, ReleaseCount: g.releaseCount,
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

// AllocationAccountRegistry returns the bounded CN-local registry shared by
// every activated HashBuild generation under this aggregate budget. The slot
// bound follows a conservation fact rather than a per-operator multiplier:
// every live allocation owns at least one byte and all accounts share the
// aggregate byte cap, so live metadata cannot exceed aggregate capacity. A
// second fixed bound reserves at most 2 GiB of the existing 4 GiB CN
// headroom at 128 bytes per metadata entry. The registry stores only the
// resulting scalar limit; it does not preallocate one object per slot.
func (g *HashBuildBudgetGeneration) AllocationAccountRegistry() (
	*mpool.AllocationAccountRegistry,
	error,
) {
	if g == nil || g.budget == nil {
		return nil, ErrHashBuildBudgetInvalid
	}
	b := g.budget
	b.allocationRegistryOnce.Do(func() {
		capBytes := b.AggregateCap()
		if capBytes == 0 {
			b.allocationRegistryErr = ErrHashBuildBudgetInvalid
			return
		}
		allocationSlots := min(capBytes, hashBuildAllocationMetadataMaxSlots)
		b.allocationRegistry, b.allocationRegistryErr =
			mpool.NewAllocationAccountRegistry(
				hashBuildAllocationGenerationSlots,
				allocationSlots,
			)
	})
	return b.allocationRegistry, b.allocationRegistryErr
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

// AcquireAllocationCapacity adapts allocation-accounted MPool ownership into
// the existing HashBuild query/CN policy during migration. It creates no
// independently releasable reservation token: the physical allocation lease
// is the sole release owner.
func (g *HashBuildBudgetGeneration) AcquireAllocationCapacity(size uint64) error {
	if size == 0 {
		return nil
	}
	_, err := g.reserve(size, true)
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrHashBuildBudgetClosed):
		return errors.Join(mpool.ErrAllocationAccountSealed, err)
	case errors.Is(err, ErrHashBuildBudgetAdmission):
		return errors.Join(mpool.ErrAllocationAccountCapacity, err)
	default:
		return errors.Join(mpool.ErrAllocationAccountInvariant, err)
	}
}

// ReleaseAllocationCapacity is called only by physical MPool Free through the
// allocation account. A mismatch is an ownership invariant failure.
func (g *HashBuildBudgetGeneration) ReleaseAllocationCapacity(size uint64) {
	if size == 0 {
		return
	}
	if g == nil || g.budget == nil {
		panic("nil hash build allocation capacity controller")
	}
	b := g.budget
	b.mu.Lock()
	if g.allocationUsed < size || g.used < size || b.aggregateUsed < size {
		b.mu.Unlock()
		panic("hash build allocation capacity release underflow")
	}
	g.allocationUsed -= size
	g.used -= size
	b.aggregateUsed -= size
	g.releaseCount++
	b.mu.Unlock()
	observeHashBuildBudget("memory", "release", "query", size)
	observeHashBuildBudget("memory", "release", "cn", size)
}

// Reserve performs the required two-level sequence: charge CN aggregate,
// then charge query-CN.  If query-CN rejects, aggregate is rolled back before
// returning, so callers never observe a partial reservation.
func (g *HashBuildBudgetGeneration) Reserve(size uint64) (*HashBuildReservation, error) {
	return g.reserve(size, false)
}

func (g *HashBuildBudgetGeneration) reserve(
	size uint64,
	allocationOwned bool,
) (*HashBuildReservation, error) {
	if g == nil || g.budget == nil {
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Message: "nil hash build generation"}
	}
	b := g.budget
	// A closed budget/generation has a deterministic lifecycle result and does
	// not need to touch the live provider.
	b.mu.Lock()
	if b.closed || g.closed {
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "query", size)
		err := &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: size, Used: g.used, Cap: g.cap}
		b.mu.Unlock()
		return nil, err
	}

	// The common cached-cap path decides whether a refresh is needed and updates
	// the ledger under one b.mu acquisition. Only an expired cache drops the
	// lock and enters the refresh single-flight gate.
	_, epoch, hasProvider, cached, err := b.aggregateCapRefreshDecisionLocked(false, 0)
	if cached {
		if err != nil {
			b.mu.Unlock()
			return nil, err
		}
		token, firstErr, aggregateRejected := g.reserveLocked(
			size,
			false,
			allocationOwned,
		)
		b.mu.Unlock()
		if firstErr == nil && !aggregateRejected {
			observeHashBuildBudget("memory", "reserve", "query", size)
			observeHashBuildBudget("memory", "reserve", "cn", size)
		}
		if !aggregateRejected {
			return token, firstErr
		}
	} else {
		b.mu.Unlock()
		// The provider is sampled once on first use and then shared by all
		// reservations until the TTL expires. refreshAggregateCap serializes
		// only the sampling itself.
		var refreshed bool
		epoch, hasProvider, refreshed, err = b.refreshAggregateCap(false, 0)
		if err != nil {
			return nil, err
		}
		b.mu.Lock()
		token, firstErr, aggregateRejected := g.reserveLocked(
			size,
			false,
			allocationOwned,
		)
		b.mu.Unlock()
		if firstErr == nil && !aggregateRejected {
			observeHashBuildBudget("memory", "reserve", "query", size)
			observeHashBuildBudget("memory", "reserve", "cn", size)
		}
		if !aggregateRejected {
			return token, firstErr
		}
		if refreshed {
			hasProvider = false
		}
	}

	// A stale cached cap can reject a request even though the physical ceiling
	// has grown. Before returning an aggregate rejection, force one refresh.
	// The epoch check turns concurrent retries into a single-flight operation.
	if hasProvider {
		if _, _, _, err = b.refreshAggregateCap(true, epoch); err != nil {
			return nil, err
		}
	}

	b.mu.Lock()
	token, err, aggregateRejected := g.reserveLocked(
		size,
		true,
		allocationOwned,
	)
	b.mu.Unlock()
	if err == nil && !aggregateRejected {
		observeHashBuildBudget("memory", "reserve", "query", size)
		observeHashBuildBudget("memory", "reserve", "cn", size)
	}
	if aggregateRejected {
		return nil, err
	}
	return token, err
}

// reserveLocked attempts one memory reservation. b.mu must be held. The bool
// result identifies an aggregate-cap failure so Reserve can trigger a forced
// live-ceiling refresh without counting a transient failure as a rejection.
func (g *HashBuildBudgetGeneration) reserveLocked(
	size uint64,
	recordAggregateReject bool,
	allocationOwned bool,
) (*HashBuildReservation, error, bool) {
	b := g.budget
	if b.closed || g.closed {
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "query", size)
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: size, Used: g.used, Cap: g.cap}, false
	}
	// Check by subtraction rather than used+size: this is safe for
	// math.MaxUint64 and rejects every overflow-sized request.
	if b.aggregateUsed > b.aggregateCap || size > b.aggregateCap-b.aggregateUsed {
		if recordAggregateReject {
			g.rejectCount++
			observeHashBuildBudget("memory", "reject", "cn", size)
		}
		return nil, newAdmissionError(size, b.aggregateUsed, b.aggregateCap), true
	}
	b.aggregateUsed += size
	if g.used > g.cap || size > g.cap-g.used {
		// Roll back the complete CN charge before returning the rejection.
		b.aggregateUsed -= size
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "query", size)
		return nil, newAdmissionError(size, g.used, g.cap), false
	}
	g.used += size
	g.reserveCount++
	if g.used > g.peakUsed {
		g.peakUsed = g.used
	}
	if allocationOwned {
		if size > math.MaxUint64-g.allocationUsed {
			panic("hash build allocation capacity overflow")
		}
		g.allocationUsed += size
		return nil, nil, false
	}
	return &HashBuildReservation{budget: b, generation: g, core: &hashBuildReservationCore{size: size}}, nil, false
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
	b.mu.Lock()
	if r.core.state.Load() != hashBuildReservationActive {
		b.mu.Unlock()
		return ErrHashBuildReservationInactive
	}
	if b.closed || r.generation.closed {
		r.generation.rejectCount++
		observeHashBuildBudget("memory", "reject", "query", additional)
		err := &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: additional, Used: r.generation.used, Cap: r.generation.cap}
		b.mu.Unlock()
		return err
	}

	_, epoch, hasProvider, cached, err := b.aggregateCapRefreshDecisionLocked(false, 0)
	if cached {
		if err != nil {
			b.mu.Unlock()
			return err
		}
		firstErr, aggregateRejected := r.growLocked(additional, false)
		b.mu.Unlock()
		if firstErr == nil {
			observeHashBuildBudget("memory", "reserve", "query", additional)
			observeHashBuildBudget("memory", "reserve", "cn", additional)
		}
		if !aggregateRejected {
			return firstErr
		}
	} else {
		b.mu.Unlock()
		var refreshed bool
		epoch, hasProvider, refreshed, err = b.refreshAggregateCap(false, 0)
		if err != nil {
			return err
		}
		b.mu.Lock()
		firstErr, aggregateRejected := r.growLocked(additional, false)
		b.mu.Unlock()
		if firstErr == nil {
			observeHashBuildBudget("memory", "reserve", "query", additional)
			observeHashBuildBudget("memory", "reserve", "cn", additional)
		}
		if !aggregateRejected {
			return firstErr
		}
		if refreshed {
			hasProvider = false
		}
	}

	if hasProvider {
		if _, _, _, err = b.refreshAggregateCap(true, epoch); err != nil {
			return err
		}
	}
	b.mu.Lock()
	err, aggregateRejected := r.growLocked(additional, true)
	b.mu.Unlock()
	if err == nil {
		observeHashBuildBudget("memory", "reserve", "query", additional)
		observeHashBuildBudget("memory", "reserve", "cn", additional)
	}
	if aggregateRejected {
		return err
	}
	return err
}

// growLocked attempts one memory reservation growth. b.mu must be held.
func (r *HashBuildReservation) growLocked(additional uint64, recordAggregateReject bool) (error, bool) {
	b := r.budget
	g := r.generation
	if r.core.state.Load() != hashBuildReservationActive {
		return ErrHashBuildReservationInactive, false
	}
	if b.closed || g.closed {
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "query", additional)
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: additional, Used: g.used, Cap: g.cap}, false
	}
	if b.aggregateUsed > b.aggregateCap || additional > b.aggregateCap-b.aggregateUsed {
		// Defer the counter/metric until the caller knows whether a forced
		// refresh can make this transient failure admissible.
		if recordAggregateReject {
			g.rejectCount++
			observeHashBuildBudget("memory", "reject", "cn", additional)
		}
		return newAdmissionError(additional, b.aggregateUsed, b.aggregateCap), true
	}
	if g.used > g.cap || additional > g.cap-g.used {
		g.rejectCount++
		observeHashBuildBudget("memory", "reject", "query", additional)
		return newAdmissionError(additional, g.used, g.cap), false
	}
	if r.core.size > math.MaxUint64-additional {
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Requested: additional, Message: "hash build reservation size overflow"}, false
	}
	b.aggregateUsed += additional
	g.used += additional
	r.core.size += additional
	if g.used > g.peakUsed {
		g.peakUsed = g.used
	}
	g.reserveCount++
	return nil, false
}

func newAdmissionError(requested, used, cap uint64) error {
	return newComponentAdmissionError(
		HashBuildBudgetComponentMemory,
		requested,
		used,
		cap,
	)
}

func newComponentAdmissionError(
	component HashBuildBudgetComponent,
	requested uint64,
	used uint64,
	cap uint64,
) error {
	return &HashBuildBudgetError{
		Kind:      HashBuildBudgetErrorAdmission,
		Component: component,
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
	if !r.core.state.CompareAndSwap(hashBuildReservationActive, hashBuildReservationReleased) {
		r.budget.mu.Unlock()
		return false
	}
	size := r.core.size
	// The subtraction is exact for a live token.  Keep a defensive branch so
	// corrupted state cannot underflow and turn into an apparent huge charge.
	if r.generation.used >= size {
		r.generation.used -= size
	} else {
		r.generation.used = 0
	}
	if r.budget.aggregateUsed >= size {
		r.budget.aggregateUsed -= size
	} else {
		r.budget.aggregateUsed = 0
	}
	r.generation.releaseCount++
	r.budget.mu.Unlock()
	observeHashBuildBudget("memory", "release", "query", size)
	observeHashBuildBudget("memory", "release", "cn", size)
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
	if r.core.state.Load() != hashBuildReservationActive {
		r.budget.mu.Unlock()
		return false, ErrHashBuildReservationInactive
	}
	if actual > r.core.size {
		r.budget.mu.Unlock()
		return false, ErrHashBuildReservationUpward
	}
	delta := r.core.size - actual
	if delta > 0 {
		if r.generation.used < delta || r.budget.aggregateUsed < delta {
			r.budget.mu.Unlock()
			return false, ErrHashBuildReservationInactive
		}
		r.generation.used -= delta
		r.budget.aggregateUsed -= delta
		r.core.size = actual
	}
	r.generation.reconcileCount++
	r.budget.mu.Unlock()
	if delta > 0 {
		observeHashBuildBudget("memory", "reconcile", "query", delta)
		observeHashBuildBudget("memory", "reconcile", "cn", delta)
	}
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
		return nil, newComponentAdmissionError(HashBuildBudgetComponentSpillDisk, size, b.spillDiskUsed, b.spillDiskCap)
	}
	if g.spillDiskUsed > g.spillDiskCap || size > g.spillDiskCap-g.spillDiskUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_disk", "reject", "query", size)
		return nil, newComponentAdmissionError(HashBuildBudgetComponentSpillDisk, size, g.spillDiskUsed, g.spillDiskCap)
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
		observeHashBuildBudget("spill_disk", "reject", "query", additional)
		return &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: additional}
	}
	if b.spillDiskUsed > b.spillDiskCap || additional > b.spillDiskCap-b.spillDiskUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_disk", "reject", "cn", additional)
		return newComponentAdmissionError(HashBuildBudgetComponentSpillDisk, additional, b.spillDiskUsed, b.spillDiskCap)
	}
	if g.spillDiskUsed > g.spillDiskCap || additional > g.spillDiskCap-g.spillDiskUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_disk", "reject", "query", additional)
		return newComponentAdmissionError(HashBuildBudgetComponentSpillDisk, additional, g.spillDiskUsed, g.spillDiskCap)
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
	// Sample while holding the ledger lock. Otherwise an older high-limit
	// sample can be delayed, overtake a concurrent low-limit sample, and raise
	// the effective cap again after an administrator lowers RLIMIT_NOFILE.
	processLimit, limitKnown := processOpenFileLimit()
	if b.closed || g.closed {
		g.rejectCount++
		observeHashBuildBudget("spill_fd", "reject", "query", size)
		return nil, &HashBuildBudgetError{Kind: HashBuildBudgetErrorClosed, Requested: size}
	}
	b.spillFDCap = clampSpillFDCap(b.spillFDConfiguredCap, processLimit, limitKnown)
	g.spillFDCap = g.spillFDConfiguredCap
	if g.spillFDCap > b.spillFDCap {
		g.spillFDCap = b.spillFDCap
	}
	if b.spillFDUsed > b.spillFDCap || size > b.spillFDCap-b.spillFDUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_fd", "reject", "cn", size)
		return nil, newComponentAdmissionError(HashBuildBudgetComponentSpillFD, size, b.spillFDUsed, b.spillFDCap)
	}
	if g.spillFDUsed > g.spillFDCap || size > g.spillFDCap-g.spillFDUsed {
		g.rejectCount++
		observeHashBuildBudget("spill_fd", "reject", "query", size)
		return nil, newComponentAdmissionError(HashBuildBudgetComponentSpillFD, size, g.spillFDUsed, g.spillFDCap)
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
	initialInputs := hashBuildProcessMemoryInputs
	initialInputs.GlobalMpoolCap = globalCap
	initialInputs.FileCacheHint = fileCacheHint
	initialInputs.ProcessLimitationSize = queryLimit
	ceiling, err := ResolveHashBuildCeiling(initialInputs)
	if err != nil {
		return nil, err
	}

	var aggregate *HashBuildBudget
	service := proc.GetService()
	if service == "" {
		service = "__process_local_cn__"
	}
	value, loaded := hashBuildCNBudgets.Load(service)
	if !loaded {
		candidate := func() *HashBuildBudget {
			b, createErr := NewHashBuildBudget(ceiling.CNHashCap, ceiling.CNHashCap)
			if createErr != nil {
				return nil
			}
			// Attach the source snapshot before publishing the candidate so
			// another process never observes an aggregate without its stable
			// provider.
			b.installCNCapProvider(initialInputs)
			return b
		}()
		value, loaded = hashBuildCNBudgets.LoadOrStore(service, candidate)
	}
	aggregate, _ = value.(*HashBuildBudget)
	if aggregate == nil {
		err = &HashBuildBudgetError{Kind: HashBuildBudgetErrorInvalid, Message: "failed to initialize CN hash build budget"}
	}
	if err != nil {
		return nil, err
	}
	if loaded {
		aggregate.mergeObservedCNCap(initialInputs, ceiling.CNHashCap)
		// The provider installed by the winning aggregate owns its evolving
		// source snapshot. Refreshing it under refreshMu avoids applying ceiling
		// samples completed out of order by concurrent statements.
		if _, _, _, err = aggregate.refreshAggregateCap(false, 0); err != nil {
			return nil, err
		}
	}
	spillDiskCap := uint64(0)
	if proc.Base.Lim.SpillSize > 0 {
		spillDiskCap = uint64(proc.Base.Lim.SpillSize)
		if err = aggregate.raiseSpillDiskCapToExplicitLimit(spillDiskCap); err != nil {
			return nil, err
		}
	}
	generation, err := aggregate.openProcessGeneration(
		hashBuildGenerationSequence.Add(1),
		ceiling.QueryCap,
		spillDiskCap,
	)
	if err != nil {
		return nil, err
	}
	proc.Base.hashBuildBudget = generation
	return generation, nil
}
