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

const executionResourceMinimumReserve = uint64(4 << 30)

const (
	executionResourceAllocationGenerationSlots = uint32(131_072)
	// Account metadata lives outside the MPool payload cap. Bound its worst-case
	// Go-heap footprint to half of executionResourceMinimumReserve: 128 bytes covers one
	// sparse pointer/lease-map entry, and 16,777,216 live entries consume at most
	// 2 GiB by construction. Small aggregate caps use the tighter byte
	// conservation bound because every physical allocation owns at least one
	// byte. Slot exhaustion is real metadata capacity pressure, not an
	// estimator rejection.
	executionResourceAllocationMetadataBytesPerSlot = uint64(128)
	executionResourceAllocationMetadataHeadroom     = executionResourceMinimumReserve / 2
	executionResourceAllocationMetadataMaxSlots     = executionResourceAllocationMetadataHeadroom / executionResourceAllocationMetadataBytesPerSlot
)

const (
	// Keep a process-wide reserve for listeners, RPC connections, object
	// storage, logs, and other descriptors that are not represented by the
	// execution spill ledger. The proportional reserve keeps the spill
	// subsystem from consuming an otherwise healthy CN's whole RLIMIT, while
	// the absolute floor protects low-limit containers.
	executionResourceNonSpillFDHeadroom    = uint64(64)
	executionResourceNonSpillFDHeadroomDiv = uint64(4)
)

// executionResourceBudgetCapRefreshTTL bounds how long a sampled configuration ceiling
// may be reused. The inputs are limits/reservations, not current memory usage.
// Cgroup max and host total are process-start snapshots; changing either
// requires restarting the CN. Runtime changes to the mpool cap or file-cache
// hint become effective within this window. A rejection below a cached cap
// bypasses the window so legitimate cap growth does not cause avoidable
// spilling.
const executionResourceBudgetCapRefreshTTL = 100 * time.Millisecond

var (
	executionResourceGenerationSequence atomic.Uint64
	executionResourceCNBudgets          sync.Map // service ID -> *ExecutionResourceBudget
	executionResourceBudgetObservers    = newExecutionResourceBudgetObservers()
	// Execution treats physical process memory as a startup contract. Keeping
	// this snapshot process-local avoids cgroup filesystem reads on query
	// admission without changing the live CgroupMemoryLimit API used by other
	// subsystems such as remote compile.
	executionResourceProcessMemoryInputs = ExecutionMemoryCeilingInputs{
		CgroupMemoryMax: system.CgroupMemoryLimit(),
		HostMemTotal:    system.MemoryTotal(),
	}
)

type executionResourceBudgetMetricKey struct {
	component string
	event     string
	scope     string
}

type executionResourceBudgetObserver func(amount uint64)

func newExecutionResourceBudgetObservers() map[executionResourceBudgetMetricKey]executionResourceBudgetObserver {
	components := [...]string{"memory", "spill_disk", "spill_fd"}
	events := [...]string{"reserve", "release", "reconcile", "reject"}
	scopes := [...]string{"query", "cn"}
	observers := make(map[executionResourceBudgetMetricKey]executionResourceBudgetObserver, len(components)*len(events)*len(scopes))
	for _, component := range components {
		for _, event := range events {
			for _, scope := range scopes {
				key := executionResourceBudgetMetricKey{component: component, event: event, scope: scope}
				eventCounter := metricv2.ExecutionResourceBudgetEventCounter.WithLabelValues(component, event, scope)
				amountCounter := metricv2.ExecutionResourceBudgetAmountCounter.WithLabelValues(component, event, scope)
				observers[key] = func(amount uint64) {
					eventCounter.Inc()
					amountCounter.Add(float64(amount))
				}
			}
		}
	}
	return observers
}

// Keep the fixed-cardinality lookup out of allocation-controller bodies.
// Inlining it duplicates map-key construction across every reserve/release
// branch and measurably regresses the physical allocation hot path.
//
//go:noinline
func observeExecutionResourceBudget(component, event, scope string, amount uint64) {
	if observer := executionResourceBudgetObservers[executionResourceBudgetMetricKey{
		component: component,
		event:     event,
		scope:     scope,
	}]; observer != nil {
		observer(amount)
	}
}

// Errors returned by execution-resource admission. These errors live in
// process rather than the SQL layer so that operators and remote execution
// code can make an admission decision without importing frontend packages.
var (
	ErrExecutionResourceAdmission        = moerr.NewInternalErrorNoCtx("execution resource admission rejected")
	ErrExecutionResourceClosed           = moerr.NewInternalErrorNoCtx("execution resource generation is closed")
	ErrExecutionResourceInvalid          = moerr.NewInternalErrorNoCtx("invalid execution resource budget")
	ErrExecutionMemoryCeilingMissing     = moerr.NewInternalErrorNoCtx("execution memory ceiling unavailable")
	ErrExecutionSpillReservationInactive = moerr.NewInternalErrorNoCtx("execution spill reservation is inactive")
	ErrExecutionSpillReservationUpward   = moerr.NewInternalErrorNoCtx("execution spill reservation reconciliation would increase charge")
)

// ExecutionResourceErrorKind identifies the class of a budget error.
type ExecutionResourceErrorKind uint8

const (
	ExecutionResourceErrorAdmission ExecutionResourceErrorKind = iota + 1
	ExecutionResourceErrorClosed
	ExecutionResourceErrorInvalid
	ExecutionResourceErrorCeilingMissing
)

// ExecutionResourceComponent identifies the independently bounded resource that
// rejected an admission. Zero is invalid: every admission error must name its
// physical resource. A spill-disk or spill-FD rejection must never enter the
// memory reclaim/reduce loop because reducing an in-memory batch cannot create
// either resource and may replay already-published spill records.
type ExecutionResourceComponent uint8

const (
	ExecutionResourceComponentMemory ExecutionResourceComponent = iota + 1
	ExecutionResourceComponentSpillDisk
	ExecutionResourceComponentSpillFD
)

// ExecutionResourceError carries bounded, observational details for an
// admission failure. Requested, Used and Cap use the unit named by Resource
// (bytes for memory/spill disk, descriptors for spill FD) and are always safe
// to inspect; they are never produced by overflowing arithmetic.
type ExecutionResourceError struct {
	Kind      ExecutionResourceErrorKind
	Component ExecutionResourceComponent
	Requested uint64
	Used      uint64
	Cap       uint64
	Message   string
}

func (e *ExecutionResourceError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	switch e.Kind {
	case ExecutionResourceErrorAdmission:
		return fmt.Sprintf("%s: requested=%d used=%d cap=%d", ErrExecutionResourceAdmission, e.Requested, e.Used, e.Cap)
	case ExecutionResourceErrorClosed:
		return ErrExecutionResourceClosed.Error()
	case ExecutionResourceErrorInvalid:
		return ErrExecutionResourceInvalid.Error()
	case ExecutionResourceErrorCeilingMissing:
		return ErrExecutionMemoryCeilingMissing.Error()
	default:
		return fmt.Sprintf("%s: unknown kind=%d", ErrExecutionResourceInvalid, e.Kind)
	}
}

func (e *ExecutionResourceError) Unwrap() error {
	if e == nil {
		return nil
	}
	switch e.Kind {
	case ExecutionResourceErrorAdmission:
		return ErrExecutionResourceAdmission
	case ExecutionResourceErrorClosed:
		return ErrExecutionResourceClosed
	case ExecutionResourceErrorInvalid:
		return ErrExecutionResourceInvalid
	case ExecutionResourceErrorCeilingMissing:
		return ErrExecutionMemoryCeilingMissing
	default:
		return ErrExecutionResourceInvalid
	}
}

// Is keeps capacity admission, lifecycle, and accounting failures disjoint.
// Callers may recover a capacity rejection through spill, while Closed and
// other lifecycle failures must remain fatal.
func (e *ExecutionResourceError) Is(target error) bool {
	if e == nil {
		return false
	}
	if target == ErrExecutionResourceAdmission {
		return e.Kind == ExecutionResourceErrorAdmission
	}
	switch e.Kind {
	case ExecutionResourceErrorClosed:
		return target == ErrExecutionResourceClosed
	case ExecutionResourceErrorInvalid:
		return target == ErrExecutionResourceInvalid
	case ExecutionResourceErrorCeilingMissing:
		return target == ErrExecutionMemoryCeilingMissing
	}
	return false
}

// ExecutionResourceBudget is a local-CN aggregate budget.  Each opened generation has
// its own query-CN cap, while all generations charge this aggregate cap.  The
// mutex is deliberately held over the two-level reservation sequence and
// closure transitions.  This gives the operation a simple linearization point
// and, importantly, makes a query rejection roll back its complete CN charge.
type ExecutionResourceBudget struct {
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
	liveCapInputs         ExecutionMemoryCeilingInputs
	liveCapInputsSnapshot atomic.Pointer[ExecutionMemoryCeilingInputs]
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

// NewExecutionResourceBudget creates a local-CN budget.  Both caps are finite and
// positive; queryCap is the cap for each statement execution generation and
// aggregateCap is shared by all generations on this CN.
func NewExecutionResourceBudget(aggregateCap, queryCap uint64) (*ExecutionResourceBudget, error) {
	if aggregateCap == 0 || queryCap == 0 || queryCap > aggregateCap {
		return nil, &ExecutionResourceError{
			Kind:      ExecutionResourceErrorInvalid,
			Requested: queryCap,
			Cap:       aggregateCap,
			Message:   fmt.Sprintf("%s: aggregate=%d query=%d", ErrExecutionResourceInvalid, aggregateCap, queryCap),
		}
	}
	configuredFDCap := configuredSpillFDCap(aggregateCap)
	return &ExecutionResourceBudget{
		aggregateCap: aggregateCap, queryCap: queryCap,
		spillDiskCap:         defaultSpillCap(aggregateCap),
		spillFDConfiguredCap: configuredFDCap,
		spillFDCap:           clampSpillFDCapToProcess(configuredFDCap),
		// Keep direct callers' historical provider semantics (sample before
		// every reservation). CN budgets obtained through GetExecutionResourceBudget
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
	headroom := processLimit / executionResourceNonSpillFDHeadroomDiv
	if headroom < executionResourceNonSpillFDHeadroom {
		headroom = executionResourceNonSpillFDHeadroom
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

// ExecutionResourceBudgetSnapshot is an immutable observational view of CN charges.
type ExecutionResourceBudgetSnapshot struct {
	AggregateCap, AggregateUsed uint64
	SpillDiskCap, SpillDiskUsed uint64
	SpillFDCap, SpillFDUsed     uint64
	Closed                      bool
}

func (b *ExecutionResourceBudget) Snapshot() ExecutionResourceBudgetSnapshot {
	if b == nil {
		return ExecutionResourceBudgetSnapshot{Closed: true}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return ExecutionResourceBudgetSnapshot{b.aggregateCap, b.aggregateUsed, b.spillDiskCap, b.spillDiskUsed, b.spillFDCap, b.spillFDUsed, b.closed}
}

func (b *ExecutionResourceBudget) SpillDiskCap() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spillDiskCap
}
func (b *ExecutionResourceBudget) SpillDiskUsed() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spillDiskUsed
}
func (b *ExecutionResourceBudget) SpillFDCap() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spillFDCap
}
func (b *ExecutionResourceBudget) SpillFDUsed() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spillFDUsed
}

// SetSpillCaps configures finite CN spill caps. Zero values restore defaults.
func (b *ExecutionResourceBudget) SetSpillCaps(diskBytes, fds uint64) error {
	if b == nil {
		return &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid}
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
		return newComponentAdmissionError(ExecutionResourceComponentSpillDisk, 0, b.spillDiskUsed, diskBytes)
	}
	if b.spillFDUsed > effectiveFDCap {
		return newComponentAdmissionError(ExecutionResourceComponentSpillFD, 0, b.spillFDUsed, effectiveFDCap)
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
func (b *ExecutionResourceBudget) raiseSpillDiskCapToExplicitLimit(diskBytes uint64) error {
	if b == nil {
		return &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid}
	}
	if diskBytes == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return &ExecutionResourceError{
			Kind:    ExecutionResourceErrorClosed,
			Message: ErrExecutionResourceClosed.Error(),
		}
	}
	if diskBytes > b.spillDiskCap {
		b.spillDiskCap = diskBytes
	}
	return nil
}

// MustNewExecutionResourceBudget is a convenience for initialization code with
// statically validated limits.
func MustNewExecutionResourceBudget(aggregateCap, queryCap uint64) *ExecutionResourceBudget {
	b, err := NewExecutionResourceBudget(aggregateCap, queryCap)
	if err != nil {
		panic(err)
	}
	return b
}

// AggregateCap returns the configured local-CN cap.
func (b *ExecutionResourceBudget) AggregateCap() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.aggregateCap
}

// QueryCap returns the per-generation, per-target-CN cap.
func (b *ExecutionResourceBudget) QueryCap() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.queryCap
}

// AggregateUsed reports bytes currently charged by all live generations.
func (b *ExecutionResourceBudget) AggregateUsed() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.aggregateUsed
}

// Closed reports whether no new generation or reservation may be opened.
func (b *ExecutionResourceBudget) Closed() bool {
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
func (b *ExecutionResourceBudget) Close() {
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
func (b *ExecutionResourceBudget) refreshAggregateCap(force bool, expectedEpoch uint64) (epoch uint64, hasProvider bool, refreshed bool, err error) {
	if b == nil {
		return 0, false, false, &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid}
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
		providerErr = &ExecutionResourceError{Kind: ExecutionResourceErrorCeilingMissing, Message: "live execution memory ceiling is zero"}
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
func (b *ExecutionResourceBudget) aggregateCapRefreshDecision(
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
func (b *ExecutionResourceBudget) aggregateCapRefreshDecisionLocked(
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
func (b *ExecutionResourceBudget) UpdateAggregateCap(cap uint64) error {
	if b == nil || cap == 0 {
		return &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Requested: cap}
	}
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	b.aggregateCap = cap
	if b.queryCap > cap {
		b.queryCap = cap
	}
	// GetExecutionResourceBudget installs its provider first, then calls this method
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
func (b *ExecutionResourceBudget) SetAggregateCapProvider(provider func() (uint64, error)) {
	if b == nil {
		return
	}
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	b.capProvider = provider
	// Installing or replacing a provider starts a fresh epoch. Callers that
	// already sampled a ceiling can seed the cache with UpdateAggregateCap
	// immediately afterward (as GetExecutionResourceBudget does).
	b.capCached = false
	b.capRefreshErr = nil
	b.capRefreshEpoch++
	b.mu.Unlock()
}

// installCNCapProvider publishes a new CN aggregate with its initial source
// snapshot already attached. The candidate is not placed in the service map
// until this method returns.
func (b *ExecutionResourceBudget) installCNCapProvider(inputs ExecutionMemoryCeilingInputs) {
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.liveCapInputs = inputs
	b.publishLiveCNCapInputs()
	b.mu.Lock()
	b.capProvider = b.sampleCNCap
	b.capRefreshTTL = executionResourceBudgetCapRefreshTTL
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
func (b *ExecutionResourceBudget) mergeObservedCNCap(inputs ExecutionMemoryCeilingInputs, cap uint64) {
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

func (b *ExecutionResourceBudget) observedCNCapInputsCurrent(inputs ExecutionMemoryCeilingInputs, cap uint64) bool {
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
// GetExecutionResourceBudget path. The caller holds refreshMu while mutating and
// publishing liveCapInputs.
func (b *ExecutionResourceBudget) publishLiveCNCapInputs() {
	snapshot := b.liveCapInputs
	b.liveCapInputsSnapshot.Store(&snapshot)
}

// sampleCNCap is invoked only while refreshMu is held. Physical process memory
// remains fixed at the package-start snapshot; only runtime mpool and
// file-cache inputs are sampled here. A runtime source reports unavailable as
// zero, so resolveCNCapSample retains its last finite value rather than
// interpreting a transient disappearance as extra memory.
func (b *ExecutionResourceBudget) sampleCNCap() (uint64, error) {
	current := executionResourceProcessMemoryInputs
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
func (b *ExecutionResourceBudget) resolveCNCapSample(current ExecutionMemoryCeilingInputs) (uint64, error) {
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
	ceiling, err := ResolveExecutionMemoryCeiling(current)
	if err != nil {
		return 0, err
	}
	b.liveCapInputs = current
	b.publishLiveCNCapInputs()
	return ceiling.CNMemoryCap, nil
}

// ExecutionResourceGeneration is a statement execution generation on one CN.
// A generation's charge is independent from every other generation, even if a
// caller happens to reuse its numeric ID after the old generation has closed.
type ExecutionResourceGeneration struct {
	budget                                                  *ExecutionResourceBudget
	id                                                      uint64
	cap                                                     uint64
	used                                                    uint64
	closed                                                  bool
	spillDiskCap, spillDiskUsed                             uint64
	spillFDConfiguredCap, spillFDCap, spillFDUsed           uint64
	reserveCount, rejectCount, reconcileCount, releaseCount uint64
	peakUsed                                                uint64
}

var _ mpool.AllocationCapacityController = (*ExecutionResourceGeneration)(nil)

// ExecutionResourceGenerationSnapshot is an immutable fixed-cardinality view.
type ExecutionResourceGenerationSnapshot struct {
	ID, Cap, Used, PeakUsed                                 uint64
	ReserveCount, RejectCount, ReconcileCount, ReleaseCount uint64
	SpillDiskCap, SpillDiskUsed, SpillFDCap                 uint64
	SpillFDUsed                                             uint64
	Closed                                                  bool
}

// OpenGeneration opens a per-statement execution generation.  The budget's
// query cap is copied by reference (and remains immutable), while used bytes
// belong solely to the returned generation.
func (b *ExecutionResourceBudget) OpenGeneration(id uint64) (*ExecutionResourceGeneration, error) {
	if b == nil {
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Message: "nil execution resource budget"}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorClosed, Message: ErrExecutionResourceClosed.Error()}
	}
	configuredFDCap := configuredSpillFDCap(b.queryCap)
	if configuredFDCap > b.spillFDConfiguredCap {
		configuredFDCap = b.spillFDConfiguredCap
	}
	effectiveFDCap := configuredFDCap
	if effectiveFDCap > b.spillFDCap {
		effectiveFDCap = b.spillFDCap
	}
	return &ExecutionResourceGeneration{budget: b, id: id, cap: b.queryCap,
		spillDiskCap:         defaultSpillCap(b.queryCap),
		spillFDConfiguredCap: configuredFDCap,
		spillFDCap:           effectiveFDCap}, nil
}

// OpenGenerationWithCap opens a generation with a query-specific cap while
// retaining the same CN aggregate. This is used when process.Limitation.Size
// narrows one statement below the CN default.
func (b *ExecutionResourceBudget) OpenGenerationWithCap(id, cap uint64) (*ExecutionResourceGeneration, error) {
	if b == nil || cap == 0 {
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Requested: cap, Message: "invalid execution resource generation cap"}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorClosed, Message: ErrExecutionResourceClosed.Error()}
	}
	if cap > b.aggregateCap {
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Requested: cap, Cap: b.aggregateCap}
	}
	configuredFDCap := configuredSpillFDCap(cap)
	if configuredFDCap > b.spillFDConfiguredCap {
		configuredFDCap = b.spillFDConfiguredCap
	}
	effectiveFDCap := configuredFDCap
	if effectiveFDCap > b.spillFDCap {
		effectiveFDCap = b.spillFDCap
	}
	return &ExecutionResourceGeneration{budget: b, id: id, cap: cap,
		spillDiskCap:         defaultSpillCap(cap),
		spillFDConfiguredCap: configuredFDCap,
		spillFDCap:           effectiveFDCap}, nil
}

// OpenGenerationWithSpillCaps opens a generation with explicit memory, disk,
// and file-descriptor ceilings. Zero spill values use the documented defaults.
func (b *ExecutionResourceBudget) OpenGenerationWithSpillCaps(id, memoryCap, spillDiskCap, spillFDCap uint64) (*ExecutionResourceGeneration, error) {
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

// openProcessGeneration opens the generation selected by GetExecutionResourceBudget.
// The resolved query cap can become stale before another statement finishes
// lowering the shared CN aggregate. Clamp and construct under one aggregate
// lock so an ordinary process cannot observe that race as an invalid budget.
func (b *ExecutionResourceBudget) openProcessGeneration(
	id, requestedMemoryCap, spillDiskCap uint64,
) (*ExecutionResourceGeneration, error) {
	if b == nil || requestedMemoryCap == 0 {
		return nil, &ExecutionResourceError{
			Kind:      ExecutionResourceErrorInvalid,
			Requested: requestedMemoryCap,
			Message:   "invalid process execution resource generation cap",
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, &ExecutionResourceError{
			Kind:    ExecutionResourceErrorClosed,
			Message: ErrExecutionResourceClosed.Error(),
		}
	}

	memoryCap := min(requestedMemoryCap, b.aggregateCap)
	if memoryCap == 0 {
		return nil, &ExecutionResourceError{
			Kind:      ExecutionResourceErrorInvalid,
			Requested: requestedMemoryCap,
			Cap:       b.aggregateCap,
			Message:   "zero live process execution resource generation cap",
		}
	}

	if spillDiskCap == 0 {
		spillDiskCap = defaultSpillCap(memoryCap)
	}
	spillDiskCap = min(spillDiskCap, b.spillDiskCap)
	configuredFDCap := min(
		configuredSpillFDCap(memoryCap),
		b.spillFDConfiguredCap,
	)
	effectiveFDCap := min(configuredFDCap, b.spillFDCap)

	return &ExecutionResourceGeneration{
		budget:               b,
		id:                   id,
		cap:                  memoryCap,
		spillDiskCap:         spillDiskCap,
		spillFDConfiguredCap: configuredFDCap,
		spillFDCap:           effectiveFDCap,
	}, nil
}

// ID returns the execution generation identity.
func (g *ExecutionResourceGeneration) ID() uint64 {
	if g == nil {
		return 0
	}
	return g.id
}

// Cap returns this generation's query-CN cap.
func (g *ExecutionResourceGeneration) Cap() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	return g.cap
}

// Used reports bytes reserved by this generation.
func (g *ExecutionResourceGeneration) Used() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.used
}

// Peak reports the maximum physically owned bytes observed by this
// generation. It is observational only; admission and release remain owned by
// AllocationAccount-backed MPool allocations.
func (g *ExecutionResourceGeneration) Peak() uint64 {
	return g.Snapshot().PeakUsed
}

// RejectCount reports physical allocation-capacity rejections observed by
// this generation.
func (g *ExecutionResourceGeneration) RejectCount() uint64 {
	return g.Snapshot().RejectCount
}

func (g *ExecutionResourceGeneration) SpillDiskCap() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.spillDiskCap
}
func (g *ExecutionResourceGeneration) SpillDiskUsed() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.spillDiskUsed
}
func (g *ExecutionResourceGeneration) SpillFDCap() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.spillFDCap
}
func (g *ExecutionResourceGeneration) SpillFDUsed() uint64 {
	if g == nil || g.budget == nil {
		return 0
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.spillFDUsed
}

func (g *ExecutionResourceGeneration) Snapshot() ExecutionResourceGenerationSnapshot {
	if g == nil || g.budget == nil {
		return ExecutionResourceGenerationSnapshot{Closed: true}
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return ExecutionResourceGenerationSnapshot{
		ID: g.id, Cap: g.cap, Used: g.used, PeakUsed: g.peakUsed,
		ReserveCount: g.reserveCount, RejectCount: g.rejectCount, ReconcileCount: g.reconcileCount, ReleaseCount: g.releaseCount,
		SpillDiskCap: g.spillDiskCap, SpillDiskUsed: g.spillDiskUsed, SpillFDCap: g.spillFDCap, SpillFDUsed: g.spillFDUsed,
		Closed: g.closed || g.budget.closed,
	}
}

// Closed reports whether this generation rejects new reservations.
func (g *ExecutionResourceGeneration) Closed() bool {
	if g == nil || g.budget == nil {
		return true
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	return g.closed || g.budget.closed
}

// AllocationAccountRegistry returns the bounded CN-local registry shared by
// every execution generation under this aggregate budget. The slot
// bound follows a conservation fact rather than a per-operator multiplier:
// every live allocation owns at least one byte and all accounts share the
// aggregate byte cap, so live metadata cannot exceed aggregate capacity. A
// second fixed bound reserves at most 2 GiB of the existing 4 GiB CN
// headroom at 128 bytes per metadata entry. The registry stores only the
// resulting scalar limit; it does not preallocate one object per slot.
func (g *ExecutionResourceGeneration) AllocationAccountRegistry() (
	*mpool.AllocationAccountRegistry,
	error,
) {
	if g == nil || g.budget == nil {
		return nil, ErrExecutionResourceInvalid
	}
	b := g.budget
	b.allocationRegistryOnce.Do(func() {
		capBytes := b.AggregateCap()
		if capBytes == 0 {
			b.allocationRegistryErr = ErrExecutionResourceInvalid
			return
		}
		allocationSlots := min(capBytes, executionResourceAllocationMetadataMaxSlots)
		b.allocationRegistry, b.allocationRegistryErr =
			mpool.NewAllocationAccountRegistry(
				executionResourceAllocationGenerationSlots,
				allocationSlots,
			)
	})
	return b.allocationRegistry, b.allocationRegistryErr
}

// Close rejects future reservations for this generation while allowing all
// currently live tokens to release.  It is idempotent.
func (g *ExecutionResourceGeneration) Close() {
	if g == nil || g.budget == nil {
		return
	}
	g.budget.mu.Lock()
	g.closed = true
	g.budget.mu.Unlock()
}

// AcquireAllocationCapacity applies the execution query/CN policy to a physical
// MPool allocation. It creates no independently releasable reservation token:
// the physical allocation lease is the sole release owner.
func (g *ExecutionResourceGeneration) AcquireAllocationCapacity(size uint64) error {
	if size == 0 {
		return nil
	}
	err := g.acquireMemory(size)
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrExecutionResourceClosed):
		return errors.Join(mpool.ErrAllocationAccountSealed, err)
	case errors.Is(err, ErrExecutionResourceAdmission):
		return errors.Join(mpool.ErrAllocationAccountCapacity, err)
	default:
		return errors.Join(mpool.ErrAllocationAccountInvariant, err)
	}
}

// ReleaseAllocationCapacity is called only by physical MPool Free through the
// allocation account. A mismatch is an ownership invariant failure.
func (g *ExecutionResourceGeneration) ReleaseAllocationCapacity(size uint64) {
	if size == 0 {
		return
	}
	if g == nil || g.budget == nil {
		panic("nil execution allocation capacity controller")
	}
	b := g.budget
	b.mu.Lock()
	if g.used < size || b.aggregateUsed < size {
		b.mu.Unlock()
		panic("execution allocation capacity release underflow")
	}
	g.used -= size
	b.aggregateUsed -= size
	g.releaseCount++
	b.mu.Unlock()
	observeExecutionResourceBudget("memory", "release", "query", size)
	observeExecutionResourceBudget("memory", "release", "cn", size)
}

// ExecutionTransientMemoryReservation owns a bounded Go-heap scratch charge.
// Physical MPool allocations continue to use AllocationAccount as their sole
// owner; this token is only for temporary buffers that cannot carry one.
type ExecutionTransientMemoryReservation struct {
	budget     *ExecutionResourceBudget
	generation *ExecutionResourceGeneration
	size       uint64
	state      atomic.Uint32
}

// ReserveTransientMemory admits non-MPool scratch against the same query and
// CN memory ceilings as accounted execution allocations.
func (g *ExecutionResourceGeneration) ReserveTransientMemory(
	size uint64,
) (*ExecutionTransientMemoryReservation, error) {
	if size == 0 {
		return &ExecutionTransientMemoryReservation{}, nil
	}
	if err := g.acquireMemory(size); err != nil {
		return nil, err
	}
	return &ExecutionTransientMemoryReservation{
		budget: g.budget, generation: g, size: size,
	}, nil
}

func (r *ExecutionTransientMemoryReservation) Release() bool {
	if r == nil || r.budget == nil || r.generation == nil || r.size == 0 ||
		!r.state.CompareAndSwap(0, 1) {
		return false
	}
	b := r.budget
	b.mu.Lock()
	if r.generation.used < r.size || b.aggregateUsed < r.size {
		b.mu.Unlock()
		panic("execution transient memory reservation release underflow")
	}
	r.generation.used -= r.size
	b.aggregateUsed -= r.size
	r.generation.releaseCount++
	b.mu.Unlock()
	observeExecutionResourceBudget("memory", "release", "query", r.size)
	observeExecutionResourceBudget("memory", "release", "cn", r.size)
	return true
}

// acquireMemory admits one physical MPool allocation. The allocation account
// is the only owner of the charge and releases it from MPool.Free; there is no
// parallel estimate/reservation token.
func (g *ExecutionResourceGeneration) acquireMemory(size uint64) error {
	if g == nil || g.budget == nil {
		return &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Message: "nil execution resource generation"}
	}
	b := g.budget
	// A closed budget/generation has a deterministic lifecycle result and does
	// not need to touch the live provider.
	b.mu.Lock()
	if b.closed || g.closed {
		g.rejectCount++
		observeExecutionResourceBudget("memory", "reject", "query", size)
		err := &ExecutionResourceError{Kind: ExecutionResourceErrorClosed, Requested: size, Used: g.used, Cap: g.cap}
		b.mu.Unlock()
		return err
	}

	// The common cached-cap path decides whether a refresh is needed and updates
	// the ledger under one b.mu acquisition. Only an expired cache drops the
	// lock and enters the refresh single-flight gate.
	_, epoch, hasProvider, cached, err := b.aggregateCapRefreshDecisionLocked(false, 0)
	if cached {
		if err != nil {
			b.mu.Unlock()
			return err
		}
		firstErr, aggregateRejected := g.acquireMemoryLocked(
			size,
			false,
		)
		b.mu.Unlock()
		if firstErr == nil && !aggregateRejected {
			observeExecutionResourceBudget("memory", "reserve", "query", size)
			observeExecutionResourceBudget("memory", "reserve", "cn", size)
		}
		if !aggregateRejected {
			return firstErr
		}
	} else {
		b.mu.Unlock()
		// The provider is sampled once on first use and then shared by all
		// reservations until the TTL expires. refreshAggregateCap serializes
		// only the sampling itself.
		var refreshed bool
		epoch, hasProvider, refreshed, err = b.refreshAggregateCap(false, 0)
		if err != nil {
			return err
		}
		b.mu.Lock()
		firstErr, aggregateRejected := g.acquireMemoryLocked(
			size,
			false,
		)
		b.mu.Unlock()
		if firstErr == nil && !aggregateRejected {
			observeExecutionResourceBudget("memory", "reserve", "query", size)
			observeExecutionResourceBudget("memory", "reserve", "cn", size)
		}
		if !aggregateRejected {
			return firstErr
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
			return err
		}
	}

	b.mu.Lock()
	err, aggregateRejected := g.acquireMemoryLocked(
		size,
		true,
	)
	b.mu.Unlock()
	if err == nil && !aggregateRejected {
		observeExecutionResourceBudget("memory", "reserve", "query", size)
		observeExecutionResourceBudget("memory", "reserve", "cn", size)
	}
	if aggregateRejected {
		return err
	}
	return err
}

// acquireMemoryLocked attempts one physical-memory admission. b.mu must be
// held. The bool result identifies an aggregate-cap failure so the caller can
// trigger a forced
// live-ceiling refresh without counting a transient failure as a rejection.
func (g *ExecutionResourceGeneration) acquireMemoryLocked(
	size uint64,
	recordAggregateReject bool,
) (error, bool) {
	b := g.budget
	if b.closed || g.closed {
		g.rejectCount++
		observeExecutionResourceBudget("memory", "reject", "query", size)
		return &ExecutionResourceError{Kind: ExecutionResourceErrorClosed, Requested: size, Used: g.used, Cap: g.cap}, false
	}
	// Check by subtraction rather than used+size: this is safe for
	// math.MaxUint64 and rejects every overflow-sized request.
	if b.aggregateUsed > b.aggregateCap || size > b.aggregateCap-b.aggregateUsed {
		if recordAggregateReject {
			g.rejectCount++
			observeExecutionResourceBudget("memory", "reject", "cn", size)
		}
		return newAdmissionError(size, b.aggregateUsed, b.aggregateCap), true
	}
	b.aggregateUsed += size
	if g.used > g.cap || size > g.cap-g.used {
		// Roll back the complete CN charge before returning the rejection.
		b.aggregateUsed -= size
		g.rejectCount++
		observeExecutionResourceBudget("memory", "reject", "query", size)
		return newAdmissionError(size, g.used, g.cap), false
	}
	g.used += size
	g.reserveCount++
	if g.used > g.peakUsed {
		g.peakUsed = g.used
	}
	return nil, false
}

func newAdmissionError(requested, used, cap uint64) error {
	return newComponentAdmissionError(
		ExecutionResourceComponentMemory,
		requested,
		used,
		cap,
	)
}

func newComponentAdmissionError(
	component ExecutionResourceComponent,
	requested uint64,
	used uint64,
	cap uint64,
) error {
	return &ExecutionResourceError{
		Kind:      ExecutionResourceErrorAdmission,
		Component: component,
		Requested: requested,
		Used:      used,
		Cap:       cap,
		Message:   fmt.Sprintf("%s: requested=%d used=%d cap=%d", ErrExecutionResourceAdmission, requested, used, cap),
	}
}

type executionSpillReservationCore struct {
	size  uint64
	state atomic.Uint32
}

const (
	executionSpillReservationActive uint32 = iota
	executionSpillReservationReleased
)

// ExecutionSpillDiskReservation owns query and CN spill-disk bytes.
type ExecutionSpillDiskReservation struct {
	budget     *ExecutionResourceBudget
	generation *ExecutionResourceGeneration
	core       *executionSpillReservationCore
}

// ExecutionSpillFDReservation owns query and CN spill file descriptors.
type ExecutionSpillFDReservation struct {
	budget     *ExecutionResourceBudget
	generation *ExecutionResourceGeneration
	core       *executionSpillReservationCore
}

func (r *ExecutionSpillDiskReservation) Size() uint64 {
	if r == nil || r.budget == nil || r.core == nil {
		return 0
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	return r.core.size
}
func (r *ExecutionSpillFDReservation) Size() uint64 {
	if r == nil || r.budget == nil || r.core == nil {
		return 0
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	return r.core.size
}
func (g *ExecutionResourceGeneration) ReserveSpillDisk(size uint64) (*ExecutionSpillDiskReservation, error) {
	if g == nil || g.budget == nil {
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid}
	}
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || g.closed {
		g.rejectCount++
		observeExecutionResourceBudget("spill_disk", "reject", "query", size)
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorClosed, Requested: size}
	}
	if b.spillDiskUsed > b.spillDiskCap || size > b.spillDiskCap-b.spillDiskUsed {
		g.rejectCount++
		observeExecutionResourceBudget("spill_disk", "reject", "cn", size)
		return nil, newComponentAdmissionError(ExecutionResourceComponentSpillDisk, size, b.spillDiskUsed, b.spillDiskCap)
	}
	if g.spillDiskUsed > g.spillDiskCap || size > g.spillDiskCap-g.spillDiskUsed {
		g.rejectCount++
		observeExecutionResourceBudget("spill_disk", "reject", "query", size)
		return nil, newComponentAdmissionError(ExecutionResourceComponentSpillDisk, size, g.spillDiskUsed, g.spillDiskCap)
	}
	b.spillDiskUsed += size
	g.spillDiskUsed += size
	observeExecutionResourceBudget("spill_disk", "reserve", "query", size)
	observeExecutionResourceBudget("spill_disk", "reserve", "cn", size)
	return &ExecutionSpillDiskReservation{budget: b, generation: g, core: &executionSpillReservationCore{size: size}}, nil
}

// Grow increases one live per-file disk reservation without allocating a new
// bookkeeping token. This keeps metadata proportional to open spill files,
// rather than to the number of tiny batch records written to those files.
func (r *ExecutionSpillDiskReservation) Grow(additional uint64) error {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return ErrExecutionSpillReservationInactive
	}
	if additional == 0 {
		return nil
	}
	b := r.budget
	g := r.generation
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.core.state.Load() != executionSpillReservationActive {
		return ErrExecutionSpillReservationInactive
	}
	if b.closed || g.closed {
		g.rejectCount++
		observeExecutionResourceBudget("spill_disk", "reject", "query", additional)
		return &ExecutionResourceError{Kind: ExecutionResourceErrorClosed, Requested: additional}
	}
	if b.spillDiskUsed > b.spillDiskCap || additional > b.spillDiskCap-b.spillDiskUsed {
		g.rejectCount++
		observeExecutionResourceBudget("spill_disk", "reject", "cn", additional)
		return newComponentAdmissionError(ExecutionResourceComponentSpillDisk, additional, b.spillDiskUsed, b.spillDiskCap)
	}
	if g.spillDiskUsed > g.spillDiskCap || additional > g.spillDiskCap-g.spillDiskUsed {
		g.rejectCount++
		observeExecutionResourceBudget("spill_disk", "reject", "query", additional)
		return newComponentAdmissionError(ExecutionResourceComponentSpillDisk, additional, g.spillDiskUsed, g.spillDiskCap)
	}
	if r.core.size > math.MaxUint64-additional {
		return &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Requested: additional, Message: "spill disk reservation size overflow"}
	}
	b.spillDiskUsed += additional
	g.spillDiskUsed += additional
	r.core.size += additional
	observeExecutionResourceBudget("spill_disk", "reserve", "query", additional)
	observeExecutionResourceBudget("spill_disk", "reserve", "cn", additional)
	g.reserveCount++
	return nil
}

func (g *ExecutionResourceGeneration) ReserveSpillFD(size uint64) (*ExecutionSpillFDReservation, error) {
	if g == nil || g.budget == nil {
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid}
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
		observeExecutionResourceBudget("spill_fd", "reject", "query", size)
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorClosed, Requested: size}
	}
	b.spillFDCap = clampSpillFDCap(b.spillFDConfiguredCap, processLimit, limitKnown)
	g.spillFDCap = g.spillFDConfiguredCap
	if g.spillFDCap > b.spillFDCap {
		g.spillFDCap = b.spillFDCap
	}
	if b.spillFDUsed > b.spillFDCap || size > b.spillFDCap-b.spillFDUsed {
		g.rejectCount++
		observeExecutionResourceBudget("spill_fd", "reject", "cn", size)
		return nil, newComponentAdmissionError(ExecutionResourceComponentSpillFD, size, b.spillFDUsed, b.spillFDCap)
	}
	if g.spillFDUsed > g.spillFDCap || size > g.spillFDCap-g.spillFDUsed {
		g.rejectCount++
		observeExecutionResourceBudget("spill_fd", "reject", "query", size)
		return nil, newComponentAdmissionError(ExecutionResourceComponentSpillFD, size, g.spillFDUsed, g.spillFDCap)
	}
	b.spillFDUsed += size
	g.spillFDUsed += size
	observeExecutionResourceBudget("spill_fd", "reserve", "query", size)
	observeExecutionResourceBudget("spill_fd", "reserve", "cn", size)
	return &ExecutionSpillFDReservation{budget: b, generation: g, core: &executionSpillReservationCore{size: size}}, nil
}

func (r *ExecutionSpillDiskReservation) Release() bool {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return false
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if !r.core.state.CompareAndSwap(executionSpillReservationActive, executionSpillReservationReleased) {
		return false
	}
	if r.generation.spillDiskUsed < r.core.size ||
		r.budget.spillDiskUsed < r.core.size {
		panic("execution spill disk reservation release underflow")
	}
	r.generation.spillDiskUsed -= r.core.size
	r.budget.spillDiskUsed -= r.core.size
	observeExecutionResourceBudget("spill_disk", "release", "query", r.core.size)
	observeExecutionResourceBudget("spill_disk", "release", "cn", r.core.size)
	r.generation.releaseCount++
	return true
}

func (r *ExecutionSpillFDReservation) Release() bool {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return false
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if !r.core.state.CompareAndSwap(executionSpillReservationActive, executionSpillReservationReleased) {
		return false
	}
	if r.generation.spillFDUsed < r.core.size ||
		r.budget.spillFDUsed < r.core.size {
		panic("execution spill fd reservation release underflow")
	}
	r.generation.spillFDUsed -= r.core.size
	r.budget.spillFDUsed -= r.core.size
	observeExecutionResourceBudget("spill_fd", "release", "query", r.core.size)
	observeExecutionResourceBudget("spill_fd", "release", "cn", r.core.size)
	r.generation.releaseCount++
	return true
}

func (r *ExecutionSpillDiskReservation) ReconcileDown(actual uint64) (bool, error) {
	if r == nil || r.core == nil || r.budget == nil || r.generation == nil {
		return false, ErrExecutionSpillReservationInactive
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if r.core.state.Load() != executionSpillReservationActive {
		return false, ErrExecutionSpillReservationInactive
	}
	if actual > r.core.size {
		return false, ErrExecutionSpillReservationUpward
	}
	delta := r.core.size - actual
	if delta > 0 {
		if r.generation.spillDiskUsed < delta || r.budget.spillDiskUsed < delta {
			return false, ErrExecutionSpillReservationInactive
		}
		r.generation.spillDiskUsed -= delta
		r.budget.spillDiskUsed -= delta
		r.core.size = actual
		observeExecutionResourceBudget("spill_disk", "reconcile", "query", delta)
		observeExecutionResourceBudget("spill_disk", "reconcile", "cn", delta)
	}
	r.generation.reconcileCount++
	return true, nil
}

// ExecutionMemoryCeilingInputs are the finite resource sources used by
// ResolveExecutionMemoryCeiling.  A zero or math.MaxUint64 source means unavailable
// or unlimited and is excluded from the minimum.  No OS probing occurs here;
// callers provide values obtained from their environment.
type ExecutionMemoryCeilingInputs struct {
	CgroupMemoryMax uint64
	HostMemTotal    uint64
	GlobalMpoolCap  uint64
	FileCacheHint   uint64
	// A positive process limitation narrows QueryCap.  Zero means no narrower
	// override (the resolved CN cap remains the query cap).
	ProcessLimitationSize uint64
}

// ExecutionMemoryCeiling is the resolved local-CN and per-generation budget.
type ExecutionMemoryCeiling struct {
	EffectiveCN      uint64
	RequestedReserve uint64
	Reserve          uint64
	CNMemoryCap      uint64
	QueryCap         uint64
}

// ResolveExecutionMemoryCeiling computes the budget ceiling without touching the OS.
// At least one finite, positive source is required, and every resulting cap
// must remain positive (fail closed otherwise).
func ResolveExecutionMemoryCeiling(in ExecutionMemoryCeilingInputs) (ExecutionMemoryCeiling, error) {
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
		return ExecutionMemoryCeiling{}, &ExecutionResourceError{Kind: ExecutionResourceErrorCeilingMissing, Message: ErrExecutionMemoryCeilingMissing.Error()}
	}

	requested := executionResourceMinimumReserve
	if fifth := effective / 5; fifth > requested {
		requested = fifth
	}
	if in.FileCacheHint > requested {
		requested = in.FileCacheHint
	}
	reserve := requested
	// Small, tightly limited CNs still need a bounded execution allowance for
	// bootstrap/internal SQL. Keep at least 5% (and normally 64 MiB) available
	// instead of turning every accounted query into a startup-fatal ceiling
	// error.
	minimumExecutionCap := effective / 20
	if minimumExecutionCap < 64*mpool.MB {
		minimumExecutionCap = 64 * mpool.MB
	}
	if minimumExecutionCap >= effective {
		minimumExecutionCap = effective / 5
	}
	maxReserve := effective - minimumExecutionCap
	if reserve > maxReserve {
		reserve = maxReserve
	}
	cnCap := effective - reserve
	if cnCap == 0 {
		return ExecutionMemoryCeiling{}, &ExecutionResourceError{Kind: ExecutionResourceErrorCeilingMissing, Message: "execution memory ceiling is zero"}
	}
	queryCap := cnCap
	if in.ProcessLimitationSize > 0 && in.ProcessLimitationSize < queryCap {
		queryCap = in.ProcessLimitationSize
	}
	if queryCap == 0 {
		return ExecutionMemoryCeiling{}, &ExecutionResourceError{Kind: ExecutionResourceErrorCeilingMissing, Message: "execution query memory cap is zero"}
	}
	return ExecutionMemoryCeiling{
		EffectiveCN:      effective,
		RequestedReserve: requested,
		Reserve:          reserve,
		CNMemoryCap:      cnCap,
		QueryCap:         queryCap,
	}, nil
}

// GetExecutionResourceBudget returns the statement generation shared by every child
// process in this BaseProcess. Different top-level processes on the same CN
// charge a shared aggregate budget.
func (proc *Process) GetExecutionResourceBudget() (*ExecutionResourceGeneration, error) {
	if proc == nil || proc.Base == nil {
		return nil, &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Message: "nil process for execution resource budget"}
	}
	proc.Base.executionResourceBudgetMu.Lock()
	defer proc.Base.executionResourceBudgetMu.Unlock()
	if proc.Base.executionResourceBudget != nil {
		return proc.Base.executionResourceBudget, nil
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
	initialInputs := executionResourceProcessMemoryInputs
	initialInputs.GlobalMpoolCap = globalCap
	initialInputs.FileCacheHint = fileCacheHint
	initialInputs.ProcessLimitationSize = queryLimit
	ceiling, err := ResolveExecutionMemoryCeiling(initialInputs)
	if err != nil {
		return nil, err
	}

	var aggregate *ExecutionResourceBudget
	service := proc.GetService()
	if service == "" {
		service = "__process_local_cn__"
	}
	value, loaded := executionResourceCNBudgets.Load(service)
	if !loaded {
		candidate := func() *ExecutionResourceBudget {
			b, createErr := NewExecutionResourceBudget(ceiling.CNMemoryCap, ceiling.CNMemoryCap)
			if createErr != nil {
				return nil
			}
			// Attach the source snapshot before publishing the candidate so
			// another process never observes an aggregate without its stable
			// provider.
			b.installCNCapProvider(initialInputs)
			return b
		}()
		value, loaded = executionResourceCNBudgets.LoadOrStore(service, candidate)
	}
	aggregate, _ = value.(*ExecutionResourceBudget)
	if aggregate == nil {
		err = &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Message: "failed to initialize CN execution resource budget"}
	}
	if err != nil {
		return nil, err
	}
	if loaded {
		aggregate.mergeObservedCNCap(initialInputs, ceiling.CNMemoryCap)
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
		executionResourceGenerationSequence.Add(1),
		ceiling.QueryCap,
		spillDiskCap,
	)
	if err != nil {
		return nil, err
	}
	proc.Base.executionResourceBudget = generation
	return generation, nil
}
