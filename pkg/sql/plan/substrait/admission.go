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

package substrait

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"mime"
	"net/http"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	ResolvePath             = "/internal/v1/sidecar/read/resolve"
	MaxLeaseTTL             = 20 * time.Minute
	MaxManifestBytes        = 64 << 20
	rollbackCleanupTimeout  = 30 * time.Second
	resolveAuditTimeout     = 5 * time.Second
	resolveAuthorityTimeout = 5 * time.Second
	resolveBudgetTimeout    = 5 * time.Second
	maxManifestSize         = MaxManifestBytes
	maxCanonicalSchemaSize  = 1 << 20
	maxResolveResponseSize  = MaxManifestBytes + maxCanonicalSchemaSize + maxTaeReadSize + 64
	// The bounded body, decoded request fields, and decoded TaeRead coexist until
	// validation completes. Reserve all of them before the first request read.
	maxResolveRequestRetainedBytes = 2*(maxResolveRequestSize+1) + maxTaeReadSize
	// Two maximum-size resolutions may run concurrently.
	maxResolveHandlerBytes  = maxResolveRequestRetainedBytes + maxResolveResponseSize
	maxResolveInFlightBytes = 2 * maxResolveHandlerBytes
)

var errStopReplayJournalScan = moerr.NewInternalErrorNoCtx("substrait: stop replay journal scan")

type replayTerminal struct {
	readRef  []byte
	released bool
}

// SnapshotFacts are produced by a snapshot-bound TAE relation lookup. A
// provider must set the rejection flags conservatively if it cannot prove a
// property. Manifest must be deterministic for the supplied snapshot.
type SnapshotFacts struct {
	Manifest                                                  []byte
	CanonicalSchema                                           []byte
	ObjectNames                                               []string
	CommittedInMemory, Uncommitted, VisibleTombstones, NonTAE bool
}

type SnapshotProvider interface {
	PrepareSnapshotRead(context.Context, Read, []byte) (SnapshotFacts, error)
}

// Protector is the narrow GC-protection seam. Begin must fail if GC is already
// running and must prevent GC from starting until the returned close function
// is called. Register and rollback are valid only within that scope; rollback
// removes only a registration created by that session, and close publishes
// every remaining registration. Unregister is the terminal release path for a
// published lease.
type Protector interface {
	Begin(context.Context) (register func(context.Context, []byte, []string, time.Time) error, rollback func(context.Context, []byte) error, close func(), err error)
	Unregister(context.Context, []byte) error
}

// LeaseJournal is the durable boundary for resolver authority.
// StoreIfCapacity must atomically check the namespace-wide live count and
// publish the complete batch with respect to every other StoreIfCapacity call
// on the same journal. It returns the prefix that may have become durable when
// publication fails. Active's final operation must read the single durable
// authority state, so a concurrent resolve either precedes MarkReleased's
// atomic revocation or observes it. MarkReleased must prevent resolution and
// replay before GC protection is removed. Load must visit records one at a
// time and must not retain a record after visit returns.
type LeaseJournal interface {
	StoreIfCapacity(context.Context, []*Lease, int) (int, error)
	Active(context.Context, *Lease) (bool, error)
	MarkReleased(context.Context, []byte) error
	Delete(context.Context, []byte) error
	Load(context.Context, func(*Lease) error) error
}

type Lease struct {
	Read                            *TaeRead
	Wire, Manifest, CanonicalSchema []byte
	AuthorizedClientSPKIHash        []byte
	ObjectNames                     []string
	Released                        bool
}

type releasePhase uint8

const (
	releaseNone releasePhase = iota
	releaseRevoking
	releaseMarked
	releaseUnprotected
)

// LeaseManager owns the single bounded lifetime from admission through
// terminal release. It never evicts a live lease to make room.
type LeaseManager struct {
	// mutation serializes lease lifecycle changes without blocking Resolve on
	// storage or protector I/O. mu protects only the published resolver state.
	mutation contextMutex
	mu       sync.RWMutex
	leases   map[string]*Lease
	releases map[string]releasePhase

	protector     Protector
	journal       LeaseJournal
	resolveBytes  *resolveByteBudget
	maximum       int
	now           func() time.Time
	ready         bool
	benchmarkNoGC bool
}

func NewLeaseManager(maximum int, protector Protector) *LeaseManager {
	return NewPersistentLeaseManager(maximum, protector, nil)
}

// NewBenchmarkLeaseManager creates the process-local lease authority used by
// the explicitly verified local-CN benchmark profile. It is intentionally
// separate from NewLeaseManager so non-durable managers cannot accidentally
// satisfy the Sirius runtime's benchmark admission check.
func NewBenchmarkLeaseManager(maximum int, protector Protector) *LeaseManager {
	manager := NewPersistentLeaseManager(maximum, protector, nil)
	manager.benchmarkNoGC = true
	return manager
}

func NewPersistentLeaseManager(maximum int, protector Protector, journal LeaseJournal) *LeaseManager {
	if maximum <= 0 {
		maximum = 1
	}
	return &LeaseManager{
		leases: make(map[string]*Lease), releases: make(map[string]releasePhase),
		protector: protector, journal: journal, resolveBytes: newResolveByteBudget(maxResolveInFlightBytes), mutation: newContextMutex(), maximum: maximum, now: time.Now,
		ready: journal == nil,
	}
}

type contextMutex struct{ token chan struct{} }

func newContextMutex() contextMutex {
	token := make(chan struct{}, 1)
	token <- struct{}{}
	return contextMutex{token: token}
}

func (m contextMutex) lock(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	select {
	case <-m.token:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (m contextMutex) unlock() { m.token <- struct{}{} }

type resolveByteBudget struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	changed  chan struct{}
}

func newResolveByteBudget(capacity int64) *resolveByteBudget {
	if capacity <= 0 {
		capacity = 1
	}
	return &resolveByteBudget{capacity: capacity, changed: make(chan struct{})}
}

func (b *resolveByteBudget) acquire(ctx context.Context, bytes int64) (func(), error) {
	if b == nil || bytes <= 0 {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid resolve byte reservation")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}
	for {
		b.mu.Lock()
		if bytes > b.capacity {
			b.mu.Unlock()
			return nil, moerr.NewInternalErrorNoCtx("substrait: resolve response exceeds byte budget")
		}
		if bytes <= b.capacity-b.used {
			b.used += bytes
			b.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					b.mu.Lock()
					b.used -= bytes
					close(b.changed)
					b.changed = make(chan struct{})
					b.mu.Unlock()
				})
			}, nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
}

func (m *LeaseManager) Acquire(ctx context.Context, leases []*Lease) error {
	return m.acquirePrepared(ctx, func() ([]*Lease, error) { return leases, nil })
}

// acquirePrepared holds GC exclusion while prepare enumerates the snapshot and
// until every durable lease has matching GC protection.
func (m *LeaseManager) acquirePrepared(ctx context.Context, prepare func() ([]*Lease, error)) error {
	if prepare == nil {
		return moerr.NewInternalErrorNoCtx("substrait: missing lease preparation")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := m.mutation.lock(ctx); err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: acquire lease mutation: %v", err)
	}
	defer m.mutation.unlock()
	m.mu.RLock()
	ready, protector := m.ready, m.protector
	m.mu.RUnlock()
	if !ready {
		return moerr.NewInternalErrorNoCtx("substrait: durable read leases have not been replayed")
	}
	if protector == nil {
		return moerr.NewInternalErrorNoCtx("substrait: read lease GC protection is not configured")
	}
	register, rollback, closeProtection, err := protector.Begin(ctx)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: begin read lease protection: %v", err)
	}
	if register == nil || rollback == nil || closeProtection == nil {
		if closeProtection != nil {
			closeProtection()
		}
		return moerr.NewInternalErrorNoCtx("substrait: invalid read lease protection session")
	}
	defer closeProtection()
	leases, err := prepare()
	if err != nil {
		return err
	}
	return m.acquireProtected(ctx, register, rollback, leases)
}

func (m *LeaseManager) acquireProtected(
	ctx context.Context,
	register func(context.Context, []byte, []string, time.Time) error,
	rollback func(context.Context, []byte) error,
	leases []*Lease,
) error {
	if len(leases) == 0 {
		return moerr.NewInternalErrorNoCtx("substrait: empty lease acquisition")
	}
	if err := m.reconcileRevokedForCapacity(ctx, len(leases)); err != nil {
		return err
	}
	m.mu.RLock()
	if !m.ready {
		m.mu.RUnlock()
		return moerr.NewInternalErrorNoCtx("substrait: durable read leases have not been replayed")
	}
	if m.protector == nil || register == nil || rollback == nil {
		m.mu.RUnlock()
		return moerr.NewInternalErrorNoCtx("substrait: read lease GC protection is not configured")
	}
	if len(m.leases)+len(leases) > m.maximum {
		m.mu.RUnlock()
		return moerr.NewInternalErrorNoCtx("substrait: read lease capacity reached")
	}
	seen := make(map[string]struct{}, len(leases))
	now := uint64(m.now().UnixMilli())
	for _, l := range leases {
		key := ""
		if l != nil && l.Read != nil {
			key = string(l.Read.ReadRef)
		}
		_, duplicate := seen[key]
		if validateLease(l, now, false) != nil || m.leases[key] != nil || duplicate {
			m.mu.RUnlock()
			return moerr.NewInternalErrorNoCtx("substrait: invalid or duplicate read lease")
		}
		seen[key] = struct{}{}
	}
	m.mu.RUnlock()
	stored := leases
	if m.journal != nil {
		storedCount, err := m.journal.StoreIfCapacity(ctx, leases, m.maximum)
		if storedCount < 0 || storedCount > len(leases) {
			rollbackErr := m.rollbackAcquisition(ctx, rollback, leases, nil)
			return errors.Join(moerr.NewInternalErrorNoCtx("substrait: lease journal returned an invalid durable prefix"), rollbackErr)
		}
		stored = leases[:storedCount]
		if err != nil {
			rollbackErr := m.rollbackAcquisition(ctx, rollback, stored, nil)
			return errors.Join(moerr.NewInternalErrorNoCtxf("substrait: persist read lease: %v", err), rollbackErr)
		}
		if storedCount != len(leases) {
			rollbackErr := m.rollbackAcquisition(ctx, rollback, stored, nil)
			return errors.Join(moerr.NewInternalErrorNoCtx("substrait: lease journal stored an incomplete batch"), rollbackErr)
		}
	}
	registered := make([]*Lease, 0, len(leases))
	for _, l := range leases {
		err := register(ctx, l.Read.ReadRef, l.ObjectNames, time.UnixMilli(int64(l.Read.ExpiresAtUnixMS)))
		if err != nil {
			rollbackErr := m.rollbackAcquisition(ctx, rollback, stored, registered)
			return errors.Join(moerr.NewInternalErrorNoCtxf("substrait: protect read lease: %v", err), rollbackErr)
		}
		registered = append(registered, l)
	}
	m.mu.Lock()
	for _, l := range leases {
		m.leases[string(l.Read.ReadRef)] = cloneLease(l)
	}
	m.mu.Unlock()
	return nil
}

// rollbackAcquisition first makes every possibly stored lease non-replayable,
// removes journal debris, then removes protection only for durably revoked
// leases. A caller cancellation must not suppress this crash-safety cleanup.
func (m *LeaseManager) rollbackAcquisition(
	ctx context.Context,
	rollback func(context.Context, []byte) error,
	stored, registered []*Lease,
) error {
	cleanupCtx, cancel := context.WithTimeoutCause(
		context.WithoutCancel(ctx),
		rollbackCleanupTimeout,
		moerr.NewInternalErrorNoCtx("substrait: read lease rollback timed out"),
	)
	defer cancel()
	var result error
	revoked := make(map[string]bool, len(stored))
	if m.journal != nil {
		for i := len(stored) - 1; i >= 0; i-- {
			readRef := stored[i].Read.ReadRef
			err := m.journal.MarkReleased(cleanupCtx, readRef)
			result = errors.Join(result, err)
			if err == nil {
				revoked[string(readRef)] = true
			}
		}
		for i := len(stored) - 1; i >= 0; i-- {
			readRef := stored[i].Read.ReadRef
			err := m.journal.Delete(cleanupCtx, readRef)
			result = errors.Join(result, err)
			if err == nil {
				revoked[string(readRef)] = true
			}
		}
	} else {
		for _, lease := range stored {
			revoked[string(lease.Read.ReadRef)] = true
		}
	}
	for i := len(registered) - 1; i >= 0; i-- {
		readRef := registered[i].Read.ReadRef
		if revoked[string(readRef)] {
			result = errors.Join(result, rollback(cleanupCtx, readRef))
		}
	}
	return result
}

func (m *LeaseManager) resolve(ctx context.Context, readRef []byte) (*Lease, bool, error) {
	m.mu.RLock()
	if !m.ready {
		m.mu.RUnlock()
		return nil, false, nil
	}
	key := string(readRef)
	l := m.leases[key]
	if l != nil && (l.Released || m.releases[key] != releaseNone || l.Read.ExpiresAtUnixMS <= uint64(m.now().UnixMilli())) {
		l = nil
	}
	journal := m.journal
	m.mu.RUnlock()
	// Publication transfers a private clone into the manager and no lifecycle
	// path mutates it. Internal resolution can therefore borrow the immutable
	// view without another manifest-sized allocation.
	result := l
	if result == nil {
		return nil, false, nil
	}
	if journal != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		authorityCtx, cancel := context.WithTimeoutCause(
			ctx,
			resolveAuthorityTimeout,
			moerr.NewInternalErrorNoCtx("substrait: durable read lease validation timed out"),
		)
		active, err := journal.Active(authorityCtx, result)
		cancel()
		if err != nil {
			return nil, false, moerr.NewInternalErrorNoCtxf("substrait: validate durable read lease: %v", err)
		}
		if !active {
			// Another manager durably revoked this authority. Use the normal
			// release owner so this manager also drops its GC pin and retains
			// retry state if cleanup fails.
			if err := m.Release(ctx, readRef); err != nil {
				return nil, false, err
			}
			return nil, false, nil
		}
	}
	return result, true, nil
}

// Release is the terminal execution-owner transition. The caller must first
// stop and join every sidecar reader that received this reference; capability
// expiry alone is not evidence that those readers have stopped.
func (m *LeaseManager) Release(ctx context.Context, readRef []byte) error {
	cleanupCtx, cancel := leaseCleanupContext(ctx)
	defer cancel()
	if err := m.mutation.lock(cleanupCtx); err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: acquire release mutation: %v", err)
	}
	defer m.mutation.unlock()
	m.mu.RLock()
	if !m.ready {
		m.mu.RUnlock()
		return moerr.NewInternalErrorNoCtx("substrait: durable read leases have not been replayed")
	}
	key := string(readRef)
	l := m.leases[key]
	m.mu.RUnlock()
	if l == nil {
		return nil
	}
	if err := m.releaseLease(cleanupCtx, key); err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: release read lease: %v", err)
	}
	return nil
}

func leaseCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(rollbackCleanupTimeout)
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	return context.WithDeadlineCause(
		context.WithoutCancel(ctx), deadline,
		moerr.NewInternalErrorNoCtx("substrait: read lease cleanup timed out"),
	)
}

// reconcileRevokedForCapacity is the admission-pressure slow path. Managers
// may share a durable journal during rolling replacement, so a lease revoked
// by another manager can remain in this manager's immutable local map. Before
// rejecting for capacity, consult the durable authority and advance stale
// local leases through the ordinary release owner. mutation must be held by
// the caller; the scan is bounded by the configured live-lease capacity.
func (m *LeaseManager) reconcileRevokedForCapacity(ctx context.Context, incoming int) error {
	if m.journal == nil {
		return nil
	}
	m.mu.RLock()
	if len(m.leases)+incoming <= m.maximum {
		m.mu.RUnlock()
		return nil
	}
	leases := make([]*Lease, 0, len(m.leases))
	for _, lease := range m.leases {
		leases = append(leases, lease)
	}
	m.mu.RUnlock()

	for _, lease := range leases {
		m.mu.RLock()
		hasCapacity := len(m.leases)+incoming <= m.maximum
		m.mu.RUnlock()
		if hasCapacity {
			return nil
		}
		active, err := m.journal.Active(ctx, lease)
		if err != nil {
			return moerr.NewInternalErrorNoCtxf("substrait: reconcile durable read lease: %v", err)
		}
		if active {
			continue
		}
		if err := m.releaseLease(ctx, string(lease.Read.ReadRef)); err != nil {
			return moerr.NewInternalErrorNoCtxf("substrait: release revoked read lease: %v", err)
		}
	}
	return nil
}

// releaseLease advances a retryable three-phase revocation. Resolver state is
// hidden before I/O, durable revocation precedes unprotection, and journal
// deletion is last. mutation must be held by the caller.
func (m *LeaseManager) releaseLease(ctx context.Context, key string) error {
	m.mu.Lock()
	l := m.leases[key]
	if l == nil {
		delete(m.releases, key)
		m.mu.Unlock()
		return nil
	}
	phase := m.releases[key]
	if phase == releaseNone {
		phase = releaseRevoking
		m.releases[key] = phase
	}
	readRef := append([]byte(nil), l.Read.ReadRef...)
	m.mu.Unlock()

	if phase == releaseRevoking {
		if m.journal != nil {
			if err := m.journal.MarkReleased(ctx, readRef); err != nil {
				// The delete may have become durable before its result was lost.
				// Keep the lease hidden in the retryable revoking phase; reopening
				// it could discard the only owner of the GC pin.
				return err
			}
		}
		m.mu.Lock()
		if m.leases[key] != nil {
			m.releases[key] = releaseMarked
		}
		m.mu.Unlock()
		phase = releaseMarked
	}
	if phase == releaseMarked {
		if m.protector != nil {
			if err := m.protector.Unregister(ctx, readRef); err != nil {
				return err
			}
		}
		m.mu.Lock()
		if m.leases[key] != nil {
			m.releases[key] = releaseUnprotected
		}
		m.mu.Unlock()
		phase = releaseUnprotected
	}
	if phase == releaseUnprotected && m.journal != nil {
		if err := m.journal.Delete(ctx, readRef); err != nil {
			return err
		}
	}
	m.mu.Lock()
	delete(m.leases, key)
	delete(m.releases, key)
	m.mu.Unlock()
	return nil
}

// Replay restores durable leases and their GC protection. Call it before the
// resolver becomes reachable or GC starts.
func (m *LeaseManager) Replay(ctx context.Context) error {
	if m.journal == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	mutationCtx, cancelMutation := leaseCleanupContext(ctx)
	defer cancelMutation()
	if err := m.mutation.lock(mutationCtx); err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: acquire replay mutation: %v", err)
	}
	defer m.mutation.unlock()
	m.mu.RLock()
	if m.ready || len(m.leases) != 0 {
		m.mu.RUnlock()
		return moerr.NewInternalErrorNoCtx("substrait: cannot replay into a live lease manager")
	}
	m.mu.RUnlock()
	var register func(context.Context, []byte, []string, time.Time) error
	var rollback func(context.Context, []byte) error
	var closeProtection func()
	var err error
	if m.protector != nil {
		register, rollback, closeProtection, err = m.protector.Begin(ctx)
		if err != nil {
			return moerr.NewInternalErrorNoCtxf("substrait: begin replay read lease protection: %v", err)
		}
		if register == nil || rollback == nil || closeProtection == nil {
			if closeProtection != nil {
				closeProtection()
			}
			return moerr.NewInternalErrorNoCtx("substrait: invalid replay read lease protection session")
		}
		defer closeProtection()
	}
	now := uint64(m.now().UnixMilli())
	// Load streams one record at a time. Retain only a fixed batch of lightweight
	// terminal references and clean it after List has unwound; some FileService
	// implementations hold a read lock while yielding. liveByKey is bounded by
	// the configured capacity.
	liveByKey := make(map[string]*Lease)
	for {
		terminals := make([]replayTerminal, 0, journalCleanupBatchSize)
		var replayErr error
		err = m.journal.Load(ctx, func(l *Lease) error {
			if err := validateLease(l, now, true); err != nil {
				replayErr = moerr.NewInternalErrorNoCtxf("substrait: invalid durable read lease: %v", err)
				return replayErr
			}
			if l.Released {
				terminals = append(terminals, replayTerminal{
					readRef:  append([]byte(nil), l.Read.ReadRef...),
					released: l.Released,
				})
				if len(terminals) == journalCleanupBatchSize {
					return errStopReplayJournalScan
				}
				return nil
			}
			key := string(l.Read.ReadRef)
			if liveByKey[key] != nil {
				return nil
			}
			if len(liveByKey) == m.maximum {
				replayErr = moerr.NewInternalErrorNoCtx("substrait: durable read leases exceed capacity")
				return replayErr
			}
			liveByKey[key] = l
			return nil
		})
		if replayErr != nil {
			return replayErr
		}
		if err != nil && !errors.Is(err, errStopReplayJournalScan) {
			return moerr.NewInternalErrorNoCtxf("substrait: load read leases: %v", err)
		}
		if len(terminals) == 0 {
			break
		}
		for _, terminal := range terminals {
			if err := m.cleanReplayLease(ctx, terminal.readRef, terminal.released); err != nil {
				return moerr.NewInternalErrorNoCtxf("substrait: clean durable read lease: %v", err)
			}
		}
	}
	live := make([]*Lease, 0, len(liveByKey))
	for _, l := range liveByKey {
		live = append(live, l)
	}
	registered := make([]*Lease, 0, len(live))
	for _, l := range live {
		if register != nil {
			if err := register(ctx, l.Read.ReadRef, l.ObjectNames, time.UnixMilli(int64(l.Read.ExpiresAtUnixMS))); err != nil {
				rollbackErr := m.rollbackReplayProtections(ctx, rollback, registered)
				return errors.Join(moerr.NewInternalErrorNoCtxf("substrait: replay read lease protection: %v", err), rollbackErr)
			}
			registered = append(registered, l)
		}
	}
	m.mu.Lock()
	for _, l := range live {
		m.leases[string(l.Read.ReadRef)] = cloneLease(l)
	}
	m.ready = true
	m.mu.Unlock()
	return nil
}

// cleanReplayLease removes a terminal durable record that was never published
// in this manager. Replay holds mutation, but never the resolver map lock.
func (m *LeaseManager) cleanReplayLease(ctx context.Context, readRef []byte, released bool) error {
	if !released {
		if err := m.journal.MarkReleased(ctx, readRef); err != nil {
			return err
		}
	}
	if m.protector != nil {
		if err := m.protector.Unregister(ctx, readRef); err != nil {
			return err
		}
	}
	return m.journal.Delete(ctx, readRef)
}

func (m *LeaseManager) rollbackReplayProtections(
	ctx context.Context,
	rollback func(context.Context, []byte) error,
	registered []*Lease,
) error {
	cleanupCtx, cancel := leaseCleanupContext(ctx)
	defer cancel()
	var result error
	for i := len(registered) - 1; i >= 0; i-- {
		result = errors.Join(result, rollback(cleanupCtx, registered[i].Read.ReadRef))
	}
	return result
}

func (m *LeaseManager) Ready() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	ready := m.ready
	m.mu.RUnlock()
	return ready
}

func (m *LeaseManager) Protected() bool {
	return m != nil && m.protector != nil
}

// DurableReady reports whether this manager is suitable for a reachable CN
// runtime: authority is journaled, replay is complete, and GC protection is
// installed. NewLeaseManager intentionally does not satisfy this predicate.
func (m *LeaseManager) DurableReady() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	ready := m.ready && m.journal != nil && m.protector != nil
	m.mu.RUnlock()
	return ready
}

// BenchmarkReady reports whether this manager was explicitly constructed for
// the no-GC benchmark profile. It never reports true for a normal
// non-durable manager, even when that manager happens to be ready.
func (m *LeaseManager) BenchmarkReady() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	ready := m.benchmarkNoGC && m.ready && m.journal == nil && m.protector != nil
	m.mu.RUnlock()
	return ready
}

func (m *LeaseManager) currentTime() time.Time {
	m.mu.RLock()
	now := m.now
	m.mu.RUnlock()
	if now == nil {
		return time.Now()
	}
	return now()
}

// PendingExecution groups live read references by the statement that owns
// their terminal sidecar cleanup. The identity is sufficient to
// reconstruct Flight's cancellation key after a CN restart.
type PendingExecution struct {
	AccountID uint64
	QueryID   []byte
	ReadRefs  [][]byte
}

// PendingExecutions returns immutable copies of every live execution group.
// Released records are cleaned during Replay and are never republished here.
func (m *LeaseManager) PendingExecutions() []PendingExecution {
	if m == nil {
		return nil
	}
	type executionKey struct {
		accountID uint64
		queryID   string
	}
	m.mu.RLock()
	if !m.ready {
		m.mu.RUnlock()
		return nil
	}
	grouped := make(map[executionKey]*PendingExecution)
	for _, lease := range m.leases {
		if lease == nil || lease.Read == nil || lease.Released {
			continue
		}
		groupKey := executionKey{accountID: lease.Read.AccountID, queryID: string(lease.Read.QueryID)}
		pending := grouped[groupKey]
		if pending == nil {
			pending = &PendingExecution{
				AccountID: lease.Read.AccountID,
				QueryID:   append([]byte(nil), lease.Read.QueryID...),
			}
			grouped[groupKey] = pending
		}
		pending.ReadRefs = append(pending.ReadRefs, append([]byte(nil), lease.Read.ReadRef...))
	}
	m.mu.RUnlock()
	result := make([]PendingExecution, 0, len(grouped))
	for _, pending := range grouped {
		result = append(result, *pending)
	}
	return result
}

func validateLease(l *Lease, now uint64, allowReleased bool) error {
	if l == nil || l.Read == nil || (!allowReleased && l.Released) {
		return moerr.NewInternalErrorNoCtx("missing or released lease")
	}
	validationNow := now
	if allowReleased {
		validationNow = 0
	}
	if err := l.Read.Validate(validationNow); err != nil {
		return err
	}
	if len(l.Wire) == 0 || len(l.Manifest) == 0 || len(l.Manifest) > maxManifestSize || len(l.CanonicalSchema) == 0 || len(l.CanonicalSchema) > maxCanonicalSchemaSize || len(l.AuthorizedClientSPKIHash) != sha256.Size {
		return moerr.NewInternalErrorNoCtx("invalid lease payload size")
	}
	decoded, err := UnmarshalTaeRead(l.Wire, validationNow)
	if err != nil || !equalBytes(decoded.ReadRef, l.Read.ReadRef) {
		return moerr.NewInternalErrorNoCtx("lease wire identity mismatch")
	}
	canonical, err := MarshalTaeRead(l.Read)
	if err != nil || !equalBytes(canonical, l.Wire) {
		return moerr.NewInternalErrorNoCtx("non-canonical lease wire")
	}
	schemaHash := sha256.Sum256(l.CanonicalSchema)
	manifestHash := sha256.Sum256(l.Manifest)
	if !equalBytes(schemaHash[:], l.Read.SchemaDigest) || !equalBytes(manifestHash[:], l.Read.ManifestSHA256) {
		return moerr.NewInternalErrorNoCtx("lease payload digest mismatch")
	}
	seen := make(map[string]struct{}, len(l.ObjectNames))
	for _, name := range l.ObjectNames {
		if name == "" {
			return moerr.NewInternalErrorNoCtx("empty protected object name")
		}
		if _, ok := seen[name]; ok {
			return moerr.NewInternalErrorNoCtx("duplicate protected object name")
		}
		seen[name] = struct{}{}
	}
	return nil
}

func cloneLease(l *Lease) *Lease {
	if l == nil {
		return nil
	}
	c := *l
	c.Read = cloneTaeRead(l.Read)
	c.Wire = append([]byte(nil), l.Wire...)
	c.Manifest = append([]byte(nil), l.Manifest...)
	c.CanonicalSchema = append([]byte(nil), l.CanonicalSchema...)
	c.AuthorizedClientSPKIHash = append([]byte(nil), l.AuthorizedClientSPKIHash...)
	c.ObjectNames = append([]string(nil), l.ObjectNames...)
	return &c
}

func cloneTaeRead(r *TaeRead) *TaeRead {
	if r == nil {
		return nil
	}
	c := *r
	c.ReadRef = append([]byte(nil), r.ReadRef...)
	c.QueryID = append([]byte(nil), r.QueryID...)
	c.SnapshotTS = append([]byte(nil), r.SnapshotTS...)
	c.SchemaDigest = append([]byte(nil), r.SchemaDigest...)
	c.ManifestSHA256 = append([]byte(nil), r.ManifestSHA256...)
	c.CapabilityHash = append([]byte(nil), r.CapabilityHash...)
	return &c
}

type AdmissionRequest struct {
	Candidate                *Candidate
	Provider                 SnapshotProvider
	Leases                   *LeaseManager
	AccountID                uint64
	QueryID, SnapshotTS      []byte
	AuthorizedClientSPKIHash []byte
	TTL                      time.Duration
	// ReadOnly and PriorWrites are transaction facts captured at the compile
	// cutpoint. They are explicit to prevent accidental admission after writes.
	ReadOnly    bool
	PriorWrites bool
	Random      io.Reader
	Now         time.Time
}

// AdmittedReads is the atomic output of snapshot admission. ReadRefs and
// ExpiresAt are returned from the admission boundary itself so downstream
// ownership never depends on re-decoding capability wires.
type AdmittedReads struct {
	Wires     map[int32][]byte
	ReadRefs  [][]byte
	ExpiresAt time.Time
}

// Admit performs storage work only after Export has accepted the complete
// logical plan. It publishes all table leases atomically or none of them.
func Admit(ctx context.Context, r AdmissionRequest) (map[int32][]byte, error) {
	admitted, err := AdmitReads(ctx, r)
	if err != nil {
		return nil, err
	}
	return admitted.Wires, nil
}

// AdmitReads performs storage work only after Export has accepted the complete
// logical plan. It publishes all table leases atomically or none of them and
// returns the immutable cleanup/deadline metadata for the new owner.
func AdmitReads(ctx context.Context, r AdmissionRequest) (*AdmittedReads, error) {
	if r.Candidate == nil || r.Provider == nil || r.Leases == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: incomplete admission request")
	}
	if !r.ReadOnly || r.PriorWrites {
		return nil, NotEligible(EligibilityTransaction, "transaction is not an admissible read-only snapshot")
	}
	if len(r.QueryID) == 0 || len(r.SnapshotTS) != 12 || len(r.AuthorizedClientSPKIHash) != sha256.Size {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid admission identity")
	}
	if r.TTL <= 0 || r.TTL > MaxLeaseTTL {
		return nil, moerr.NewInternalErrorNoCtx("substrait: lease TTL is outside the supported bound")
	}
	if r.Random == nil {
		r.Random = rand.Reader
	}
	type preparedRead struct {
		read  Read
		facts SnapshotFacts
	}
	result := &AdmittedReads{}
	err := r.Leases.acquirePrepared(ctx, func() ([]*Lease, error) {
		reads := r.Candidate.Reads()
		prepared := make([]preparedRead, 0, len(reads))
		for _, read := range reads {
			read.AccountID = r.AccountID
			facts, err := r.Provider.PrepareSnapshotRead(ctx, read, r.SnapshotTS)
			if err != nil {
				if IsNotEligible(err) {
					return nil, err
				}
				return nil, moerr.NewInternalErrorNoCtxf("substrait: prepare table %d: %v", read.TableID, err)
			}
			if facts.CommittedInMemory || facts.Uncommitted || facts.VisibleTombstones || facts.NonTAE {
				return nil, notEligiblef(EligibilitySnapshot, "table %d has snapshot state unsupported by Sirius v1", read.TableID)
			}
			if len(facts.Manifest) > maxManifestSize || len(facts.CanonicalSchema) > maxCanonicalSchemaSize {
				return nil, notEligiblef(EligibilitySnapshot, "table %d snapshot metadata exceeds the Sirius v1 size bound", read.TableID)
			}
			if len(facts.Manifest) == 0 || !equalBytes(facts.CanonicalSchema, read.Schema) {
				return nil, moerr.NewInternalErrorNoCtxf("substrait: table %d schema or manifest mismatch", read.TableID)
			}
			prepared = append(prepared, preparedRead{read: read, facts: facts})
		}
		now := r.Now
		if now.IsZero() {
			now = r.Leases.currentTime()
		}
		expires := now.Add(r.TTL).UnixMilli()
		if expires <= now.UnixMilli() || expires <= 0 {
			return nil, moerr.NewInternalErrorNoCtx("substrait: invalid lease expiry")
		}
		leases := make([]*Lease, 0, len(prepared))
		result.Wires = make(map[int32][]byte, len(prepared))
		result.ReadRefs = make([][]byte, 0, len(prepared))
		result.ExpiresAt = time.UnixMilli(expires)
		for _, item := range prepared {
			ref := make([]byte, 32)
			if _, err := io.ReadFull(r.Random, ref); err != nil {
				return nil, moerr.NewInternalErrorNoCtxf("substrait: create read reference: %v", err)
			}
			schemaHash := sha256.Sum256(item.facts.CanonicalSchema)
			manifestHash := sha256.Sum256(item.facts.Manifest)
			tr := &TaeRead{ProtocolVersion: TaeReadProtocolVersion, ReadRef: ref, QueryID: append([]byte(nil), r.QueryID...), AccountID: r.AccountID, DatabaseID: item.read.DatabaseID, TableID: item.read.TableID, SnapshotTS: append([]byte(nil), r.SnapshotTS...), SchemaDigest: schemaHash[:], ManifestSHA256: manifestHash[:], CapabilityHash: CapabilityHash[:], ExpiresAtUnixMS: uint64(expires)}
			wire, err := MarshalTaeRead(tr)
			if err != nil {
				return nil, err
			}
			leases = append(leases, &Lease{Read: tr, Wire: wire, Manifest: item.facts.Manifest, CanonicalSchema: item.facts.CanonicalSchema, AuthorizedClientSPKIHash: append([]byte(nil), r.AuthorizedClientSPKIHash...), ObjectNames: item.facts.ObjectNames})
			result.Wires[item.read.NodeID] = wire
			result.ReadRefs = append(result.ReadRefs, append([]byte(nil), ref...))
		}
		return leases, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ResolveAuditEvent is emitted exactly once before a successful manifest
// resolution. ReadRefSHA256 identifies the capability without logging it.
type ResolveAuditEvent struct {
	AccountID, DatabaseID, TableID uint64
	QueryID                        []byte
	ClientSPKIHash, ReadRefSHA256  []byte
}

type ResolveAuditRecorder interface {
	// RecordResolve must honor ctx. The resolver supplies a bounded deadline
	// and fails closed rather than returning an unaudited manifest.
	RecordResolve(context.Context, ResolveAuditEvent) error
}

type ResolveAuditFunc func(context.Context, ResolveAuditEvent) error

func (f ResolveAuditFunc) RecordResolve(ctx context.Context, event ResolveAuditEvent) error {
	if f == nil {
		return moerr.NewInternalErrorNoCtx("substrait: nil resolution audit function")
	}
	return f(ctx, event)
}

// ResolveHandler exposes the exact strict, mTLS-only Sirius resolver route.
// An audit recorder is mandatory so no successful resolution is unaudited.
func ResolveHandler(leases *LeaseManager, now func() time.Time, auditor ResolveAuditRecorder) http.Handler {
	if now == nil {
		now = time.Now
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != ResolvePath {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaErr != nil || mediaType != "application/x-protobuf" {
			http.Error(w, "protobuf content type required", http.StatusUnsupportedMediaType)
			return
		}
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 || len(r.TLS.VerifiedChains[0][0].RawSubjectPublicKeyInfo) == 0 {
			http.Error(w, "verified client certificate required", http.StatusUnauthorized)
			return
		}
		principalHash := sha256.Sum256(r.TLS.VerifiedChains[0][0].RawSubjectPublicKeyInfo)
		if leases == nil || auditor == nil {
			http.Error(w, "resolver unavailable", http.StatusServiceUnavailable)
			return
		}
		budgetCtx, cancelBudget := context.WithTimeoutCause(
			r.Context(),
			resolveBudgetTimeout,
			moerr.NewInternalErrorNoCtx("substrait: resolve byte admission timed out"),
		)
		releaseBytes, err := leases.resolveBytes.acquire(budgetCtx, int64(maxResolveHandlerBytes))
		cancelBudget()
		if err != nil {
			http.Error(w, "resolver busy", http.StatusServiceUnavailable)
			return
		}
		defer releaseBytes()
		body, err := readResolveRequestBody(w, r)
		if err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		req, err := UnmarshalResolveRequest(body)
		if err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		tr, err := UnmarshalTaeRead(req.TaeRead, uint64(now().UnixMilli()))
		if err != nil {
			http.Error(w, "invalid TaeRead", http.StatusUnauthorized)
			return
		}
		lease, ok, resolveErr := leases.resolve(r.Context(), tr.ReadRef)
		if resolveErr != nil {
			http.Error(w, "resolver unavailable", http.StatusServiceUnavailable)
			return
		}
		if !ok || !equalBytes(lease.AuthorizedClientSPKIHash, principalHash[:]) ||
			lease.Read.AccountID != tr.AccountID || lease.Read.DatabaseID != tr.DatabaseID || !equalBytes(lease.Read.QueryID, tr.QueryID) ||
			!equalBytes(lease.Read.SchemaDigest, tr.SchemaDigest) || !equalBytes(lease.Read.ManifestSHA256, tr.ManifestSHA256) ||
			!equalBytes(lease.Wire, req.TaeRead) || !equalBytes(lease.CanonicalSchema, req.RequestedSchema) {
			http.Error(w, "read lease not found", http.StatusNotFound)
			return
		}
		response := ResolveTaeReadResponse{TaeRead: lease.Wire, Manifest: lease.Manifest, CanonicalSchema: lease.CanonicalSchema}
		_, err = resolveResponseSize(response)
		if err != nil {
			http.Error(w, "invalid lease", http.StatusInternalServerError)
			return
		}
		readRefHash := sha256.Sum256(tr.ReadRef)
		audit := ResolveAuditEvent{
			AccountID:      tr.AccountID,
			DatabaseID:     tr.DatabaseID,
			TableID:        tr.TableID,
			QueryID:        append([]byte(nil), tr.QueryID...),
			ClientSPKIHash: append([]byte(nil), principalHash[:]...),
			ReadRefSHA256:  append([]byte(nil), readRefHash[:]...),
		}
		auditCtx, cancelAudit := context.WithTimeoutCause(
			r.Context(),
			resolveAuditTimeout,
			moerr.NewInternalErrorNoCtx("substrait: resolution audit timed out"),
		)
		err = auditor.RecordResolve(auditCtx, audit)
		cancelAudit()
		if err != nil {
			http.Error(w, "resolution audit unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_ = writeResolveResponse(w, response)
	})
}

func readResolveRequestBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	limited := http.MaxBytesReader(w, r.Body, maxResolveRequestSize)
	body := make([]byte, maxResolveRequestSize+1)
	n, err := io.ReadFull(limited, body)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return body[:n], nil
	}
	if err != nil {
		return nil, err
	}
	return nil, moerr.NewInternalErrorNoCtx("substrait: resolve request exceeds size limit")
}
