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
	ResolvePath            = "/internal/v1/sidecar/read/resolve"
	MaxLeaseTTL            = 20 * time.Minute
	MaxManifestBytes       = 64 << 20
	rollbackCleanupTimeout = 30 * time.Second
	resolveAuditTimeout    = 5 * time.Second
	maxManifestSize        = MaxManifestBytes
	maxCanonicalSchemaSize = 1 << 20
)

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

// LeaseJournal is the durable boundary for resolver authority. Store must
// make a complete lease durable before returning. MarkReleased must durably
// prevent replay before GC protection is removed. Load must visit records one
// at a time and must not retain a record after visit returns.
type LeaseJournal interface {
	Store(context.Context, *Lease) error
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

// LeaseManager owns the single bounded lifetime from admission through
// terminal release. It never evicts a live lease to make room.
type LeaseManager struct {
	mu        sync.RWMutex
	leases    map[string]*Lease
	protector Protector
	journal   LeaseJournal
	maximum   int
	now       func() time.Time
	ready     bool
}

func NewLeaseManager(maximum int, protector Protector) *LeaseManager {
	return NewPersistentLeaseManager(maximum, protector, nil)
}

func NewPersistentLeaseManager(maximum int, protector Protector, journal LeaseJournal) *LeaseManager {
	if maximum <= 0 {
		maximum = 1
	}
	return &LeaseManager{leases: make(map[string]*Lease), protector: protector, journal: journal, maximum: maximum, now: time.Now, ready: journal == nil}
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
	m.mu.Lock()
	if !m.ready {
		m.mu.Unlock()
		return moerr.NewInternalErrorNoCtx("substrait: durable read leases have not been replayed")
	}
	if m.protector == nil || register == nil || rollback == nil {
		m.mu.Unlock()
		return moerr.NewInternalErrorNoCtx("substrait: read lease GC protection is not configured")
	}
	if err := m.pruneExpiredLocked(ctx); err != nil {
		m.mu.Unlock()
		return err
	}
	if len(m.leases)+len(leases) > m.maximum {
		m.mu.Unlock()
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
			m.mu.Unlock()
			return moerr.NewInternalErrorNoCtx("substrait: invalid or duplicate read lease")
		}
		seen[key] = struct{}{}
	}
	stored := make([]*Lease, 0, len(leases))
	for _, l := range leases {
		if m.journal != nil {
			if err := m.journal.Store(ctx, l); err != nil {
				// Store can fail after the write became visible. Include the
				// ambiguous record in the durable revocation set.
				stored = append(stored, l)
				rollbackErr := m.rollbackAcquisition(ctx, rollback, stored, nil)
				m.mu.Unlock()
				return errors.Join(moerr.NewInternalErrorNoCtxf("substrait: persist read lease: %v", err), rollbackErr)
			}
		}
		stored = append(stored, l)
	}
	registered := make([]*Lease, 0, len(leases))
	for _, l := range leases {
		err := register(ctx, l.Read.ReadRef, l.ObjectNames, time.UnixMilli(int64(l.Read.ExpiresAtUnixMS)))
		if err != nil {
			rollbackErr := m.rollbackAcquisition(ctx, rollback, stored, registered)
			m.mu.Unlock()
			return errors.Join(moerr.NewInternalErrorNoCtxf("substrait: protect read lease: %v", err), rollbackErr)
		}
		registered = append(registered, l)
	}
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

func (m *LeaseManager) Resolve(readRef []byte) (*Lease, bool) {
	m.mu.RLock()
	if !m.ready {
		m.mu.RUnlock()
		return nil, false
	}
	l := m.leases[string(readRef)]
	if l != nil && (l.Released || l.Read.ExpiresAtUnixMS <= uint64(m.now().UnixMilli())) {
		l = nil
	}
	result := cloneLease(l)
	m.mu.RUnlock()
	return result, result != nil
}

func (m *LeaseManager) Release(ctx context.Context, readRef []byte) error {
	cleanupCtx, cancel := leaseCleanupContext(ctx)
	defer cancel()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.ready {
		return moerr.NewInternalErrorNoCtx("substrait: durable read leases have not been replayed")
	}
	l := m.leases[string(readRef)]
	if l == nil {
		return nil
	}
	if err := m.releaseLocked(cleanupCtx, l); err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: release read lease: %v", err)
	}
	return nil
}

func leaseCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeoutCause(
		context.WithoutCancel(ctx),
		rollbackCleanupTimeout,
		moerr.NewInternalErrorNoCtx("substrait: read lease cleanup timed out"),
	)
}

func (m *LeaseManager) pruneExpiredLocked(ctx context.Context) error {
	now := uint64(m.now().UnixMilli())
	for _, l := range m.leases {
		if l.Read.ExpiresAtUnixMS <= now {
			if err := m.releaseLocked(ctx, l); err != nil {
				return moerr.NewInternalErrorNoCtxf("substrait: prune expired read lease: %v", err)
			}
		}
	}
	return nil
}

func (m *LeaseManager) releaseLocked(ctx context.Context, l *Lease) error {
	if !l.Released {
		if m.journal != nil {
			if err := m.journal.MarkReleased(ctx, l.Read.ReadRef); err != nil {
				return err
			}
		}
		l.Released = true
	}
	if m.protector != nil {
		if err := m.protector.Unregister(ctx, l.Read.ReadRef); err != nil {
			return err
		}
	}
	if m.journal != nil {
		if err := m.journal.Delete(ctx, l.Read.ReadRef); err != nil {
			return err
		}
	}
	delete(m.leases, string(l.Read.ReadRef))
	return nil
}

// Replay restores durable leases and their GC protection. Call it before the
// resolver becomes reachable or GC starts.
func (m *LeaseManager) Replay(ctx context.Context) error {
	if m.journal == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ready || len(m.leases) != 0 {
		return moerr.NewInternalErrorNoCtx("substrait: cannot replay into a live lease manager")
	}
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
	var live []*Lease
	var replayErr error
	err = m.journal.Load(ctx, func(l *Lease) error {
		if err := validateLease(l, now, true); err != nil {
			replayErr = moerr.NewInternalErrorNoCtxf("substrait: invalid durable read lease: %v", err)
			return replayErr
		}
		if l.Released || l.Read.ExpiresAtUnixMS <= now {
			if err := m.releaseLocked(ctx, l); err != nil {
				replayErr = moerr.NewInternalErrorNoCtxf("substrait: clean durable read lease: %v", err)
				return replayErr
			}
			return nil
		}
		if len(live) == m.maximum {
			replayErr = moerr.NewInternalErrorNoCtx("substrait: durable read leases exceed capacity")
			return replayErr
		}
		live = append(live, l)
		return nil
	})
	if replayErr != nil {
		return replayErr
	}
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: load read leases: %v", err)
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
	for _, l := range live {
		m.leases[string(l.Read.ReadRef)] = cloneLease(l)
	}
	m.ready = true
	return nil
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

// Admit performs storage work only after Export has accepted the complete
// logical plan. It publishes all table leases atomically or none of them.
func Admit(ctx context.Context, r AdmissionRequest) (map[int32][]byte, error) {
	if r.Candidate == nil || r.Provider == nil || r.Leases == nil || !r.ReadOnly || r.PriorWrites {
		return nil, moerr.NewInternalErrorNoCtx("substrait: transaction is not an admissible read-only snapshot")
	}
	if r.AccountID == 0 || len(r.QueryID) == 0 || len(r.SnapshotTS) != 12 || len(r.AuthorizedClientSPKIHash) != sha256.Size {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid admission identity")
	}
	if r.TTL <= 0 || r.TTL > MaxLeaseTTL {
		return nil, moerr.NewInternalErrorNoCtx("substrait: lease TTL is outside the supported bound")
	}
	if r.Random == nil {
		r.Random = rand.Reader
	}
	if r.Now.IsZero() {
		r.Now = time.Now()
	}
	expires := r.Now.Add(r.TTL).UnixMilli()
	if expires <= r.Now.UnixMilli() || expires <= 0 {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid lease expiry")
	}
	var wires map[int32][]byte
	err := r.Leases.acquirePrepared(ctx, func() ([]*Lease, error) {
		reads := r.Candidate.Reads()
		leases := make([]*Lease, 0, len(reads))
		wires = make(map[int32][]byte, len(reads))
		for _, read := range reads {
			read.AccountID = r.AccountID
			facts, err := r.Provider.PrepareSnapshotRead(ctx, read, r.SnapshotTS)
			if err != nil {
				return nil, moerr.NewInternalErrorNoCtxf("substrait: prepare table %d: %v", read.TableID, err)
			}
			if facts.CommittedInMemory || facts.Uncommitted || facts.VisibleTombstones || facts.NonTAE {
				return nil, moerr.NewInternalErrorNoCtxf("substrait: table %d has snapshot state unsupported by Sirius v1", read.TableID)
			}
			if len(facts.Manifest) == 0 || len(facts.Manifest) > maxManifestSize || len(facts.CanonicalSchema) > maxCanonicalSchemaSize || !equalBytes(facts.CanonicalSchema, read.Schema) {
				return nil, moerr.NewInternalErrorNoCtxf("substrait: table %d schema or manifest mismatch", read.TableID)
			}
			ref := make([]byte, 32)
			if _, err = io.ReadFull(r.Random, ref); err != nil {
				return nil, moerr.NewInternalErrorNoCtxf("substrait: create read reference: %v", err)
			}
			schemaHash := sha256.Sum256(facts.CanonicalSchema)
			manifestHash := sha256.Sum256(facts.Manifest)
			tr := &TaeRead{ProtocolVersion: TaeReadProtocolVersion, ReadRef: ref, QueryID: append([]byte(nil), r.QueryID...), AccountID: r.AccountID, DatabaseID: read.DatabaseID, TableID: read.TableID, SnapshotTS: append([]byte(nil), r.SnapshotTS...), SchemaDigest: schemaHash[:], ManifestSHA256: manifestHash[:], CapabilityHash: CapabilityHash[:], ExpiresAtUnixMS: uint64(expires)}
			wire, err := MarshalTaeRead(tr)
			if err != nil {
				return nil, err
			}
			leases = append(leases, &Lease{Read: tr, Wire: wire, Manifest: facts.Manifest, CanonicalSchema: facts.CanonicalSchema, AuthorizedClientSPKIHash: append([]byte(nil), r.AuthorizedClientSPKIHash...), ObjectNames: facts.ObjectNames})
			wires[read.NodeID] = wire
		}
		return leases, nil
	})
	if err != nil {
		return nil, err
	}
	return wires, nil
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
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxResolveRequestSize))
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
		if leases == nil || auditor == nil {
			http.Error(w, "resolver unavailable", http.StatusServiceUnavailable)
			return
		}
		lease, ok := leases.Resolve(tr.ReadRef)
		if !ok || !equalBytes(lease.AuthorizedClientSPKIHash, principalHash[:]) ||
			lease.Read.AccountID != tr.AccountID || lease.Read.DatabaseID != tr.DatabaseID || !equalBytes(lease.Read.QueryID, tr.QueryID) ||
			!equalBytes(lease.Read.SchemaDigest, tr.SchemaDigest) || !equalBytes(lease.Read.ManifestSHA256, tr.ManifestSHA256) ||
			!equalBytes(lease.Wire, req.TaeRead) || !equalBytes(lease.CanonicalSchema, req.RequestedSchema) {
			http.Error(w, "read lease not found", http.StatusNotFound)
			return
		}
		response, err := MarshalResolveResponse(ResolveTaeReadResponse{TaeRead: lease.Wire, Manifest: lease.Manifest, CanonicalSchema: lease.CanonicalSchema})
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
		_, _ = w.Write(response)
	})
}
