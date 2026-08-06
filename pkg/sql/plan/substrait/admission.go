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
	"io"
	"mime"
	"net/http"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const ResolvePath = "/internal/v1/sidecar/read/resolve"
const MaxLeaseTTL = 20 * time.Minute

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

// Protector is the narrow GC-protection seam. Register must reject while GC
// is already running, so a lease can never appear after its objects were swept.
type Protector interface {
	Register(context.Context, []byte, []string, time.Time) error
	Unregister(context.Context, []byte) error
}

// LeaseJournal is the durable boundary for resolver authority. Store must
// make a complete lease durable before returning. MarkReleased must durably
// prevent replay before GC protection is removed.
type LeaseJournal interface {
	Store(context.Context, *Lease) error
	MarkReleased(context.Context, []byte) error
	Delete(context.Context, []byte) error
	Load(context.Context) ([]*Lease, error)
}

type Lease struct {
	Read                            *TaeRead
	Wire, Manifest, CanonicalSchema []byte
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
	if len(leases) == 0 {
		return moerr.NewInternalErrorNoCtx("substrait: empty lease acquisition")
	}
	m.mu.Lock()
	if !m.ready {
		m.mu.Unlock()
		return moerr.NewInternalErrorNoCtx("substrait: durable read leases have not been replayed")
	}
	if m.protector == nil {
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
				for i := len(stored) - 1; i >= 0; i-- {
					_ = m.journal.Delete(ctx, stored[i].Read.ReadRef)
				}
				m.mu.Unlock()
				return moerr.NewInternalErrorNoCtxf("substrait: persist read lease: %v", err)
			}
		}
		stored = append(stored, l)
	}
	registered := make([]*Lease, 0, len(leases))
	for _, l := range leases {
		if m.protector != nil {
			err := m.protector.Register(ctx, l.Read.ReadRef, l.ObjectNames, time.UnixMilli(int64(l.Read.ExpiresAtUnixMS)))
			if err != nil {
				for i := len(registered) - 1; i >= 0; i-- {
					_ = m.protector.Unregister(ctx, registered[i].Read.ReadRef)
				}
				if m.journal != nil {
					for i := len(stored) - 1; i >= 0; i-- {
						_ = m.journal.Delete(ctx, stored[i].Read.ReadRef)
					}
				}
				m.mu.Unlock()
				return moerr.NewInternalErrorNoCtxf("substrait: protect read lease: %v", err)
			}
		}
		registered = append(registered, l)
	}
	for _, l := range leases {
		m.leases[string(l.Read.ReadRef)] = cloneLease(l)
	}
	m.mu.Unlock()
	return nil
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.ready {
		return moerr.NewInternalErrorNoCtx("substrait: durable read leases have not been replayed")
	}
	l := m.leases[string(readRef)]
	if l == nil {
		return nil
	}
	if !l.Released {
		if m.journal != nil {
			if err := m.journal.MarkReleased(ctx, readRef); err != nil {
				return moerr.NewInternalErrorNoCtxf("substrait: persist read lease release: %v", err)
			}
		}
		l.Released = true
	}
	if m.protector != nil {
		if err := m.protector.Unregister(ctx, readRef); err != nil {
			return err
		}
	}
	if m.journal != nil {
		if err := m.journal.Delete(ctx, readRef); err != nil {
			return moerr.NewInternalErrorNoCtxf("substrait: delete released read lease: %v", err)
		}
	}
	delete(m.leases, string(readRef))
	return nil
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
	loaded, err := m.journal.Load(ctx)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: load read leases: %v", err)
	}
	if len(loaded) > m.maximum {
		return moerr.NewInternalErrorNoCtx("substrait: durable read leases exceed capacity")
	}
	now := uint64(m.now().UnixMilli())
	registered := make([]*Lease, 0, len(loaded))
	for _, l := range loaded {
		if err := validateLease(l, now, true); err != nil {
			return moerr.NewInternalErrorNoCtxf("substrait: invalid durable read lease: %v", err)
		}
		if l.Released || l.Read.ExpiresAtUnixMS <= now {
			if err := m.releaseLocked(ctx, l); err != nil {
				return moerr.NewInternalErrorNoCtxf("substrait: clean durable read lease: %v", err)
			}
			continue
		}
		if m.protector != nil {
			if err := m.protector.Register(ctx, l.Read.ReadRef, l.ObjectNames, time.UnixMilli(int64(l.Read.ExpiresAtUnixMS))); err != nil {
				return moerr.NewInternalErrorNoCtxf("substrait: replay read lease protection: %v", err)
			}
		}
		registered = append(registered, l)
	}
	for _, l := range registered {
		m.leases[string(l.Read.ReadRef)] = cloneLease(l)
	}
	m.ready = true
	return nil
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
	if len(l.Wire) == 0 || len(l.Manifest) == 0 || len(l.Manifest) > 64<<20 || len(l.CanonicalSchema) == 0 || len(l.CanonicalSchema) > 1<<20 {
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
	Candidate           *Candidate
	Provider            SnapshotProvider
	Leases              *LeaseManager
	AccountID           uint64
	QueryID, SnapshotTS []byte
	TTL                 time.Duration
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
	if r.AccountID == 0 || len(r.QueryID) == 0 || len(r.SnapshotTS) != 12 {
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
	reads := r.Candidate.Reads()
	leases := make([]*Lease, 0, len(reads))
	wires := make(map[int32][]byte, len(reads))
	for _, read := range reads {
		facts, err := r.Provider.PrepareSnapshotRead(ctx, read, r.SnapshotTS)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: prepare table %d: %v", read.TableID, err)
		}
		if facts.CommittedInMemory || facts.Uncommitted || facts.VisibleTombstones || facts.NonTAE {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: table %d has snapshot state unsupported by Sirius v1", read.TableID)
		}
		if len(facts.Manifest) == 0 || len(facts.Manifest) > 64<<20 || len(facts.CanonicalSchema) > 1<<20 || !equalBytes(facts.CanonicalSchema, read.Schema) {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: table %d schema or manifest mismatch", read.TableID)
		}
		ref := make([]byte, 32)
		if _, err = io.ReadFull(r.Random, ref); err != nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: create read reference: %v", err)
		}
		schemaHash := sha256.Sum256(facts.CanonicalSchema)
		manifestHash := sha256.Sum256(facts.Manifest)
		tr := &TaeRead{ProtocolVersion: TaeReadProtocolVersion, ReadRef: ref, QueryID: append([]byte(nil), r.QueryID...), AccountID: r.AccountID, TableID: read.TableID, SnapshotTS: append([]byte(nil), r.SnapshotTS...), SchemaDigest: schemaHash[:], ManifestSHA256: manifestHash[:], CapabilityHash: CapabilityHash[:], ExpiresAtUnixMS: uint64(expires)}
		wire, err := MarshalTaeRead(tr)
		if err != nil {
			return nil, err
		}
		leases = append(leases, &Lease{Read: tr, Wire: wire, Manifest: facts.Manifest, CanonicalSchema: facts.CanonicalSchema, ObjectNames: facts.ObjectNames})
		wires[read.NodeID] = wire
	}
	if err := r.Leases.Acquire(ctx, leases); err != nil {
		return nil, err
	}
	return wires, nil
}

// ResolveHandler exposes the exact strict, mTLS-only Sirius resolver route.
func ResolveHandler(leases *LeaseManager, now func() time.Time) http.Handler {
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
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			http.Error(w, "verified client certificate required", http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 17<<20))
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
		if leases == nil {
			http.Error(w, "resolver unavailable", http.StatusServiceUnavailable)
			return
		}
		lease, ok := leases.Resolve(tr.ReadRef)
		if !ok || !equalBytes(lease.Wire, req.TaeRead) || !equalBytes(lease.CanonicalSchema, req.RequestedSchema) {
			http.Error(w, "read lease not found", http.StatusNotFound)
			return
		}
		response, err := MarshalResolveResponse(ResolveTaeReadResponse{TaeRead: lease.Wire, Manifest: lease.Manifest, CanonicalSchema: lease.CanonicalSchema})
		if err != nil {
			http.Error(w, "invalid lease", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(response)
	})
}
