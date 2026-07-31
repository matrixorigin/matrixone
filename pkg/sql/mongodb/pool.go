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

package mongodb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const clientCleanupTimeout = 10 * time.Second

type poolKey struct {
	accountID    uint32
	connectionID uint64
	version      uint64
	identity     string
}

type poolEntry struct {
	client   Client
	refs     int
	draining bool
	lastUsed uint64
}

type connectionKey struct {
	accountID    uint32
	connectionID uint64
}

type retirementState struct {
	versionFloor uint64
	dropped      bool
}

// ClientPool keeps authentication state tenant- and generation-local. A
// rotated connection creates a new key and drains prior generations only
// after their last cursor lease is released.
type ClientPool struct {
	mu      sync.Mutex
	factory ClientFactory
	// validator closes the resolver-to-pool race. Acquire registers an
	// in-flight generation first, then rechecks the catalog version; therefore
	// ALTER/DROP either makes validation fail or observes this Connect and keeps
	// its retirement tombstone until publication finishes.
	validator ConnectionResolver
	entries   map[poolKey]*poolEntry
	// A statement can resolve version N just before ALTER/DROP, then finish
	// Connect after the DDL retirement pass. These tombstones ensure that such
	// a late client starts draining instead of republishing an old idle pool.
	retirements map[connectionKey]retirementState
	connecting  map[connectionKey]int
	closed      bool
	clock       uint64
	maxIdle     int
}

func NewClientPool(factory ClientFactory, maxCachedClients ...int) *ClientPool {
	return newClientPool(factory, nil, maxCachedClients...)
}

func NewValidatedClientPool(factory ClientFactory, validator ConnectionResolver, maxCachedClients ...int) *ClientPool {
	return newClientPool(factory, validator, maxCachedClients...)
}

func newClientPool(factory ClientFactory, validator ConnectionResolver, maxCachedClients ...int) *ClientPool {
	maxIdle := DefaultRuntimeConfig().MaxCachedClients
	if len(maxCachedClients) > 0 && maxCachedClients[0] > 0 {
		maxIdle = maxCachedClients[0]
	}
	return &ClientPool{
		factory:     factory,
		validator:   validator,
		entries:     make(map[poolKey]*poolEntry),
		retirements: make(map[connectionKey]retirementState),
		connecting:  make(map[connectionKey]int),
		maxIdle:     maxIdle,
	}
}

type ClientLease struct {
	pool   *ClientPool
	key    poolKey
	client Client
	once   sync.Once
}

func (l *ClientLease) Client() Client { return l.client }

func (l *ClientLease) Release(ctx context.Context) error {
	var err error
	l.once.Do(func() { err = l.pool.release(ctx, l.key) })
	return err
}

func (p *ClientPool) Acquire(ctx context.Context, connection Connection, credentials Credentials, cfg RuntimeConfig) (*ClientLease, error) {
	key := poolKey{
		accountID:    connection.AccountID,
		connectionID: connection.ConnectionID,
		version:      connection.Version,
		identity:     credentialIdentity(credentials),
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, moerr.NewInternalError(ctx, "MongoDB client pool is closed")
	}
	if entry := p.entries[key]; entry != nil {
		// Exact-key reuse is safe even while the generation is draining: only
		// statements whose catalog plan already carries this old version can
		// ask for it. New statements resolve the newer version and cannot land
		// here.
		entry.refs++
		client := entry.client
		p.mu.Unlock()
		return &ClientLease{pool: p, key: key, client: client}, nil
	}
	sourceKey := connectionKey{accountID: key.accountID, connectionID: key.connectionID}
	p.connecting[sourceKey]++
	retired := p.evictIdleLocked(1)
	p.mu.Unlock()
	_ = disconnectClients(retired)
	if p.validator != nil {
		validated, validateErr := p.validator.ResolveMongoDBConnection(
			ctx, connection.AccountID, connection.ConnectionID, connection.Version)
		if validateErr != nil {
			p.mu.Lock()
			p.finishConnectLocked(sourceKey)
			p.pruneRetirementLocked(sourceKey)
			p.mu.Unlock()
			return nil, validateErr
		}
		// A resolver is authoritative for all non-secret connection metadata.
		// The secret was resolved from the first read immediately before this
		// call, so a same-version identity mismatch is catalog corruption or an
		// invalid resolver and must fail closed rather than mix credentials.
		if validated.AccountID != connection.AccountID ||
			validated.ConnectionID != connection.ConnectionID ||
			validated.Version != connection.Version ||
			validated.CredentialSecretRef != connection.CredentialSecretRef ||
			validated.TLSCASecretRef != connection.TLSCASecretRef {
			p.mu.Lock()
			p.finishConnectLocked(sourceKey)
			p.pruneRetirementLocked(sourceKey)
			p.mu.Unlock()
			return nil, moerr.NewInvalidInput(ctx, "MongoDB connection changed during client acquisition")
		}
		connection = validated
	}

	client, err := p.factory.Connect(ctx, connection, credentials, cfg)
	if err != nil {
		p.mu.Lock()
		p.finishConnectLocked(sourceKey)
		p.pruneRetirementLocked(sourceKey)
		p.mu.Unlock()
		return nil, err
	}

	p.mu.Lock()
	p.finishConnectLocked(sourceKey)
	if p.closed {
		p.mu.Unlock()
		_ = disconnectClients([]Client{client})
		return nil, moerr.NewInternalError(ctx, "MongoDB client pool is closed")
	}
	if entry := p.entries[key]; entry != nil {
		entry.refs++
		existing := entry.client
		p.pruneRetirementLocked(sourceKey)
		p.mu.Unlock()
		_ = disconnectClients([]Client{client})
		return &ClientLease{pool: p, key: key, client: existing}, nil
	}
	retired = retired[:0]
	incomingDraining := p.isRetiredLocked(key)
	for oldKey, entry := range p.entries {
		if oldKey.accountID == key.accountID && oldKey.connectionID == key.connectionID && oldKey != key {
			// A statement planned before a catalog-version rotation can finish
			// Connect after the newer generation has already been published. It
			// may use its own client, but it must not drain that newer generation.
			// At the same catalog version, a changed credential digest represents
			// in-place secret-manager rotation, so the newly resolved identity wins.
			if oldKey.version > key.version {
				incomingDraining = true
				continue
			}
			entry.draining = true
			if entry.refs == 0 {
				delete(p.entries, oldKey)
				retired = append(retired, entry.client)
			}
		}
	}
	retired = append(retired, p.evictIdleLocked(1)...)
	p.clock++
	p.entries[key] = &poolEntry{client: client, refs: 1, draining: incomingDraining, lastUsed: p.clock}
	p.pruneRetirementLocked(sourceKey)
	p.mu.Unlock()
	_ = disconnectClients(retired)
	return &ClientLease{pool: p, key: key, client: client}, nil
}

func credentialIdentity(credentials Credentials) string {
	hash := sha256.New()
	var size [8]byte
	for _, value := range [][]byte{[]byte(credentials.Username), []byte(credentials.Password), credentials.TLSCA} {
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(value)
	}
	// The digest is an opaque in-memory map key. It is never formatted or
	// serialized, and unlike the secret references it changes when a secret
	// manager rotates content in place.
	return string(hash.Sum(nil))
}

func (p *ClientPool) release(_ context.Context, key poolKey) error {
	p.mu.Lock()
	entry := p.entries[key]
	if entry == nil {
		p.mu.Unlock()
		return nil
	}
	if entry.refs > 0 {
		entry.refs--
	}
	if entry.refs != 0 {
		p.mu.Unlock()
		return nil
	}
	p.clock++
	entry.lastUsed = p.clock
	var clients []Client
	if entry.draining {
		delete(p.entries, key)
		clients = append(clients, entry.client)
	}
	clients = append(clients, p.evictIdleLocked(0)...)
	p.pruneRetirementLocked(connectionKey{accountID: key.accountID, connectionID: key.connectionID})
	p.mu.Unlock()
	return disconnectClients(clients)
}

// RetireBefore marks every older generation of one tenant connection as
// draining. Idle clients disconnect immediately; active statements retain
// their leased client until the last release. The exclusive upper bound lets
// a DDL commit retire the version it replaced without racing and accidentally
// retiring a new-version client acquired immediately after that commit.
func (p *ClientPool) RetireBefore(accountID uint32, connectionID, versionExclusive uint64) error {
	p.mu.Lock()
	connection := connectionKey{accountID: accountID, connectionID: connectionID}
	retirement := p.retirements[connection]
	if versionExclusive > retirement.versionFloor {
		retirement.versionFloor = versionExclusive
	}
	p.retirements[connection] = retirement
	clients := make([]Client, 0)
	for key, entry := range p.entries {
		if key.accountID != accountID || key.connectionID != connectionID || key.version >= versionExclusive {
			continue
		}
		entry.draining = true
		if entry.refs == 0 {
			delete(p.entries, key)
			clients = append(clients, entry.client)
		}
	}
	p.pruneRetirementLocked(connection)
	p.mu.Unlock()
	return disconnectClients(clients)
}

// RetireConnection is used after DROP, where no catalog generation can be
// acquired again. It has the same active-lease semantics as RetireBefore.
func (p *ClientPool) RetireConnection(accountID uint32, connectionID uint64) error {
	p.mu.Lock()
	connection := connectionKey{accountID: accountID, connectionID: connectionID}
	retirement := p.retirements[connection]
	retirement.dropped = true
	p.retirements[connection] = retirement
	clients := make([]Client, 0)
	for key, entry := range p.entries {
		if key.accountID != accountID || key.connectionID != connectionID {
			continue
		}
		entry.draining = true
		if entry.refs == 0 {
			delete(p.entries, key)
			clients = append(clients, entry.client)
		}
	}
	p.pruneRetirementLocked(connection)
	p.mu.Unlock()
	return disconnectClients(clients)
}

// RetireAccount drains every client owned by an account after DROP ACCOUNT.
// It also installs per-connection tombstones for Connect calls that started
// before the catalog transaction committed and have not published yet.
func (p *ClientPool) RetireAccount(accountID uint32) error {
	p.mu.Lock()
	connections := make(map[connectionKey]struct{})
	clients := make([]Client, 0)
	for key, entry := range p.entries {
		if key.accountID != accountID {
			continue
		}
		connection := connectionKey{accountID: key.accountID, connectionID: key.connectionID}
		connections[connection] = struct{}{}
		retirement := p.retirements[connection]
		retirement.dropped = true
		p.retirements[connection] = retirement
		entry.draining = true
		if entry.refs == 0 {
			delete(p.entries, key)
			clients = append(clients, entry.client)
		}
	}
	for connection := range p.connecting {
		if connection.accountID != accountID {
			continue
		}
		connections[connection] = struct{}{}
		retirement := p.retirements[connection]
		retirement.dropped = true
		p.retirements[connection] = retirement
	}
	for connection := range connections {
		p.pruneRetirementLocked(connection)
	}
	p.mu.Unlock()
	return disconnectClients(clients)
}

func (p *ClientPool) isRetiredLocked(key poolKey) bool {
	retirement := p.retirements[connectionKey{accountID: key.accountID, connectionID: key.connectionID}]
	return retirement.dropped || key.version < retirement.versionFloor
}

func (p *ClientPool) finishConnectLocked(connection connectionKey) {
	if p.connecting[connection] <= 1 {
		delete(p.connecting, connection)
	} else {
		p.connecting[connection]--
	}
}

// A retirement tombstone is needed only while a pre-DDL Connect can still
// publish, or while an affected entry remains leased/reusable. Removing it at
// that point keeps repeated create/drop cycles from growing metadata without
// bound while preserving the generation race barrier.
func (p *ClientPool) pruneRetirementLocked(connection connectionKey) {
	retirement, ok := p.retirements[connection]
	if !ok || p.connecting[connection] != 0 {
		return
	}
	for key := range p.entries {
		if key.accountID != connection.accountID || key.connectionID != connection.connectionID {
			continue
		}
		if retirement.dropped || key.version < retirement.versionFloor {
			return
		}
	}
	delete(p.retirements, connection)
}

// evictIdleLocked bounds client objects (each of which owns a driver socket
// pool) without disrupting active statements. extra is the number of entries
// the caller is about to add. The mutex must be held.
func (p *ClientPool) evictIdleLocked(extra int) []Client {
	clients := make([]Client, 0)
	for len(p.entries)+extra > p.maxIdle {
		var oldestKey poolKey
		var oldest *poolEntry
		for key, entry := range p.entries {
			if entry.refs != 0 || oldest != nil && entry.lastUsed >= oldest.lastUsed {
				continue
			}
			oldestKey, oldest = key, entry
		}
		if oldest == nil {
			break
		}
		delete(p.entries, oldestKey)
		clients = append(clients, oldest.client)
	}
	return clients
}

func (p *ClientPool) Close(_ context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	clients := make([]Client, 0, len(p.entries))
	for _, entry := range p.entries {
		clients = append(clients, entry.client)
	}
	p.entries = make(map[poolKey]*poolEntry)
	p.retirements = make(map[connectionKey]retirementState)
	p.connecting = make(map[connectionKey]int)
	p.mu.Unlock()
	return disconnectClients(clients)
}

func disconnectClients(clients []Client) error {
	if len(clients) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientCleanupTimeout)
	defer cancel()
	var first error
	for _, client := range clients {
		if err := client.Disconnect(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

type SecretResolver interface {
	ResolveMongoDBCredentials(context.Context, uint32, string, string) (Credentials, error)
}

// ConnectionResolver reads non-secret connection metadata for the current
// tenant and requires the catalog version to match the version embedded in the
// plan. Implementations must fail closed on a disabled or stale row.
type ConnectionResolver interface {
	ResolveMongoDBConnection(context.Context, uint32, uint64, uint64) (Connection, error)
}

// MappingResolver reads the exact tenant-scoped mapping generation embedded in
// the plan. The operator validates the returned namespace and schema before it
// opens a source cursor, so catalog drift cannot silently redirect an old plan.
type MappingResolver interface {
	ResolveMongoDBMapping(context.Context, uint32, uint64, uint64) (TableMapping, error)
}

type RuntimeDependencies struct {
	Config      RuntimeConfig
	Connections ConnectionResolver
	Mappings    MappingResolver
	Secrets     SecretResolver
	Pool        *ClientPool
	Limiter     *SourceLimiter
}

const (
	RuntimeDependenciesKey = "mongodb.runtime-dependencies"
)
