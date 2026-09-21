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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type leaseManagerBrokerState uint8

const (
	leaseManagerBrokerEmpty leaseManagerBrokerState = iota
	leaseManagerBrokerPreparing
	leaseManagerBrokerPublished
	leaseManagerBrokerSealed
)

// LeaseManagerBroker transfers ownership of exactly one replayed, co-located
// TAE lease-manager generation from TN construction to the embedded CN. It is
// deliberately not a replacement registry: observing a second live storage
// generation seals the broker so callers fail closed instead of silently
// switching the GC-protection authority beneath an active CN.
//
// A broker belongs to one process launch. It has no Reset operation. The
// launcher must create a new broker for a new process generation and must seal
// the old broker before tearing down the TAE protection owner.
type LeaseManagerBroker struct {
	mu         sync.RWMutex
	state      leaseManagerBrokerState
	generation uint64
	identity   string
	manager    *LeaseManager
}

// LeaseManagerPublication is a prepare token. Publish is valid only after the
// complete TAE storage (including its logtail server) has been constructed.
// Abort removes an unpublished preparation after storage-open failure.
type LeaseManagerPublication struct {
	broker     *LeaseManagerBroker
	generation uint64
	identity   string
	manager    *LeaseManager
	mu         sync.Mutex
	done       bool
}

func NewLeaseManagerBroker() *LeaseManagerBroker {
	return new(LeaseManagerBroker)
}

// Prepare reserves the broker for manager without making it acquirable.
func (b *LeaseManagerBroker) Prepare(
	storageIdentity string,
	manager *LeaseManager,
) (*LeaseManagerPublication, error) {
	if b == nil || storageIdentity == "" || manager == nil || !manager.DurableReady() {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid lease manager publication")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case leaseManagerBrokerEmpty:
		b.generation++
		b.state = leaseManagerBrokerPreparing
		b.identity = storageIdentity
		b.manager = manager
		return &LeaseManagerPublication{
			broker: b, generation: b.generation,
			identity: storageIdentity, manager: manager,
		}, nil
	case leaseManagerBrokerPreparing, leaseManagerBrokerPublished:
		// A concurrent or replacement storage generation invalidates the whole
		// single-generation handoff. Keep the manager's protection alive, but
		// make it impossible for a CN to acquire either ambiguous generation.
		b.state = leaseManagerBrokerSealed
		b.manager = nil
		return nil, moerr.NewInvalidStateNoCtx("substrait: a second TAE lease manager generation is not supported")
	case leaseManagerBrokerSealed:
		return nil, moerr.NewInvalidStateNoCtx("substrait: TAE lease manager broker is sealed")
	default:
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid lease manager broker state")
	}
}

// Publish is the linearization point at which later CN injection may acquire
// the manager. It never replaces an existing publication.
func (p *LeaseManagerPublication) Publish() error {
	if p == nil || p.broker == nil {
		return moerr.NewInternalErrorNoCtx("substrait: invalid lease manager publication token")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return moerr.NewInvalidStateNoCtx("substrait: lease manager publication token is already terminal")
	}
	b := p.broker
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != leaseManagerBrokerPreparing || b.generation != p.generation ||
		b.identity != p.identity || b.manager != p.manager {
		p.done = true
		return moerr.NewInvalidStateNoCtx("substrait: lease manager publication was superseded")
	}
	b.state = leaseManagerBrokerPublished
	p.done = true
	return nil
}

// Abort forgets only this unpublished preparation. It never clears a
// published manager or unprotects its recovered leases.
func (p *LeaseManagerPublication) Abort() error {
	if p == nil || p.broker == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return nil
	}
	b := p.broker
	b.mu.Lock()
	if b.state == leaseManagerBrokerPreparing && b.generation == p.generation &&
		b.identity == p.identity && b.manager == p.manager {
		b.state = leaseManagerBrokerEmpty
		b.identity = ""
		b.manager = nil
	}
	b.mu.Unlock()
	p.done = true
	return nil
}

// Acquire returns the one published manager and its exact storage-generation
// identity. An acquired pointer remains owned by the process generation; the
// broker does not provide a release operation that could remove GC protection.
func (b *LeaseManagerBroker) Acquire() (*LeaseManager, string, error) {
	if b == nil {
		return nil, "", moerr.NewInvalidStateNoCtx("substrait: TAE lease manager broker is unavailable")
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	switch b.state {
	case leaseManagerBrokerPublished:
		return b.manager, b.identity, nil
	case leaseManagerBrokerSealed:
		return nil, "", moerr.NewInvalidStateNoCtx("substrait: TAE lease manager broker is sealed")
	default:
		return nil, "", moerr.NewInvalidStateNoCtx("substrait: TAE lease manager is not published")
	}
}

// Seal prevents future publication or acquisition. It intentionally does not
// release a manager or its GC protections; the launcher owns CN drain followed
// by TAE shutdown ordering.
func (b *LeaseManagerBroker) Seal() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.state = leaseManagerBrokerSealed
	b.manager = nil
	b.mu.Unlock()
}
