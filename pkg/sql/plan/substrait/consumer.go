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
	"crypto/sha256"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ReadConsumer identifies the execution boundary that owns a protected TAE
// read. The zero value deliberately remains Flight so existing nonzero-SPKI
// admission callers retain their meaning while the embedded path opts in.
type ReadConsumer uint8

const (
	ReadConsumerFlight ReadConsumer = iota
	ReadConsumerEmbeddedTAE
)

func (c ReadConsumer) String() string {
	switch c {
	case ReadConsumerFlight:
		return "flight"
	case ReadConsumerEmbeddedTAE:
		return "embedded-tae"
	default:
		return "invalid"
	}
}

func inferReadConsumer(hash []byte) (ReadConsumer, error) {
	if len(hash) != sha256.Size {
		return 0, moerr.NewInternalErrorNoCtx("substrait: invalid read consumer marker")
	}
	for _, b := range hash {
		if b != 0 {
			return ReadConsumerFlight, nil
		}
	}
	return ReadConsumerEmbeddedTAE, nil
}

func consumerMarker(consumer ReadConsumer, clientSPKI []byte) ([]byte, error) {
	switch consumer {
	case ReadConsumerFlight:
		inferred, err := inferReadConsumer(clientSPKI)
		if err != nil || inferred != ReadConsumerFlight {
			return nil, moerr.NewInternalErrorNoCtx("substrait: Flight reads require a nonzero 32-byte client SPKI hash")
		}
		return append([]byte(nil), clientSPKI...), nil
	case ReadConsumerEmbeddedTAE:
		if len(clientSPKI) != 0 {
			inferred, err := inferReadConsumer(clientSPKI)
			if err != nil || inferred != ReadConsumerEmbeddedTAE {
				return nil, moerr.NewInternalErrorNoCtx("substrait: embedded TAE reads require the reserved zero consumer marker")
			}
		}
		return make([]byte, sha256.Size), nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid read consumer")
	}
}

func (m *LeaseManager) embeddedTAERead(readRef []byte) (AdmittedTAERead, bool) {
	if m == nil {
		return AdmittedTAERead{}, false
	}
	m.mu.RLock()
	lease := m.leases[string(readRef)]
	if lease == nil || lease.Read == nil || lease.Released ||
		m.releases[string(readRef)] != releaseNone ||
		lease.Consumer != ReadConsumerEmbeddedTAE {
		m.mu.RUnlock()
		return AdmittedTAERead{}, false
	}
	read := AdmittedTAERead{
		ReadRef:  append([]byte(nil), lease.Read.ReadRef...),
		Manifest: lease.Manifest, CanonicalSchema: lease.CanonicalSchema,
	}
	m.mu.RUnlock()
	return read, true
}

// ReconcileRestart applies the restart rule before a new runtime is
// published. An in-process embedded consumer cannot survive process death, so
// its stale protection is durably released. Flight work can outlive the CN and
// is returned to a Flight runtime for external quiescence; an embedded runtime
// must fail startup while any such work remains unreconciled.
func (m *LeaseManager) ReconcileRestart(ctx context.Context, active ReadConsumer) ([]PendingExecution, error) {
	if m == nil || (active != ReadConsumerFlight && active != ReadConsumerEmbeddedTAE) {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid restart reconciliation consumer")
	}
	cleanupCtx, cancel := leaseCleanupContext(ctx)
	defer cancel()
	if err := m.mutation.lock(cleanupCtx); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: acquire restart reconciliation mutation: %v", err)
	}
	defer m.mutation.unlock()

	m.mu.RLock()
	if !m.ready {
		m.mu.RUnlock()
		return nil, moerr.NewInternalErrorNoCtx("substrait: durable read leases have not been replayed")
	}
	embeddedKeys := make([]string, 0)
	for key, lease := range m.leases {
		if lease != nil && lease.Consumer == ReadConsumerEmbeddedTAE {
			embeddedKeys = append(embeddedKeys, key)
		}
	}
	m.mu.RUnlock()

	var releaseErr error
	for _, key := range embeddedKeys {
		if err := m.releaseLease(cleanupCtx, key); err != nil {
			releaseErr = errors.Join(releaseErr, err)
		}
	}
	if releaseErr != nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: release stale embedded TAE reads: %v", releaseErr)
	}

	pending := m.pendingExecutions(ReadConsumerFlight)
	if active == ReadConsumerEmbeddedTAE && len(pending) != 0 {
		return nil, moerr.NewInvalidStateNoCtx("substrait: embedded Sirius startup has unreconciled Flight reads")
	}
	return pending, nil
}
