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

// Package clusteradmission serializes complete in-process test clusters across
// test binaries sharing a runner. A process may acquire more than one lease so
// tests that intentionally run multiple clusters together keep working.
package clusteradmission

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	lockFilename = "mo-test-cluster-lifecycle.lock"
	retryDelay   = 50 * time.Millisecond
)

var processAdmission = newManager(
	filepath.Join(os.TempDir(), lockFilename),
	retryDelay,
)

// Lease represents one complete test cluster owned by the current process.
// Release is idempotent. The runner-wide lock is released after the process's
// last active lease is released, or automatically when the process exits.
type Lease struct {
	mu       sync.Mutex
	manager  *manager
	released bool
}

// Acquire waits until this test process has exclusive runner-wide admission.
// Further acquisitions in the same process are reentrant.
func Acquire(ctx context.Context) (*Lease, error) {
	return processAdmission.acquire(ctx)
}

// Release relinquishes this cluster's share of the process admission.
func (l *Lease) Release() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	if err := l.manager.release(); err != nil {
		return err
	}
	l.released = true
	return nil
}

type manager struct {
	mu         sync.Mutex
	path       string
	retryDelay time.Duration
	lock       *flock.Flock
	references int
}

func newManager(path string, delay time.Duration) *manager {
	return &manager{path: path, retryDelay: delay}
}

func (m *manager) acquire(ctx context.Context) (*Lease, error) {
	if ctx == nil {
		return nil, moerr.NewInvalidInputNoCtx("cluster admission requires a context")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.references > 0 {
		m.references++
		return &Lease{manager: m}, nil
	}

	lock := flock.New(m.path)
	locked, err := lock.TryLockContext(ctx, m.retryDelay)
	if err != nil {
		return nil, errors.Join(
			moerr.NewInternalErrorNoCtxf("acquire test cluster admission %s", m.path),
			err,
			lock.Close(),
		)
	}
	if !locked {
		return nil, errors.Join(
			moerr.NewInvalidStateNoCtxf("test cluster admission %s was not acquired", m.path),
			lock.Close(),
		)
	}
	m.lock = lock
	m.references = 1
	return &Lease{manager: m}, nil
}

func (m *manager) release() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.references <= 0 || m.lock == nil {
		return moerr.NewInvalidStateNoCtxf("test cluster admission %s has no active lease", m.path)
	}
	if m.references > 1 {
		m.references--
		return nil
	}
	if err := m.lock.Close(); err != nil {
		return errors.Join(
			moerr.NewInternalErrorNoCtxf("release test cluster admission %s", m.path),
			err,
		)
	}
	m.lock = nil
	m.references = 0
	return nil
}
