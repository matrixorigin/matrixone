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

// Package clusteradmission serializes complete test-cluster lifecycles across
// test binaries sharing a runner.
package clusteradmission

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	lockFilename = "mo-test-cluster-lifecycle.lock"
	retryDelay   = 50 * time.Millisecond
	// ProcessPoolSizeEnv is set only by the bounded race-UT scheduler. A pool
	// uses shared admission plus one exclusive slot lock per process, so a
	// normal exclusive test still excludes the whole pool.
	ProcessPoolSizeEnv = "MO_TEST_CLUSTER_ADMISSION_POOL_SIZE"
)

var processAdmission = newManager(filepath.Join(os.TempDir(), lockFilename), retryDelay)

// Mode combines independent permissions for same-process cluster borrowing
// and bounded cross-process admission. Process scheduling never permits local
// overlap by itself; accidental overlap must fail fast.
type Mode uint8

const (
	Exclusive                Mode = 0
	AllowConcurrent          Mode = 1 << 0
	AllowConcurrentProcesses Mode = 1 << 1
)

// Lease represents one complete test cluster owned by the current process.
// Release is idempotent. The runner-wide lock is released after the process's
// last active lease is released, or automatically when the process exits.
type Lease struct {
	mu         sync.Mutex
	manager    *manager
	released   bool
	requested  time.Time
	acquired   time.Time
	releasedAt time.Time
}

// Timing describes one admission lease. WaitDuration is the time from the
// Acquire request until admission is granted; HoldDuration is the time from
// acquisition until Release. The values remain available after Release for
// test diagnostics.
type Timing struct {
	WaitDuration time.Duration
	HoldDuration time.Duration
	RequestedAt  time.Time
	AcquiredAt   time.Time
	ReleasedAt   time.Time
}

// Acquire waits until this test process has runner-wide admission. A second
// cluster in the same process is rejected unless the caller explicitly opts
// into AllowConcurrent for a test whose subject is multi-cluster behavior.
// AllowConcurrentProcesses uses a shared gate and one of a bounded set of
// slot locks, so ordinary Exclusive admission still excludes the whole pool.
// Combine both permissions for intentional borrowing in a pooled process.
// Borrowing retains the active lease's lock mode and does not acquire a slot.
func Acquire(ctx context.Context, mode Mode) (*Lease, error) {
	return processAdmission.acquire(ctx, mode)
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
	l.releasedAt = time.Now()
	l.released = true
	return nil
}

// Timing returns a race-safe snapshot of this lease's wait and hold times.
// Before Release, HoldDuration is measured up to the snapshot time.
func (l *Lease) Timing() Timing {
	if l == nil {
		return Timing{}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.timingLocked(time.Now())
}

func (l *Lease) timingLocked(now time.Time) Timing {
	timing := Timing{
		RequestedAt: l.requested,
		AcquiredAt:  l.acquired,
		ReleasedAt:  l.releasedAt,
	}
	if !l.requested.IsZero() && !l.acquired.IsZero() {
		timing.WaitDuration = l.acquired.Sub(l.requested)
	}
	if !l.acquired.IsZero() {
		releasedAt := l.releasedAt
		if releasedAt.IsZero() {
			releasedAt = now
		}
		timing.HoldDuration = releasedAt.Sub(l.acquired)
	}
	return timing
}

type manager struct {
	mu         sync.Mutex
	path       string
	retryDelay time.Duration
	lock       *flock.Flock
	gate       *flock.Flock
	references int
}

func newManager(path string, delay time.Duration) *manager {
	return &manager{path: path, retryDelay: delay}
}

func (m *manager) acquire(ctx context.Context, mode Mode) (*Lease, error) {
	if ctx == nil {
		return nil, moerr.NewInvalidInputNoCtx("cluster admission requires a context")
	}

	requested := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.references > 0 {
		if mode&AllowConcurrent == 0 {
			return nil, moerr.NewInvalidStateNoCtx("another complete test cluster is already active in this process")
		}
		m.references++
		return &Lease{manager: m, requested: requested, acquired: time.Now()}, nil
	}
	if mode&AllowConcurrentProcesses != 0 {
		return m.acquireFromProcessPool(ctx, requested)
	}

	lock := flock.New(m.path)
	// TryLockContext succeeds with a held lock or returns an error after the
	// dependency has cleaned up the unheld handle. Publish only a real owner.
	if _, err := lock.TryLockContext(ctx, m.retryDelay); err != nil {
		return nil, errors.Join(
			moerr.NewInternalErrorNoCtxf("acquire test cluster admission %s", m.path),
			err,
		)
	}
	m.lock = lock
	m.references = 1
	return &Lease{manager: m, requested: requested, acquired: time.Now()}, nil
}

func (m *manager) acquireFromProcessPool(ctx context.Context, requested time.Time) (*Lease, error) {
	poolSize, err := processPoolSize()
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Shared gate admission excludes ordinary Exclusive leases while
		// allowing the bounded pool members to coexist.
		gate := flock.New(m.path)
		locked, err := gate.TryRLock()
		if err != nil {
			return nil, errors.Join(
				moerr.NewInternalErrorNoCtxf("acquire test cluster admission gate %s", m.path),
				err,
			)
		}
		if !locked {
			_ = gate.Close()
			if err := waitAdmissionRetry(ctx, m.retryDelay); err != nil {
				return nil, err
			}
			continue
		}

		for slotIndex := 0; slotIndex < poolSize; slotIndex++ {
			slotPath := fmt.Sprintf("%s.slot.%d", m.path, slotIndex)
			slot := flock.New(slotPath)
			locked, err := slot.TryLock()
			if err != nil {
				_ = slot.Close()
				_ = gate.Close()
				return nil, errors.Join(
					moerr.NewInternalErrorNoCtxf("acquire test cluster admission slot %s", slotPath),
					err,
				)
			}
			if !locked {
				_ = slot.Close()
				continue
			}
			m.gate = gate
			m.lock = slot
			m.references = 1
			return &Lease{manager: m, requested: requested, acquired: time.Now()}, nil
		}

		_ = gate.Close()
		if err := waitAdmissionRetry(ctx, m.retryDelay); err != nil {
			return nil, err
		}
	}
}

func processPoolSize() (int, error) {
	value := os.Getenv(ProcessPoolSizeEnv)
	poolSize, err := strconv.Atoi(value)
	if err != nil || poolSize < 2 {
		return 0, moerr.NewInvalidInputNoCtxf(
			"%s must be an integer greater than one, got %q", ProcessPoolSizeEnv, value)
	}
	return poolSize, nil
}

func waitAdmissionRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
	releaseErr := m.lock.Close()
	if m.gate != nil {
		releaseErr = errors.Join(releaseErr, m.gate.Close())
	}
	if releaseErr != nil {
		return errors.Join(
			moerr.NewInternalErrorNoCtxf("release test cluster admission %s", m.path),
			releaseErr,
		)
	}
	m.lock = nil
	m.gate = nil
	m.references = 0
	return nil
}
