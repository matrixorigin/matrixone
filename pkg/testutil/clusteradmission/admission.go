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
)

// ParticipantEnv is set only by the bounded embedded test wave. Other test
// processes retain exclusive admission on the same global lifecycle gate.
const ParticipantEnv = "MO_UT_CLUSTER_PARTICIPANT"

var processAdmission = newProcessManager()

func newProcessManager() *manager {
	m := newManager(filepath.Join(os.TempDir(), lockFilename), retryDelay)
	switch value := os.Getenv(ParticipantEnv); value {
	case "":
	case "1":
		m.participant = true
	default:
		m.configErr = moerr.NewInvalidInputNoCtxf("%s must be empty or 1, got %q", ParticipantEnv, value)
	}
	return m
}

// Mode controls whether a test deliberately starts another complete cluster in
// the same test process. The default must be Exclusive: accidental overlap is
// otherwise invisible to the runner-wide file lock and can starve HAKeeper.
type Mode uint8

const (
	Exclusive Mode = iota
	AllowConcurrent
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
	mu          sync.Mutex
	path        string
	retryDelay  time.Duration
	lock        *flock.Flock
	references  int
	participant bool
	configErr   error
	slot        *flock.Flock
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
	if m.configErr != nil {
		return nil, m.configErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.references > 0 {
		if mode != AllowConcurrent {
			return nil, moerr.NewInvalidStateNoCtx("another complete test cluster is already active in this process")
		}
		if m.lock == nil || (!m.participant && !m.lock.Locked()) || (m.participant && (m.slot == nil || !m.slot.Locked() || !m.lock.RLocked())) {
			return nil, moerr.NewInvalidStateNoCtx("test cluster admission cleanup is incomplete")
		}
		m.references++
		return &Lease{manager: m, requested: requested, acquired: time.Now()}, nil
	}

	if err := m.acquireHandles(ctx); err != nil {
		cleanupErr := m.closeHandles()
		cause := errors.Join(moerr.NewInternalErrorNoCtxf("acquire test cluster admission %s", m.path), err, cleanupErr)
		if cleanupErr != nil {
			// Return the remaining cleanup owner even though no usable admission
			// was granted. The cluster must retain this lease and retry Release.
			m.references = 1
			return &Lease{manager: m, requested: requested}, cause
		}
		return nil, cause
	}
	m.references = 1
	return &Lease{manager: m, requested: requested, acquired: time.Now()}, nil
}

func (m *manager) acquireHandles(ctx context.Context) error {
	for {
		m.lock = flock.New(m.path)
		if !m.participant {
			locked, err := m.lock.TryLockContext(ctx, m.retryDelay)
			if err != nil {
				return err
			}
			if !locked {
				return moerr.NewInvalidStateNoCtxf("test cluster admission %s was not acquired", m.path)
			}
			return nil
		}
		if _, err := m.lock.TryRLockContext(ctx, m.retryDelay); err != nil {
			return err
		}
		for index := 0; index < 2; index++ {
			m.slot = flock.New(m.path + ".slot-" + strconv.Itoa(index))
			locked, err := m.slot.TryLock()
			if err != nil {
				return err
			}
			if locked {
				return nil
			}
			if err := m.slot.Close(); err != nil {
				return err
			}
			m.slot = nil
		}
		// A waiter without a slot must not keep the shared gate and prevent an
		// ordinary exclusive owner from making progress.
		if err := m.closeHandles(); err != nil {
			return err
		}
		timer := time.NewTimer(m.retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *manager) closeHandles() error {
	if m.slot != nil {
		if err := m.slot.Close(); err != nil {
			return err
		}
		m.slot = nil
	}
	if m.lock != nil {
		if err := m.lock.Close(); err != nil {
			return err
		}
		m.lock = nil
	}
	return nil
}

func (m *manager) release() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.references <= 0 || (m.lock == nil && m.slot == nil) {
		return moerr.NewInvalidStateNoCtxf("test cluster admission %s has no active lease", m.path)
	}
	if m.references > 1 {
		m.references--
		return nil
	}
	if err := m.closeHandles(); err != nil {
		return errors.Join(
			moerr.NewInternalErrorNoCtxf("release test cluster admission %s", m.path),
			err,
		)
	}
	m.references = 0
	return nil
}
