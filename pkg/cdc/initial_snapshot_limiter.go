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

package cdc

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/system"
)

const (
	initialSnapshotMinConcurrency = 1
	initialSnapshotMaxConcurrency = 8
	initialSnapshotMemoryFraction = 4
	initialSnapshotBatchEstimate  = 256 * mpool.MB
	initialSnapshotMemoryPoll     = 200 * time.Millisecond
)

type memoryAvailableFunc func() (uint64, bool)

// InitialSnapshotLimiter adapts the number of retained snapshot batches to
// cgroup-aware memory headroom. It keeps a hard concurrency bound and falls
// back to the historical concurrency when memory cannot be measured. Memory
// admission is estimate-based because the next engine batch size is unknown.
type InitialSnapshotLimiter struct {
	mu sync.Mutex

	inFlight int
	// unobserved is either zero or one. A newly admitted collector must report
	// its real allocation (or release on failure) before another collector may
	// allocate, so a stale estimate can never grant a burst of unknown batches.
	unobserved          int
	waiters             int
	nextTicket          uint64
	servingTicket       uint64
	canceledTickets     map[uint64]struct{}
	batchBytesEstimate  uint64
	minConcurrency      int
	maxConcurrency      int
	fallbackConcurrency int
	memoryAvailable     memoryAvailableFunc
	notify              chan struct{}
}

func NewInitialSnapshotLimiter() *InitialSnapshotLimiter {
	return newInitialSnapshotLimiter(
		initialSnapshotMinConcurrency,
		initialSnapshotMaxConcurrency,
		CDCDefaultInitialSnapshotConcurrency,
		initialSnapshotBatchEstimate,
		system.MemoryAvailableIncludingCache,
	)
}

func newInitialSnapshotLimiter(
	minConcurrency int,
	maxConcurrency int,
	fallbackConcurrency int,
	batchBytesEstimate uint64,
	memoryAvailable memoryAvailableFunc,
) *InitialSnapshotLimiter {
	return &InitialSnapshotLimiter{
		batchBytesEstimate:  batchBytesEstimate,
		minConcurrency:      minConcurrency,
		maxConcurrency:      maxConcurrency,
		fallbackConcurrency: fallbackConcurrency,
		memoryAvailable:     memoryAvailable,
		notify:              make(chan struct{}),
		canceledTickets:     make(map[uint64]struct{}),
	}
}

func (l *InitialSnapshotLimiter) concurrencyLocked(available uint64, measured bool) int {
	if !measured || l.batchBytesEstimate == 0 {
		return min(max(l.fallbackConcurrency, l.minConcurrency), l.maxConcurrency)
	}

	// Derive the concurrency target so estimated retained CDC snapshot batches
	// use at most one quarter of current cgroup/host headroom. This is not a byte
	// reservation: one indivisible engine batch can exceed the estimate.
	concurrency := available / initialSnapshotMemoryFraction / l.batchBytesEstimate
	if concurrency <= uint64(l.minConcurrency) {
		return l.minConcurrency
	}
	if concurrency >= uint64(l.maxConcurrency) {
		return l.maxConcurrency
	}
	return int(concurrency)
}

func (l *InitialSnapshotLimiter) acquire(ctx context.Context) (*snapshotPermit, error) {
	l.mu.Lock()
	ticket := l.nextTicket
	l.nextTicket++
	l.waiters++
	l.mu.Unlock()

	ticker := time.NewTicker(initialSnapshotMemoryPoll)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			l.cancelTicket(ticket)
			return nil, err
		}

		l.mu.Lock()
		if ticket != l.servingTicket {
			notify := l.notify
			l.mu.Unlock()
			select {
			case <-ctx.Done():
			case <-notify:
			}
			continue
		}
		l.mu.Unlock()

		// Memory discovery may touch procfs/cgroupfs. Keep it outside the mutex so
		// Release remains an independent, fast control path. Only the FIFO head
		// performs this measurement, avoiding one procfs poll per waiting table.
		available, measured := l.memoryAvailable()
		l.mu.Lock()
		if err := ctx.Err(); err != nil {
			l.mu.Unlock()
			l.cancelTicket(ticket)
			return nil, err
		}
		if ticket == l.servingTicket &&
			l.unobserved == 0 &&
			l.inFlight < l.concurrencyLocked(available, measured) {
			l.inFlight++
			l.unobserved++
			l.waiters--
			l.servingTicket++
			l.advanceCanceledTicketsLocked()
			l.signalLocked()
			l.mu.Unlock()
			return &snapshotPermit{
				release: l.release,
				observe: l.observeBatchBytes,
			}, nil
		}
		notify := l.notify
		l.mu.Unlock()

		select {
		case <-ctx.Done():
		case <-notify:
		case <-ticker.C:
		}
	}
}

// tryAcquire admits without joining the FIFO. It is used only by a stream that
// already owns a staged snapshot group: if no slot is immediately available,
// that stream commits its partial group and releases its existing permits before
// entering the ordinary FIFO. Never bypass an existing waiter.
func (l *InitialSnapshotLimiter) tryAcquire() (*snapshotPermit, bool) {
	l.mu.Lock()
	if l.waiters != 0 || l.unobserved != 0 {
		l.mu.Unlock()
		return nil, false
	}
	l.mu.Unlock()

	// Match acquire: memory discovery can touch procfs/cgroupfs and must not hold
	// the limiter mutex needed by permit release.
	available, measured := l.memoryAvailable()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.waiters != 0 || l.unobserved != 0 ||
		l.inFlight >= l.concurrencyLocked(available, measured) {
		return nil, false
	}
	l.inFlight++
	l.unobserved++
	return &snapshotPermit{
		release: l.release,
		observe: l.observeBatchBytes,
	}, true
}

func (l *InitialSnapshotLimiter) cancelTicket(ticket uint64) {
	l.mu.Lock()
	l.canceledTickets[ticket] = struct{}{}
	l.waiters--
	l.advanceCanceledTicketsLocked()
	l.signalLocked()
	l.mu.Unlock()
}

func (l *InitialSnapshotLimiter) advanceCanceledTicketsLocked() {
	for {
		if _, ok := l.canceledTickets[l.servingTicket]; !ok {
			return
		}
		delete(l.canceledTickets, l.servingTicket)
		l.servingTicket++
	}
}

func (l *InitialSnapshotLimiter) release(observed bool) {
	l.mu.Lock()
	if l.inFlight == 0 {
		l.mu.Unlock()
		panic("cdc: initial snapshot limiter released without acquisition")
	}
	if !observed {
		if l.unobserved == 0 {
			l.mu.Unlock()
			panic("cdc: initial snapshot limiter released an unknown observed permit")
		}
		l.unobserved--
	}
	l.inFlight--
	l.signalLocked()
	l.mu.Unlock()
}

func (l *InitialSnapshotLimiter) observeBatchBytes(bytes uint64) {
	l.mu.Lock()
	if l.unobserved == 0 {
		l.mu.Unlock()
		panic("cdc: initial snapshot limiter observed without acquisition")
	}
	l.unobserved--
	if bytes > l.batchBytesEstimate {
		// React to unexpectedly wide batches immediately.
		l.batchBytesEstimate = bytes
	} else {
		// Reduce conservatively so a run of narrow tables does not make one wide
		// table admit too many batches at once.
		l.batchBytesEstimate -= (l.batchBytesEstimate - bytes) / 4
	}
	l.signalLocked()
	l.mu.Unlock()
}

func (l *InitialSnapshotLimiter) signalLocked() {
	close(l.notify)
	l.notify = make(chan struct{})
}
