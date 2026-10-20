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

package system

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var (
	ErrMemoryStatementQueueFull = moerr.NewInternalErrorNoCtx("memory statement admission queue full")
	ErrMemoryStatementTimeout   = moerr.NewInternalErrorNoCtx("memory statement admission wait timed out")
)

// MemoryStatementAdmissionError retains the last failed decision, not a later
// diagnostic resample. Unwrap preserves timeout/queue-full classification.
type MemoryStatementAdmissionError struct {
	Cause               error
	Snapshot            MemoryAdmissionBudgetSnapshot
	NonGoHighWater      uint64
	RawAvailable        uint64
	WorkingSetAvailable uint64
	UnderPressure       bool
	PressureConcurrency uint64
	Control             bool
	ControlAvailable    uint64
	OrdinaryReason      string
	ControlReason       string
}

func (e *MemoryStatementAdmissionError) Error() string {
	s := e.Snapshot
	return fmt.Sprintf("%v: source=%s measured=%t used=%d limit=%d soft_limit=%d capacity=%d non_go=%d non_go_high_water=%d reserve=%d reserved=%d available=%d reclaimable=%d raw_available=%d ws_available=%d pressure=%t active=%d concurrency=%d queued=%d ordinary_reason=%s control=%t control_active=%d control_reserved=%d control_available=%d control_reason=%s observed_at=%s",
		e.Cause, s.Domain.Source, s.Measured, s.Domain.Used, s.Domain.Limit, s.Domain.SoftLimit, s.Domain.CapacityLimit(),
		s.Domain.NonGo, e.NonGoHighWater, s.Reserve, s.Reserved, s.Available,
		s.Domain.Reclaimable, e.RawAvailable, e.WorkingSetAvailable, e.UnderPressure, s.StatementActive, e.PressureConcurrency,
		s.StatementQueued, e.OrdinaryReason, e.Control, s.ControlActive, s.ControlReserved, e.ControlAvailable, e.ControlReason,
		s.Domain.ObservedAt.Format(time.RFC3339Nano))
}

func (e *MemoryStatementAdmissionError) Unwrap() error { return e.Cause }

func (e MemoryStatementAdmissionError) withCause(cause error) error {
	e.Cause = cause
	return &e
}

func (b *MemoryAdmissionBudget) statementRejectionLocked(snapshot MemoryDomainSnapshot, available, pressureAvailable, concurrency uint64, atHead, control bool) MemoryStatementAdmissionError {
	d := MemoryStatementAdmissionError{Snapshot: b.snapshotLocked(snapshot, available), NonGoHighWater: b.nonGoHighWater,
		WorkingSetAvailable: pressureAvailable,
		PressureConcurrency: concurrency, Control: control, OrdinaryReason: "pressure-concurrency", ControlReason: "not-requested"}
	d.UnderPressure = pressureAvailable < d.Snapshot.Reserve
	if snapshot.Measured && snapshot.AdmissionLimit() > 0 {
		// This is the cache-blind gate's headroom, with its runtime floor and
		// every outstanding claim deducted, not raw limit-minus-usage.
		d.RawAvailable = b.headroomLocked(snapshot, snapshot.Used, min(d.Snapshot.Reserve, b.policy.RuntimeBytes))
	}
	switch {
	case !snapshot.Measured || snapshot.AdmissionLimit() == 0:
		d.OrdinaryReason = "domain-unmeasured"
	case available == 0:
		d.OrdinaryReason = "no-headroom"
	case !atHead:
		d.OrdinaryReason = "queue-order"
	}
	if control {
		switch {
		case !snapshot.Measured || snapshot.AdmissionLimit() == 0:
			d.ControlReason = "domain-unmeasured"
		case b.controlActive >= memoryControlConcurrency:
			d.ControlReason = "slots-exhausted"
		case b.controlLimitLocked(snapshot)/memoryControlConcurrency == 0:
			d.ControlReason = "disabled"
		default:
			// A non-owning view: no slot is acquired or released for diagnostics.
			view := MemoryControlLease{budget: b, limit: b.controlLimitLocked(snapshot) / memoryControlConcurrency}
			d.ControlAvailable, _ = view.availableLocked(snapshot)
			d.ControlReason = "no-headroom"
		}
	}
	return d
}

// Count terminal rejections once, not every poll or wakeup while waiting. The
// bounded reason labels retain the failed decision even when timeout or queue
// overflow is the client-visible error. Context termination is explicit.
func (b *MemoryAdmissionBudget) rejectStatementLocked(reason string) {
	b.statementRejected++
	metric.MemoryStatementRejectedCounter.WithLabelValues(reason).Inc()
}

// MemoryStatementAdmissionPolicy bounds waiting, not the bytes a statement may
// allocate. Healthy work is unrestricted. At less than one calibrated reserve
// of working-set headroom, PressureConcurrency bounds new in-flight statements.
// Raw charged headroom remains an independent requirement for entry/allocation.
// Zero concurrency derives the limit from the current Go scheduler capacity.
type MemoryStatementAdmissionPolicy struct {
	PressureConcurrency uint64
	MaxQueued           uint64
	WaitTimeout         time.Duration
}

func defaultMemoryStatementAdmissionPolicy() MemoryStatementAdmissionPolicy {
	p := MemoryStatementAdmissionPolicy{MaxQueued: 1024, WaitTimeout: 250 * time.Millisecond}
	if n, ok := memoryUintFromEnv("MO_MEMORY_STATEMENT_PRESSURE_CONCURRENCY"); ok {
		p.PressureConcurrency = n
	}
	if n, ok := memoryUintFromEnv("MO_MEMORY_STATEMENT_MAX_QUEUED"); ok {
		p.MaxQueued = n
	}
	if d, err := time.ParseDuration(os.Getenv("MO_MEMORY_STATEMENT_WAIT_TIMEOUT")); err == nil && d >= 0 {
		p.WaitTimeout = d
	}
	return p
}

type memoryStatementContextKey struct{}

type memoryStatementWaiter struct {
	wake chan struct{}
}

// MemoryStatementLease owns one statement admission. It never precharges a
// query cap or a fixed entry floor: physical allocations still acquire bytes
// from the same ledger. A control lease, when needed, is also used by execution.
type MemoryStatementLease struct {
	budget  *MemoryAdmissionBudget
	control *MemoryControlLease
	closed  bool // protected by budget.mu
}

// EnterStatement is the pre-execution gate. Ordinary waiters are FIFO, bounded
// and cancellable; narrowly classified control work can use the existing
// bounded control reserve instead of waiting behind data queries. A nested SQL
// statement borrows a live admission in its context, never acquiring twice.
func (b *MemoryAdmissionBudget) EnterStatement(ctx context.Context, control bool) (context.Context, *MemoryStatementLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if b == nil {
		return ctx, nil, ErrMemoryStatementQueueFull
	}
	if err := ctx.Err(); err != nil {
		return ctx, nil, err
	}
	if parent, _ := ctx.Value(memoryStatementContextKey{}).(*MemoryStatementLease); parent != nil && parent.budget == b {
		b.mu.Lock()
		live := !parent.closed
		b.mu.Unlock()
		if live {
			return ctx, nil, nil
		}
	}
	var element *list.Element
	var waiter *memoryStatementWaiter
	var timer *time.Timer
	var ticker *time.Ticker
	var expires time.Time
	var rejection MemoryStatementAdmissionError
	defer func() {
		if timer != nil {
			timer.Stop()
			ticker.Stop()
		}
	}()
	for {
		snapshot, _ := b.domainProvider()
		b.mu.Lock()
		if err := ctx.Err(); err != nil {
			b.removeStatementWaiterLocked(element)
			reason := "context-canceled"
			if err == context.DeadlineExceeded {
				reason = "deadline-exceeded"
			}
			b.rejectStatementLocked(reason)
			b.mu.Unlock()
			return ctx, nil, err
		}
		if element != nil && !time.Now().Before(expires) {
			b.removeStatementWaiterLocked(element)
			b.rejectStatementLocked(rejection.OrdinaryReason)
			b.mu.Unlock()
			return ctx, nil, rejection.withCause(ErrMemoryStatementTimeout)
		}
		available := b.availableLocked(snapshot)
		pressureAvailable := b.availableForPressureLocked(snapshot)
		concurrency := b.statementPolicy.PressureConcurrency
		if concurrency == 0 {
			concurrency = uint64(max(1, GoMaxProcs()))
		}
		reserveSample := snapshot
		reserveSample.NonGo = b.nonGoHighWater
		underPressure := pressureAvailable < b.policy.Reserve(reserveSample)
		atHead := b.statementQueue.Len() == 0 || element != nil && b.statementQueue.Front() == element
		if snapshot.Measured && available > 0 && atHead && (!underPressure || b.statementActive < concurrency) {
			b.removeStatementWaiterLocked(element)
			lease := &MemoryStatementLease{budget: b}
			b.statementActive++
			b.statementAdmitted++
			b.mu.Unlock()
			return context.WithValue(ctx, memoryStatementContextKey{}, lease), lease, nil
		}
		if control {
			if allowance, ok := b.acquireControlLocked(snapshot); ok {
				if left, valid := allowance.availableLocked(snapshot); valid && left > 0 {
					b.removeStatementWaiterLocked(element)
					lease := &MemoryStatementLease{budget: b, control: allowance}
					b.statementAdmitted++
					b.mu.Unlock()
					return context.WithValue(ctx, memoryStatementContextKey{}, lease), lease, nil
				}
				allowance.closed = true
				allowance.releaseSlotLocked()
			}
		}
		rejection = b.statementRejectionLocked(snapshot, available, pressureAvailable, concurrency, atHead, control)
		if element == nil {
			if uint64(b.statementQueue.Len()) >= b.statementPolicy.MaxQueued {
				b.rejectStatementLocked(rejection.OrdinaryReason)
				b.mu.Unlock()
				return ctx, nil, rejection.withCause(ErrMemoryStatementQueueFull)
			}
			if b.statementPolicy.WaitTimeout <= 0 {
				b.rejectStatementLocked(rejection.OrdinaryReason)
				b.mu.Unlock()
				return ctx, nil, rejection.withCause(ErrMemoryStatementTimeout)
			}
			waiter = &memoryStatementWaiter{wake: make(chan struct{}, 1)}
			element = b.statementQueue.PushBack(waiter)
			expires = time.Now().Add(b.statementPolicy.WaitTimeout)
			timer = time.NewTimer(b.statementPolicy.WaitTimeout)
			// External cgroup changes have no in-process Release notification.
			ticker = time.NewTicker(memoryDomainRefreshInterval)
		}
		b.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-waiter.wake:
		case <-ticker.C:
		case <-timer.C:
			b.mu.Lock()
			b.removeStatementWaiterLocked(element)
			b.rejectStatementLocked(rejection.OrdinaryReason)
			b.mu.Unlock()
			return ctx, nil, rejection.withCause(ErrMemoryStatementTimeout)
		}
	}
}

func (b *MemoryAdmissionBudget) removeStatementWaiterLocked(element *list.Element) {
	if element != nil {
		b.statementQueue.Remove(element)
	}
	b.notifyStatementsLocked()
}

func (b *MemoryAdmissionBudget) notifyStatementsLocked() {
	if head := b.statementQueue.Front(); head != nil {
		select {
		case head.Value.(*memoryStatementWaiter).wake <- struct{}{}:
		default:
		}
	}
}

func (l *MemoryStatementLease) Close() {
	if l == nil {
		return
	}
	b := l.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	if l.control != nil {
		l.control.closed = true
		l.control.releaseSlotLocked()
	} else {
		b.statementActive--
	}
	b.notifyStatementsLocked()
}

// StatementControlMemory returns only a live control allowance for this owner.
// Execution borrows it; it must not open a second control slot for the same SQL.
func (b *MemoryAdmissionBudget) StatementControlMemory(ctx context.Context) *MemoryControlLease {
	if b == nil || ctx == nil {
		return nil
	}
	l, _ := ctx.Value(memoryStatementContextKey{}).(*MemoryStatementLease)
	if l == nil || l.budget != b {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.closed {
		return nil
	}
	return l.control
}
