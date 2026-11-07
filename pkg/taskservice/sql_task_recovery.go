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

package taskservice

import (
	"container/list"
	"context"
	"strings"
	"sync"
	"time"

	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
)

type sqlTaskReceiptClass uint8

const (
	sqlTaskManualReceipt sqlTaskReceiptClass = iota
	sqlTaskScheduledReceipt
	sqlTaskReceiptClasses

	sqlTaskManualCapacity = 4096
	sqlTaskNameCapacity   = 8
	sqlTaskMaxCapacity    = 16384
	// VARCHAR limits are in characters; retained UTF-8 strings are bounded in bytes.
	sqlTaskRunnerMaxBytes      = 128 * 4
	sqlTaskAsyncRunnerMaxBytes = 50 * 4
)

var sqlTaskPendingGauges = [sqlTaskReceiptClasses]prometheus.Gauge{
	v2.SQLTaskManualPendingGauge, v2.SQLTaskScheduledPendingGauge,
}

// A receipt is metadata only: it must never retain an SQL body, session,
// execution context, or unrestricted error chain.
type sqlTaskReceipt struct {
	class   sqlTaskReceiptClass
	run     SQLTaskRun
	claim   sqlTaskAsyncClaim
	kind    sqlTaskReceiptKind
	nextRun SQLTaskRun
	credit  *sqlTaskCredit
	endAt   int64
}

type sqlTaskReceiptKind uint8

const (
	sqlTaskReceiptTerminal sqlTaskReceiptKind = iota
	sqlTaskReceiptUnknown
	sqlTaskReceiptAdmission
	sqlTaskReceiptRetryAdmission
)

// Only bounded identifiers from the async row are retained for recovery.
type sqlTaskAsyncClaim struct {
	id       uint64
	epoch    uint32
	runner   string
	parent   string
	metadata string
}

type sqlTaskNameKey struct {
	account uint32
	name    string
}

type sqlTaskGuardKey struct {
	kind    uint8
	account uint32
	value   string
}

type sqlTaskCredit struct {
	class    sqlTaskReceiptClass
	name     sqlTaskNameKey
	released bool // protected by sqlTaskRecovery.mu
	queued   bool // protected by sqlTaskRecovery.mu
}

type sqlTaskGuard struct{ key sqlTaskGuardKey }

type sqlTaskManualGuardContextKey struct{}

type sqlTaskManualGuard struct {
	owner *sqlTaskRecovery
	guard *sqlTaskGuard
}

// BeginSQLTaskManualAdmission bounds concurrent requests for one tenant and
// name before the frontend reads the definition. The returned release is safe
// after the executor has already released the same guard.
func BeginSQLTaskManualAdmission(ctx context.Context, service TaskService, account uint32, name string) (context.Context, func(), error) {
	s := sqlTaskServiceOwner(service)
	if s == nil {
		return ctx, nil, ErrNotReady
	}
	guard := s.sqlRecovery.tryGuard(sqlTaskGuardKey{account: account, value: name}, 512)
	if guard == nil {
		return ctx, nil, ErrSQLTaskRecoveryCapacity
	}
	owned := &sqlTaskManualGuard{owner: s.sqlRecovery, guard: guard}
	return context.WithValue(ctx, sqlTaskManualGuardContextKey{}, owned), func() { owned.owner.releaseGuard(owned.guard) }, nil
}

// sqlTaskRecovery owns bounded admission credits and one lazily started
// worker. The processor may perform metadata I/O but is never called with mu
// held; it must not run user SQL.
type sqlTaskRecovery struct {
	mu       sync.Mutex
	capacity [sqlTaskReceiptClasses]int
	used     [sqlTaskReceiptClasses]int
	names    map[sqlTaskNameKey][2]int
	guards   map[sqlTaskGuardKey]*sqlTaskGuard
	pending  [sqlTaskReceiptClasses]list.List
	rotate   sqlTaskReceiptClass
	closed   bool
	started  bool

	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	process func(context.Context, *sqlTaskReceipt) bool
}

func newSQLTaskRecovery(manualCapacity, scheduledCapacity int, process func(context.Context, *sqlTaskReceipt) bool) *sqlTaskRecovery {
	ctx, cancel := context.WithCancel(context.Background())
	manualCapacity = min(max(manualCapacity, 16), sqlTaskMaxCapacity)
	scheduledCapacity = min(max(scheduledCapacity, 9), sqlTaskMaxCapacity)
	return &sqlTaskRecovery{
		capacity: [sqlTaskReceiptClasses]int{manualCapacity, scheduledCapacity},
		names:    make(map[sqlTaskNameKey][2]int),
		guards:   make(map[sqlTaskGuardKey]*sqlTaskGuard),
		ctx:      ctx,
		cancel:   cancel,
		process:  process,
	}
}

func sqlTaskScheduledCapacity(parallelism int) int {
	if parallelism < 1 {
		parallelism = 1
	}
	// Clamp before addition to avoid integer overflow from configuration.
	if parallelism >= sqlTaskMaxCapacity/2 {
		return sqlTaskMaxCapacity
	}
	return min(parallelism+max(parallelism, 8), sqlTaskMaxCapacity)
}

func (r *sqlTaskRecovery) setScheduledCapacity(parallelism int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capacity[sqlTaskScheduledReceipt] = max(r.used[sqlTaskScheduledReceipt], sqlTaskScheduledCapacity(parallelism))
}

// The caller holds the authoritative definition lock while reserving a named
// credit. A busy or full share is rejected before run insertion or SQL.
func (r *sqlTaskRecovery) reserve(class sqlTaskReceiptClass, account uint32, name string) *sqlTaskCredit {
	if class >= sqlTaskReceiptClasses || name == "" || len(name) > 512 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.used[class] >= r.capacity[class] {
		v2.SQLTaskReceiptCapacityCounter.Inc()
		return nil
	}
	key := sqlTaskNameKey{account: account, name: name}
	counts := r.names[key]
	if counts[class] >= sqlTaskNameCapacity {
		v2.SQLTaskReceiptCapacityCounter.Inc()
		return nil
	}
	key.name = strings.Clone(name)
	counts[class]++
	r.names[key] = counts
	r.used[class]++
	return &sqlTaskCredit{class: class, name: key}
}

func (r *sqlTaskRecovery) releaseLocked(credit *sqlTaskCredit) {
	if credit == nil || credit.released {
		return
	}
	credit.released = true
	r.used[credit.class]--
	if credit.queued {
		sqlTaskPendingGauges[credit.class].Dec()
	}
	counts := r.names[credit.name]
	counts[credit.class]--
	if counts[0] == 0 && counts[1] == 0 {
		delete(r.names, credit.name)
	} else {
		r.names[credit.name] = counts
	}
}

func (r *sqlTaskRecovery) release(credit *sqlTaskCredit) {
	r.mu.Lock()
	if credit != nil && !credit.queued {
		r.releaseLocked(credit)
	}
	r.mu.Unlock()
}

// tryGuard is a nonblocking resource guard, not authority to execute SQL.
// Keys are bounded before cloning so user inputs cannot retain large buffers.
func (r *sqlTaskRecovery) tryGuard(key sqlTaskGuardKey, maxBytes int) *sqlTaskGuard {
	if key.value == "" || len(key.value) > maxBytes {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || len(r.guards) >= r.capacity[0]+r.capacity[1]+1 {
		return nil
	}
	if _, exists := r.guards[key]; exists {
		return nil
	}
	key.value = strings.Clone(key.value)
	guard := &sqlTaskGuard{key: key}
	r.guards[key] = guard
	return guard
}

func (r *sqlTaskRecovery) releaseGuard(guard *sqlTaskGuard) {
	if guard == nil {
		return
	}
	r.mu.Lock()
	if r.guards[guard.key] == guard {
		delete(r.guards, guard.key)
	}
	r.mu.Unlock()
}

// enqueue transfers a reserved credit and starts the one service-owned worker
// on first use. The caller must retain its credit if enqueue fails.
func (r *sqlTaskRecovery) enqueue(receipt *sqlTaskReceipt) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || receipt.credit == nil || receipt.credit.released || receipt.credit.queued || receipt.credit.class != receipt.class {
		return false
	}
	if len(receipt.run.TaskName) > 512 || len(receipt.run.RunnerCN) > sqlTaskRunnerMaxBytes || len(receipt.run.ErrorMessage) > 4096 ||
		len(receipt.nextRun.TaskName) > 512 || len(receipt.nextRun.RunnerCN) > sqlTaskRunnerMaxBytes || len(receipt.nextRun.ErrorMessage) > 4096 ||
		len(receipt.claim.runner) > sqlTaskAsyncRunnerMaxBytes || len(receipt.claim.parent) > 50 || len(receipt.claim.metadata) > 50 {
		return false
	}
	receipt.credit.queued = true
	sqlTaskPendingGauges[receipt.class].Inc()
	receipt.run.TaskName, receipt.run.RunnerCN, receipt.run.ErrorMessage = strings.Clone(receipt.run.TaskName), strings.Clone(receipt.run.RunnerCN), strings.Clone(receipt.run.ErrorMessage)
	receipt.nextRun.TaskName, receipt.nextRun.RunnerCN, receipt.nextRun.ErrorMessage = strings.Clone(receipt.nextRun.TaskName), strings.Clone(receipt.nextRun.RunnerCN), strings.Clone(receipt.nextRun.ErrorMessage)
	receipt.claim.runner, receipt.claim.parent, receipt.claim.metadata = strings.Clone(receipt.claim.runner), strings.Clone(receipt.claim.parent), strings.Clone(receipt.claim.metadata)
	r.pending[receipt.class].PushBack(receipt)
	if !r.started {
		r.started = true
		r.done = make(chan struct{})
		go r.run(r.done)
	}
	return true
}

func (r *sqlTaskRecovery) beginBatch() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0
	}
	total := 0
	for class := range r.pending {
		total += r.pending[class].Len()
	}
	if total == 0 {
		r.started = false
	}
	return min(total, 32)
}

func (r *sqlTaskRecovery) take() *sqlTaskReceipt {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	for n := sqlTaskReceiptClass(0); n < sqlTaskReceiptClasses; n++ {
		class := (r.rotate + n) % sqlTaskReceiptClasses
		queue := &r.pending[class]
		element := queue.Front()
		if element == nil {
			continue
		}
		r.rotate = (class + 1) % sqlTaskReceiptClasses
		return queue.Remove(element).(*sqlTaskReceipt)
	}
	return nil
}

func (r *sqlTaskRecovery) run(done chan struct{}) {
	defer close(done)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	wait := func(delay time.Duration) bool {
		timer.Reset(delay)
		select {
		case <-r.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		case <-timer.C:
			return true
		}
	}
	backoff := 125 * time.Millisecond
	for {
		batch := r.beginBatch()
		if batch == 0 {
			return
		}
		failed, succeeded := false, false
		for i := 0; i < batch; i++ {
			receipt := r.take()
			if receipt == nil {
				return
			}
			ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
			resolved := r.process(ctx, receipt)
			cancel()
			r.mu.Lock()
			if resolved || r.closed {
				r.releaseLocked(receipt.credit)
				succeeded = true
			} else {
				r.pending[receipt.class].PushBack(receipt)
				failed = true
			}
			closed := r.closed
			r.mu.Unlock()
			if closed {
				return
			}
		}
		if failed {
			if succeeded {
				backoff = 250 * time.Millisecond
			} else {
				backoff = min(backoff*2, 30*time.Second)
			}
			if !wait(backoff) {
				return
			}
		} else {
			backoff = 125 * time.Millisecond
			if batch == 32 && !wait(10*time.Millisecond) {
				return
			}
		}
	}
}

func (r *sqlTaskRecovery) close() {
	r.mu.Lock()
	r.closed = true
	started := r.started
	done := r.done
	r.cancel()
	r.mu.Unlock()
	if started {
		<-done
	}
	r.mu.Lock()
	for class := range r.pending {
		queue := &r.pending[class]
		for element := queue.Front(); element != nil; {
			next := element.Next()
			receipt := queue.Remove(element).(*sqlTaskReceipt)
			r.releaseLocked(receipt.credit)
			element = next
		}
	}
	r.mu.Unlock()
}
