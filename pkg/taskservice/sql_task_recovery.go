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
	SQLTaskReceiptRequest
	credit *sqlTaskCredit
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

func (r *sqlTaskRecovery) release(credit *sqlTaskCredit) bool {
	r.mu.Lock()
	released := credit != nil && !credit.queued && !credit.released
	if released {
		r.releaseLocked(credit)
	}
	r.mu.Unlock()
	return released
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
	if r.closed || receipt.credit == nil || receipt.credit.released || receipt.credit.queued {
		return false
	}
	if len(receipt.Run.TaskName) > 512 || len(receipt.Run.RunnerCN) > sqlTaskRunnerMaxBytes || len(receipt.Run.ErrorMessage) > 4096 ||
		len(receipt.NextRun.TaskName) > 512 || len(receipt.NextRun.RunnerCN) > sqlTaskRunnerMaxBytes || len(receipt.NextRun.ErrorMessage) > 4096 ||
		len(receipt.Claim.Runner) > sqlTaskAsyncRunnerMaxBytes || len(receipt.Claim.ParentTaskID) > 50 || len(receipt.Claim.MetadataID) > 50 {
		return false
	}
	receipt.credit.queued = true
	sqlTaskPendingGauges[receipt.credit.class].Inc()
	receipt.Run.TaskName, receipt.Run.RunnerCN, receipt.Run.ErrorMessage = strings.Clone(receipt.Run.TaskName), strings.Clone(receipt.Run.RunnerCN), strings.Clone(receipt.Run.ErrorMessage)
	receipt.NextRun.TaskName, receipt.NextRun.RunnerCN, receipt.NextRun.ErrorMessage = strings.Clone(receipt.NextRun.TaskName), strings.Clone(receipt.NextRun.RunnerCN), strings.Clone(receipt.NextRun.ErrorMessage)
	receipt.Claim.Runner, receipt.Claim.ParentTaskID, receipt.Claim.MetadataID = strings.Clone(receipt.Claim.Runner), strings.Clone(receipt.Claim.ParentTaskID), strings.Clone(receipt.Claim.MetadataID)
	r.pending[receipt.credit.class].PushBack(receipt)
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
			if resolved {
				r.releaseLocked(receipt.credit)
				succeeded = true
			} else {
				r.pending[receipt.credit.class].PushBack(receipt)
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

func (r *sqlTaskRecovery) close() (int, []uint64) {
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
	var count int
	var sample []uint64
	for class := range r.pending {
		queue := &r.pending[class]
		for element := queue.Front(); element != nil; {
			next := element.Next()
			receipt := queue.Remove(element).(*sqlTaskReceipt)
			count++
			if receipt.Run.RunID != 0 && len(sample) < 4 {
				sample = append(sample, receipt.Run.RunID)
			}
			r.releaseLocked(receipt.credit)
			element = next
		}
	}
	r.mu.Unlock()
	return count, sample
}
