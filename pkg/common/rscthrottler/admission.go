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

package rscthrottler

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"go.uber.org/zap"
)

const admissionFallbackInterval = time.Second

type admissionWaiter struct {
	throttler *memThrottler
	wake      chan struct{}
	kind      string
	ask       int64
	enqueued  time.Time
}

type ioWaiter struct {
	ticket *WorkingSet
	wake   chan struct{}
}

// memoryCoordinator owns the process admission queues and physical promises.
// Sampling/reclamation never holds mu. A worker exists only while admission
// waits need to observe external frees; IO and input slots use release events.
type memoryCoordinator struct {
	mu               sync.Mutex
	queue            []*admissionWaiter
	ioQueue          []*ioWaiter
	ioBusy           bool
	tickets          map[*WorkingSet]struct{}
	dataReserved     atomic.Int64
	sampleCount      atomic.Uint64
	reclaimCount     atomic.Uint64
	nativeCount      atomic.Uint64
	inputPromise     atomic.Int64
	workspacePromise atomic.Int64
	workerStop       chan struct{}
	workerDone       chan struct{}
	workerWake       chan struct{}
	reclaimMu        sync.Mutex
	lastReclaim      time.Time
	lastWaitLog      time.Time
	sampleMu         sync.Mutex
	lastSample       time.Time
	knownCgroup      bool
	knownCgroupLimit int64
	sample           memorySample
	nativeMu         sync.Mutex
	lastNative       int64
	nativeHard       bool
	goMu             sync.Mutex
	lastGo           int64
	cacheMu          sync.Mutex
	lastCache        int64
	lastCacheTarget  int64
	fullMu           sync.Mutex
	fullDone         chan struct{}
	// Construction-time dependencies allow deterministic ownership tests.
	readSample func() memorySample
	now        func() time.Time
	reclaim    func(*memThrottler)
	ticks      <-chan time.Time
	queued     func(string)
}

var processMemoryCoordinator = newMemoryCoordinator()

func newMemoryCoordinator() *memoryCoordinator {
	return &memoryCoordinator{tickets: make(map[*WorkingSet]struct{}), readSample: readProcessMemory, now: time.Now}
}

func (m *memThrottler) memoryCoordinator() *memoryCoordinator {
	if m.coordinator != nil {
		return m.coordinator
	}
	return processMemoryCoordinator
}

func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (c *memoryCoordinator) notifyAdmissionLocked() {
	if len(c.queue) > 0 {
		wake(c.queue[0].wake)
	}
	if c.workerWake != nil {
		wake(c.workerWake)
	}
}

func (c *memoryCoordinator) notifyIOLocked() {
	if len(c.ioQueue) > 0 {
		wake(c.ioQueue[0].wake)
	}
}

func (c *memoryCoordinator) removeIOLocked(n *ioWaiter) {
	for i, item := range c.ioQueue {
		if item == n {
			c.ioQueue = append(c.ioQueue[:i], c.ioQueue[i+1:]...)
			break
		}
	}
	if len(c.ioQueue) == 0 {
		c.ioQueue = nil
	}
	c.notifyIOLocked()
}

func (c *memoryCoordinator) removeAdmissionLocked(n *admissionWaiter) {
	for i, item := range c.queue {
		if item == n {
			c.queue = append(c.queue[:i], c.queue[i+1:]...)
			break
		}
	}
	if len(c.queue) == 0 {
		c.queue = nil
		if c.workerStop != nil {
			close(c.workerStop)
			c.workerStop = nil
			c.workerWake = nil
		}
	} else {
		c.notifyAdmissionLocked()
	}
}

func (c *memoryCoordinator) startWorkerLocked() {
	if c.workerStop != nil {
		return
	}
	stop, done, signal := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	c.workerStop, c.workerDone, c.workerWake = stop, done, signal
	go c.run(stop, done, signal)
	wake(signal)
}

func (c *memoryCoordinator) run(stop <-chan struct{}, done chan<- struct{}, signal <-chan struct{}) {
	defer close(done)
	ticks := c.ticks
	if ticks == nil {
		ticker := time.NewTicker(admissionFallbackInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case <-stop:
			return
		case <-signal:
		case <-ticks:
		}
		c.mu.Lock()
		if len(c.queue) == 0 {
			c.mu.Unlock()
			continue
		}
		m := c.queue[0].throttler
		c.mu.Unlock()
		c.reclaimMu.Lock()
		// Releases wake immediately. Expensive process recovery is coalesced
		// to at most one cycle per fallback interval, including across pools.
		if time.Since(c.lastReclaim) >= admissionFallbackInterval {
			c.reclaimCount.Add(1)
			c.lastReclaim = time.Now()
			if c.reclaim != nil {
				c.reclaim(m)
			} else {
				objectio.ReleaseIdleArenas()
				if m.options.rssCacheEvictor != nil {
					ctx, cancel := context.WithTimeout(context.Background(), rssCacheEvictTimeout)
					c.evictCache(ctx, m, rssCacheHardTarget, time.Now().UnixNano())
					cancel()
				}
				visible := m.reserved.Load() + mpool.GlobalStats().NumCurrBytes.Load()
				if float64(max(0, visible)) < float64(m.rss.Load())*rssScavengeVisibleRate {
					c.releaseGo(time.Now().UnixNano(), freeOSMemory)
				}
				// A denied request may need native pages even below 85% RSS.
				c.releaseNative(time.Now().UnixNano(), false, true)
			}
			c.invalidateSample()
			m.refresh(true)
		}
		c.reclaimMu.Unlock()
		c.mu.Lock()
		// Do not signal workerWake here: completion must not self-schedule
		// another recovery cycle while the same request remains blocked.
		if len(c.queue) > 0 {
			wake(c.queue[0].wake)
		}
		c.mu.Unlock()
		c.reportWait(m)
	}
}

func (c *memoryCoordinator) reportWait(m *memThrottler) {
	c.mu.Lock()
	now := c.now()
	if len(c.queue) == 0 || now.Sub(c.queue[0].enqueued) < refreshMaxInterval || now.Sub(c.lastWaitLog) < refreshMaxInterval {
		c.mu.Unlock()
		return
	}
	n := c.queue[0]
	request, kind, waited, waiters := n.ask, n.kind, now.Sub(n.enqueued), len(c.queue)
	c.lastWaitLog = now
	c.mu.Unlock()
	c.sampleMu.Lock()
	sampleErr := c.sample.err
	c.sampleMu.Unlock()
	logutil.Info("memory admission waiting",
		zap.String("pool", m.name), zap.String("role", kind), zap.Int64("ask", request), zap.Duration("waited", waited), zap.Int("waiters", waiters),
		zap.Int64("physical-used", m.physicalUsed()), zap.Uint64("limit", m.actualTotalMemory.Load()), zap.Int64("ordinary", c.dataReserved.Load()), zap.Int64("input", c.inputPromise.Load()), zap.Int64("workspace", c.workspacePromise.Load()), zap.Error(sampleErr))
}

func (c *memoryCoordinator) invalidateSample() {
	c.sampleMu.Lock()
	c.lastSample = time.Time{}
	c.sampleMu.Unlock()
}

func (c *memoryCoordinator) evictCache(ctx context.Context, m *memThrottler, target, now int64) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if time.Duration(now-c.lastCache) < admissionFallbackInterval && target >= c.lastCacheTarget {
		return
	}
	c.lastCache = now
	c.lastCacheTarget = target
	m.options.rssCacheEvictor(ctx, target)
}

func (c *memoryCoordinator) releaseGo(now int64, release func()) {
	c.goMu.Lock()
	defer c.goMu.Unlock()
	if time.Duration(now-c.lastGo) <= rssScavengeInterval {
		return
	}
	c.lastGo = now
	release()
}

func (c *memoryCoordinator) releaseNative(now int64, hard, admission bool) {
	c.nativeMu.Lock()
	defer c.nativeMu.Unlock()
	interval := rssNativeScavengeInterval
	if admission {
		interval = admissionFallbackInterval
	}
	escalated := hard && !c.nativeHard
	if !admission {
		c.nativeHard = hard
	}
	if !escalated && time.Duration(now-c.lastNative) <= interval {
		return
	}
	c.lastNative = now
	c.nativeCount.Add(1)
	releaseNativeMemory()
}

func (m *memThrottler) MaxAcquire() int64 {
	if m.options.allowOutOfMemoryAcquire {
		return math.MaxInt64
	}
	if m.options.maxAcquire != nil {
		return max(0, m.options.maxAcquire(m))
	}
	return max(0, m.limit.Load())
}

func (m *memThrottler) BatchBytes() int64 { return min(64<<20, m.MaxAcquire()) }

func MaxAcquireForCNFlushS3(m *memThrottler) int64 {
	return int64(float64(m.limit.Load()) * cnFlushS3PinnedRejectRate)
}

func (m *memThrottler) acquireLocked(ask int64) (int64, bool) {
	before := m.reserved.Load()
	var left int64
	var granted bool
	if m.options.acquirePolicy != nil {
		left, granted = m.options.acquirePolicy(m, ask)
	} else {
		left, granted = defaultAcquirePolicy(m, ask)
	}
	if granted {
		m.memoryCoordinator().dataReserved.Add(m.reserved.Load() - before)
	}
	return left, granted
}

func (m *memThrottler) AcquireContext(ctx context.Context, ask int64) error {
	if ask < 0 {
		return errors.New("negative memory request")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if ask == 0 {
		return nil
	}
	m.Refresh()
	if maximum := m.MaxAcquire(); ask > maximum {
		return &RequestTooLargeError{ask, maximum}
	}
	c := m.memoryCoordinator()
	n := &admissionWaiter{throttler: m, wake: make(chan struct{}, 1), kind: "ordinary", ask: ask, enqueued: c.now()}
	c.mu.Lock()
	c.queue = append(c.queue, n)
	if c.queued != nil {
		c.queued("data")
	}
	c.startWorkerLocked()
	for {
		if err := context.Cause(ctx); err != nil {
			c.removeAdmissionLocked(n)
			c.mu.Unlock()
			return err
		}
		if c.queue[0] == n {
			if _, ok := m.acquireLocked(ask); ok {
				c.removeAdmissionLocked(n)
				c.mu.Unlock()
				return nil
			}
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-n.wake:
		}
		c.mu.Lock()
	}
}

func workingSetBytes(spec WorkingSetSpec) (int64, error) {
	if spec.UnitBytes <= 0 || spec.IOBytes < 0 {
		return 0, errors.New("invalid memory working set size")
	}
	var input int64
	for _, slot := range spec.Inputs {
		if slot.Count == 0 && slot.Bytes == 0 {
			continue
		}
		if slot.Count == 0 || slot.Bytes <= 0 || slot.Bytes > math.MaxInt64/int64(slot.Count) {
			return 0, errors.New("invalid memory input slot size")
		}
		bytes := slot.Bytes * int64(slot.Count)
		if input > math.MaxInt64-bytes {
			return 0, errors.New("memory input slots overflow")
		}
		input += bytes
	}
	if input > math.MaxInt64-spec.IOBytes {
		return 0, errors.New("memory working set overflow")
	}
	return input, nil
}

type WorkingSetHeadroomSnapshot struct {
	Limit, PhysicalUsed, Data, Input, Workspace, Available int64
	HasOwners                                              bool
}

func WorkingSetHeadroom(t RSCThrottler) WorkingSetHeadroomSnapshot {
	if m, ok := t.(interface {
		WorkingSetHeadroom() WorkingSetHeadroomSnapshot
	}); ok {
		return m.WorkingSetHeadroom()
	}
	return WorkingSetHeadroomSnapshot{Available: t.Available(), HasOwners: true}
}

func (m *memThrottler) WorkingSetHeadroom() WorkingSetHeadroomSnapshot {
	m.refresh(true)
	c := m.memoryCoordinator()
	c.mu.Lock()
	defer c.mu.Unlock()
	s := WorkingSetHeadroomSnapshot{Limit: int64(m.actualTotalMemory.Load()), PhysicalUsed: m.physicalUsed(), Data: c.dataReserved.Load(), Input: c.inputPromise.Load(), Workspace: c.workspacePromise.Load(), HasOwners: len(c.tickets) > 0 || c.dataReserved.Load() > 0 || c.ioBusy}
	s.Available = max(0, s.Limit-s.PhysicalUsed-s.Data-s.Input-s.Workspace)
	if m.sampleInvalid.Load() {
		s.Available = 0
	}
	return s
}

func TryReserveWorkingSet(ctx context.Context, t RSCThrottler, spec WorkingSetSpec) (*WorkingSet, bool, error) {
	if m, ok := t.(interface {
		TryReserveWorkingSet(context.Context, WorkingSetSpec) (*WorkingSet, bool, error)
	}); ok {
		return m.TryReserveWorkingSet(ctx, spec)
	}
	return nil, false, errors.New("memory throttler does not support working sets")
}

func newWorkingSet(c *memoryCoordinator, spec WorkingSetSpec, input int64) *WorkingSet {
	spec.Inputs = append([]InputSlotSpec(nil), spec.Inputs...)
	w := &WorkingSet{coordinator: c, spec: spec, inputBytes: input, inputUsed: make([]uint32, len(spec.Inputs)), inputWait: make([]chan struct{}, len(spec.Inputs)), refs: 1, closing: make(chan struct{})}
	for i := range w.inputWait {
		w.inputWait[i] = make(chan struct{})
	}
	return w
}

func (m *memThrottler) TryReserveWorkingSet(ctx context.Context, spec WorkingSetSpec) (*WorkingSet, bool, error) {
	input, err := workingSetBytes(spec)
	if err != nil {
		return nil, false, err
	}
	if err = context.Cause(ctx); err != nil {
		return nil, false, err
	}
	m.Refresh()
	maximum := int64(m.actualTotalMemory.Load())
	if maximum > 0 && input+spec.IOBytes > maximum {
		return nil, false, &RequestTooLargeError{input + spec.IOBytes, maximum}
	}
	c := m.memoryCoordinator()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err = context.Cause(ctx); err != nil {
		return nil, false, err
	}
	delta := input + max(0, spec.IOBytes-c.workspacePromise.Load())
	if len(c.queue) > 0 || !m.physicalFits(delta) {
		return nil, false, nil
	}
	w := newWorkingSet(c, spec, input)
	c.inputPromise.Add(input)
	c.workspacePromise.Store(max(c.workspacePromise.Load(), spec.IOBytes))
	c.tickets[w] = struct{}{}
	return w, true, nil
}

// ReclaimForWorkingSet is used once before declaring a minimum layout unable
// to fit a process with no active owners. Cancellation does not wait for IO.
func ReclaimForWorkingSet(ctx context.Context, t RSCThrottler) error {
	if m, ok := t.(interface{ ReclaimForWorkingSet(context.Context) error }); ok {
		return m.ReclaimForWorkingSet(ctx)
	}
	return errors.New("memory throttler does not support working sets")
}

func (m *memThrottler) ReclaimForWorkingSet(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	c := m.memoryCoordinator()
	c.fullMu.Lock()
	done := c.fullDone
	if done == nil {
		done = make(chan struct{})
		c.fullDone = done
		go func() {
			c.reclaimMu.Lock()
			c.lastReclaim = time.Now()
			c.reclaimCount.Add(1)
			if c.reclaim != nil {
				c.reclaim(m)
			} else {
				objectio.ReleaseIdleArenas()
				if m.options.rssCacheEvictor != nil {
					ioCtx, cancel := context.WithTimeout(context.Background(), rssCacheEvictTimeout)
					c.evictCache(ioCtx, m, 0, time.Now().UnixNano())
					cancel()
				}
				c.goMu.Lock()
				freeOSMemory()
				c.lastGo = time.Now().UnixNano()
				c.goMu.Unlock()
				c.releaseNative(time.Now().UnixNano(), false, true)
			}
			c.invalidateSample()
			m.refresh(true)
			c.reclaimMu.Unlock()
			c.fullMu.Lock()
			close(done)
			c.fullDone = nil
			c.fullMu.Unlock()
		}()
	}
	c.fullMu.Unlock()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
		return nil
	}
}

func (m *memThrottler) ReserveWorkingSet(ctx context.Context, spec WorkingSetSpec) (*WorkingSet, error) {
	return m.reserveWorkingSetCandidates(ctx, []WorkingSetSpec{spec}, workingSetSingleSpec)
}

func (m *memThrottler) ReserveWorkingSetCandidates(ctx context.Context, specs []WorkingSetSpec) (*WorkingSet, error) {
	return m.reserveWorkingSetCandidates(ctx, specs, workingSetCandidatesRetry)
}

func (m *memThrottler) ReserveWorkingSetCandidatesUntilFit(ctx context.Context, specs []WorkingSetSpec) (*WorkingSet, error) {
	return m.reserveWorkingSetCandidates(ctx, specs, workingSetCandidatesUntilFit)
}

type workingSetAdmissionMode uint8

const (
	workingSetSingleSpec workingSetAdmissionMode = iota
	workingSetCandidatesRetry
	workingSetCandidatesUntilFit
)

type workingSetCandidate struct {
	spec  WorkingSetSpec
	input int64
}

// One FIFO node owns the entire choice. Candidate construction is outside the
// mutex; fitting and publication use the current shared W maximum under it.
func (m *memThrottler) reserveWorkingSetCandidates(ctx context.Context, specs []WorkingSetSpec, mode workingSetAdmissionMode) (*WorkingSet, error) {
	if len(specs) == 0 {
		return nil, errors.New("empty memory working-set candidates")
	}
	candidates := make([]workingSetCandidate, len(specs))
	for i, spec := range specs {
		input, err := workingSetBytes(spec)
		if err != nil {
			return nil, err
		}
		if i > 0 && spec.UnitBytes >= specs[i-1].UnitBytes {
			return nil, errors.New("working-set candidates must have descending units")
		}
		spec.Inputs = append([]InputSlotSpec(nil), spec.Inputs...)
		candidates[i] = workingSetCandidate{spec: spec, input: input}
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	m.Refresh()
	maximum := int64(m.actualTotalMemory.Load())
	candidateRequest := mode != workingSetSingleSpec
	if !candidateRequest && maximum > 0 && candidates[0].input+candidates[0].spec.IOBytes > maximum {
		return nil, &RequestTooLargeError{candidates[0].input + candidates[0].spec.IOBytes, maximum}
	}
	firstFeasible := 0
	if candidateRequest && maximum > 0 {
		minimum := int64(math.MaxInt64)
		firstFeasible = len(candidates)
		for i, candidate := range candidates {
			total := candidate.input + candidate.spec.IOBytes
			minimum = min(minimum, total)
			if firstFeasible == len(candidates) && total <= maximum {
				firstFeasible = i
			}
		}
		// Even with zero resident memory and no other inputs, this ticket must
		// account for its entire I and W. Existing owners cannot make it fit.
		if minimum > maximum {
			return nil, &WorkingSetCapacityError{Minimum: minimum, Available: maximum}
		}
	}
	c := m.memoryCoordinator()
	n := &admissionWaiter{throttler: m, wake: make(chan struct{}, 1), kind: "working-set", ask: candidates[0].input + candidates[0].spec.IOBytes, enqueued: c.now()}
	c.mu.Lock()
	c.queue = append(c.queue, n)
	if c.queued != nil {
		c.queued("working-set")
	}
	reclaimed := false
	for {
		if err := context.Cause(ctx); err != nil {
			c.removeAdmissionLocked(n)
			c.mu.Unlock()
			return nil, err
		}
		if c.queue[0] == n {
			minimum := int64(math.MaxInt64)
			selected := -1
			for i, candidate := range candidates {
				delta := candidate.input + max(0, candidate.spec.IOBytes-c.workspacePromise.Load())
				minimum = min(minimum, delta)
				if m.physicalFits(delta) {
					selected = i
					break
				}
			}
			hasOwners := len(c.tickets) > 0 || c.dataReserved.Load() > 0 || c.ioBusy
			// An idle process can retain reclaimable cache/native pages from the
			// previous statement. Before accepting a smaller unit, give its largest
			// statically possible candidate one recovery attempt. Keep this FIFO
			// node while recovery runs; the next iteration rechecks cancellation,
			// other-pool owners, physical usage and the shared workspace promise.
			recoverFirst := candidateRequest && !hasOwners && !reclaimed && selected > firstFeasible
			if selected >= 0 && !recoverFirst {
				candidate := candidates[selected]
				w := newWorkingSet(c, candidate.spec, candidate.input)
				c.inputPromise.Add(candidate.input)
				c.workspacePromise.Store(max(c.workspacePromise.Load(), candidate.spec.IOBytes))
				c.tickets[w] = struct{}{}
				c.removeAdmissionLocked(n)
				c.mu.Unlock()
				return w, nil
			}
			if candidateRequest && !hasOwners {
				if !reclaimed {
					// The queued node stays in place while this existing process-wide full
					// reclaim runs. New requests cannot bypass it or consume its progress.
					c.mu.Unlock()
					err := m.ReclaimForWorkingSet(ctx)
					c.mu.Lock()
					if err != nil {
						c.removeAdmissionLocked(n)
						c.mu.Unlock()
						return nil, err
					}
					reclaimed = true
					continue
				}
				if mode == workingSetCandidatesRetry {
					available := max(0, int64(m.actualTotalMemory.Load())-m.physicalUsed()-c.inputPromise.Load()-c.workspacePromise.Load()-max(c.dataReserved.Load(), m.reserved.Load()))
					if m.sampleInvalid.Load() {
						available = 0
					}
					c.removeAdmissionLocked(n)
					c.mu.Unlock()
					return nil, &WorkingSetCapacityError{Minimum: minimum, Available: available}
				}
				// No controlled owner does not mean all physical memory is
				// permanently resident. Background completion can release or
				// repopulate idle pools after this recovery. A final graph keeps
				// its node and full candidate set for the existing worker's next
				// sample, rather than turning temporary pressure into failure.
			}
		}
		c.startWorkerLocked()
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-n.wake:
		}
		c.mu.Lock()
	}
}

func (m *memThrottler) physicalUsed() int64 {
	return max(m.rss.Load(), max(0, m.cgroupUsage.Load()-m.cgroupInactiveFile.Load()))
}

func (m *memThrottler) physicalFits(ask int64) bool {
	if m.sampleInvalid.Load() {
		return false
	}
	total := int64(m.actualTotalMemory.Load())
	if total <= 0 || total == math.MaxInt64 {
		return true
	}
	c := m.memoryCoordinator()
	remaining := total - m.physicalUsed()
	for _, value := range []int64{c.inputPromise.Load(), c.workspacePromise.Load(), max(c.dataReserved.Load(), m.reserved.Load()), ask} {
		if value < 0 || value > remaining {
			return false
		}
		remaining -= value
	}
	return true
}
