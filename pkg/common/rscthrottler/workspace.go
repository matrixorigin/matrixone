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
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ContextThrottler is the optional blocking capability. Callers must retire
// their reclaimable data before waiting; spill workers must never use it.
type ContextThrottler interface {
	AcquireContext(context.Context, int64) error
	MaxAcquire() int64
	BatchBytes() int64
}

type RequestTooLargeError struct{ Requested, Maximum int64 }

func (e *RequestTooLargeError) Error() string {
	return fmt.Sprintf("memory request %d exceeds maximum %d", e.Requested, e.Maximum)
}

// WorkingSetCapacityError reports a statically impossible layout, or an
// ownerless pressure snapshot to a caller that can retry a smaller graph.
// UntilFit callers receive it only for static impossibility, not temporary
// whole-process physical pressure. An explicit NoWait caller can also use it
// for immediate admission failure: FIFO contention can deny a try even when
// Available >= Minimum. Neither snapshot promises permanent impossibility.
type WorkingSetCapacityError struct{ Minimum, Available int64 }

func (e *WorkingSetCapacityError) Error() string {
	return fmt.Sprintf("import minimum working layout needs %d bytes, available %d", e.Minimum, e.Available)
}

// WorkingSetThrottler is the complete local import-admission contract. All CN
// instances provide it; there are no unreleased older-capability providers.
// Retry-capable candidates may request a smaller graph; final candidates wait
// for transient pressure. Explicit transactions use Try without entering FIFO.
type WorkingSetThrottler interface {
	RSCThrottler
	ContextThrottler
	WorkingSetHeadroom() WorkingSetHeadroomSnapshot
	TryReserveWorkingSet(context.Context, WorkingSetSpec) (*WorkingSet, bool, error)
	ReserveWorkingSetCandidates(context.Context, []WorkingSetSpec) (*WorkingSet, error)
	ReserveWorkingSetCandidatesUntilFit(context.Context, []WorkingSetSpec) (*WorkingSet, error)
}

var ErrWorkingSetClosed = moerr.NewInternalErrorNoCtx("memory working set closed")

type InputSlotSpec struct {
	Name  string
	Count uint32
	Bytes int64
}

type WorkingSetSpec struct {
	UnitBytes int64
	Inputs    []InputSlotSpec
	IOBytes   int64
}

// WorkingSet is admitted before execution locks and the first input read.
// Input credits and the single process IO slot cannot borrow from each other.
type WorkingSet struct {
	coordinator   *memoryCoordinator
	throttler     *memThrottler
	spec          atomic.Pointer[WorkingSetSpec]
	inputBytes    int64
	inputUsed     []uint32
	inputWait     []chan struct{}
	closed        bool
	inputRefs     int
	ioRefs        int
	stagingBytes  int64
	stagingInUse  bool
	inputReleased bool
	ioReleased    bool
	closing       chan struct{}
}

type InputLease struct {
	ticket   *WorkingSet
	index    int
	released atomic.Bool
}

// StagingLease is an optional, non-blocking data buffer for one insert writer.
// Its capacity is included in the owning working set's input promise.
type StagingLease struct {
	ticket   *WorkingSet
	bytes    int64
	released atomic.Bool
}

type WorkspaceLease struct {
	mu     sync.Mutex
	ticket *WorkingSet
	bytes  int64
	valid  bool
}

func MaxAcquire(t RSCThrottler) int64 {
	if c, ok := t.(ContextThrottler); ok {
		return c.MaxAcquire()
	}
	return math.MaxInt64
}

func BatchBytes(t RSCThrottler) int64 {
	if c, ok := t.(ContextThrottler); ok {
		return c.BatchBytes()
	}
	return 64 << 20
}

func AcquireContext(ctx context.Context, t RSCThrottler, ask int64) error {
	if c, ok := t.(ContextThrottler); ok {
		return c.AcquireContext(ctx, ask)
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if _, ok := t.Acquire(ask); ok {
		return nil
	}
	return moerr.NewInternalErrorNoCtx("memory throttler does not support cancellable admission")
}

func (w *WorkingSet) UnitBytes() int64 { return w.spec.Load().UnitBytes }

func (w *WorkingSet) IOBytes() int64 { return w.spec.Load().IOBytes }

func (w *WorkingSet) InputCapacity(index int) int64 { return w.spec.Load().Inputs[index].Bytes }

// TryGrow extends an executing owner's promises without waiting on resources
// held by its downstream consumers. Shape and unit stay fixed; concurrent older
// requests cannot shrink capacities. Publication follows the complete grant.
func (w *WorkingSet) TryGrow(ctx context.Context, next WorkingSetSpec) error {
	if _, err := workingSetBytes(next); err != nil {
		return err
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	w.throttler.Refresh()
	c := w.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if w.closed {
		return ErrWorkingSetClosed
	}
	previous := w.spec.Load()
	if next.UnitBytes != previous.UnitBytes || len(next.Inputs) != len(previous.Inputs) {
		return moerr.NewInternalErrorNoCtx("memory working set growth changes layout")
	}
	next.Inputs = append([]InputSlotSpec(nil), next.Inputs...)
	for i, slot := range previous.Inputs {
		if next.Inputs[i].Name != slot.Name || next.Inputs[i].Count != slot.Count {
			return moerr.NewInternalErrorNoCtx("memory working set growth changes input owners")
		}
		next.Inputs[i].Bytes = max(next.Inputs[i].Bytes, slot.Bytes)
	}
	next.IOBytes = max(next.IOBytes, previous.IOBytes)
	input, err := workingSetBytes(next)
	if err != nil {
		return err
	}
	if w.stagingBytes > math.MaxInt64-next.IOBytes-input {
		return moerr.NewInternalErrorNoCtx("memory working set growth overflows")
	}
	input += w.stagingBytes
	delta := input - w.inputBytes + max(0, next.IOBytes-c.workspacePromise.Load())
	// An already admitted owner has priority over new admission. Queuing here
	// could wait on the very writer that is pulling this owner's next row.
	if delta > 0 && !w.throttler.physicalFits(delta) {
		return moerr.NewInternalErrorNoCtxf("import record growth cannot reserve %d additional bytes", delta)
	}
	c.inputPromise.Add(input - w.inputBytes)
	c.workspacePromise.Store(max(c.workspacePromise.Load(), next.IOBytes))
	w.inputBytes = input
	w.spec.Store(&next)
	return nil
}

// TryReserveStaging adds one optional writer buffer after the regular layout
// is admitted. It never waits or changes the selected batch unit.
func (w *WorkingSet) TryReserveStaging(ctx context.Context, bytes int64) (bool, error) {
	if bytes <= 0 {
		return false, moerr.NewInternalErrorNoCtx("invalid staging reservation")
	}
	if err := context.Cause(ctx); err != nil {
		return false, err
	}
	w.throttler.Refresh()
	c := w.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return false, err
	}
	if w.closed {
		return false, ErrWorkingSetClosed
	}
	if w.stagingBytes != 0 || bytes > math.MaxInt64-w.IOBytes()-w.inputBytes || bytes > math.MaxInt64-c.inputPromise.Load() {
		return false, moerr.NewInternalErrorNoCtx("invalid staging reservation state")
	}
	// Optional buffering must not bypass an older memory-admission waiter.
	if len(c.queue) > 0 || !w.throttler.physicalFits(bytes) {
		return false, nil
	}
	w.stagingBytes = bytes
	w.inputBytes += bytes
	c.inputPromise.Add(bytes)
	return true, nil
}

// TryAcquireStaging never waits while its caller owns an upstream input.
func (w *WorkingSet) TryAcquireStaging(ctx context.Context) (*StagingLease, error) {
	c := w.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if w.closed {
		return nil, ErrWorkingSetClosed
	}
	if w.stagingBytes == 0 || w.stagingInUse {
		return nil, nil
	}
	w.stagingInUse = true
	w.inputRefs++
	return &StagingLease{ticket: w, bytes: w.stagingBytes}, nil
}

func (l *StagingLease) Capacity() int64 { return l.bytes }

func (l *StagingLease) Release() {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	w := l.ticket
	c := w.coordinator
	c.mu.Lock()
	w.stagingInUse = false
	w.inputRefs--
	w.releasePromisesLocked()
	c.mu.Unlock()
}

func (w *WorkingSet) AcquireInput(ctx context.Context, index int) (*InputLease, error) {
	spec := w.spec.Load()
	if index < 0 || index >= len(spec.Inputs) {
		return nil, moerr.NewInternalErrorNoCtx("invalid memory input slot")
	}
	if spec.Inputs[index].Count == 0 {
		return nil, moerr.NewInternalErrorNoCtx("memory input slot is disabled")
	}
	c := w.coordinator
	for {
		c.mu.Lock()
		if err := context.Cause(ctx); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if w.closed {
			c.mu.Unlock()
			return nil, ErrWorkingSetClosed
		}
		if w.inputUsed[index] < spec.Inputs[index].Count {
			w.inputUsed[index]++
			w.inputRefs++
			c.mu.Unlock()
			return &InputLease{ticket: w, index: index}, nil
		}
		wake := w.inputWait[index]
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-w.closing:
			return nil, ErrWorkingSetClosed
		case <-wake:
		}
	}
}

func (l *InputLease) Capacity() int64 { return l.ticket.InputCapacity(l.index) }

func (l *InputLease) Release() {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	w := l.ticket
	c := w.coordinator
	c.mu.Lock()
	w.inputUsed[l.index]--
	w.inputRefs--
	close(w.inputWait[l.index])
	w.inputWait[l.index] = make(chan struct{})
	w.releasePromisesLocked()
	c.mu.Unlock()
}

func (w *WorkingSet) AcquireIO(ctx context.Context, need int64) (*WorkspaceLease, error) {
	if need < 0 {
		return nil, moerr.NewInternalErrorNoCtx("negative IO workspace size")
	}
	if capacity := w.IOBytes(); need > capacity {
		return nil, &RequestTooLargeError{need, capacity}
	}
	c := w.coordinator
	n := &ioWaiter{ticket: w, wake: make(chan struct{}, 1)}
	c.mu.Lock()
	if w.closed {
		c.mu.Unlock()
		return nil, ErrWorkingSetClosed
	}
	w.ioRefs++
	c.ioQueue = append(c.ioQueue, n)
	if c.queued != nil {
		c.queued("io")
	}
	c.notifyIOLocked()
	c.mu.Unlock()
	for {
		c.mu.Lock()
		err := context.Cause(ctx)
		if err == nil && w.closed {
			err = ErrWorkingSetClosed
		}
		if err != nil {
			c.removeIOLocked(n)
			w.ioRefs--
			w.releasePromisesLocked()
			c.mu.Unlock()
			return nil, err
		}
		if !c.ioBusy && len(c.ioQueue) > 0 && c.ioQueue[0] == n {
			c.ioBusy = true
			c.ioQueue = c.ioQueue[1:]
			if len(c.ioQueue) == 0 {
				c.ioQueue = nil
			}
			c.mu.Unlock()
			return &WorkspaceLease{ticket: w, bytes: w.IOBytes(), valid: true}, nil
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-w.closing:
		case <-n.wake:
		}
	}
}

func (l *WorkspaceLease) Capacity() int64 { return l.bytes }

// Transfer invalidates this handle. The receiving job becomes the sole owner.
func (l *WorkspaceLease) Transfer() *WorkspaceLease {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.valid {
		return nil
	}
	l.valid = false
	return &WorkspaceLease{ticket: l.ticket, bytes: l.bytes, valid: true}
}

func (l *WorkspaceLease) Release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if !l.valid {
		l.mu.Unlock()
		return
	}
	l.valid = false
	w := l.ticket
	l.mu.Unlock()
	c := w.coordinator
	c.mu.Lock()
	c.ioBusy = false
	w.ioRefs--
	w.releasePromisesLocked()
	c.notifyIOLocked()
	c.mu.Unlock()
}

func (w *WorkingSet) Close() {
	if w == nil {
		return
	}
	c := w.coordinator
	c.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.closing)
		w.releasePromisesLocked()
	}
	c.mu.Unlock()
}

func (w *WorkingSet) releasePromisesLocked() {
	c := w.coordinator
	if w.closed && w.inputRefs == 0 && !w.inputReleased {
		w.inputReleased = true
		c.inputPromise.Add(-w.inputBytes)
	}
	if w.closed && w.ioRefs == 0 && !w.ioReleased {
		w.ioReleased = true
		var maximum int64
		for ticket := range c.tickets {
			if !ticket.ioReleased {
				maximum = max(maximum, ticket.IOBytes())
			}
		}
		c.workspacePromise.Store(maximum)
	}
	if w.inputReleased && w.ioReleased {
		delete(c.tickets, w)
	}
	c.notifyAdmissionLocked()
}
